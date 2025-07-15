package auth.datalab.siesta

import auth.datalab.siesta.Structs.{Event, MetaData}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession, functions}
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp

case class ExChoiceRecord(trace_id: String, ev_a: String, ev_b: String, found: Int)

case class CoExistenceRecord(trace_id: String, ev_a: String, ev_b: String)


object extract_unordered {

  def main(args: Array[String]): Unit = {

    val s3Connector = new S3Connector()
    s3Connector.initialize(args(0))

    val metaData = s3Connector.get_metadata()
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val all_events: Dataset[Event] = s3Connector.get_events_sequence_table()

    all_events.persist(StorageLevel.MEMORY_AND_DISK)

    val event_types_occurrences: scala.collection.Map[String, Long] = all_events
      .select("event_type", "trace_id")
      .groupBy("event_type")
      .agg(functions.count("trace_id").alias("unique"))
      .collect()
      .map(row => (row.getAs[String]("event_type"), row.getAs[Long]("unique")))
      .toMap

    val bEvent_types_occurrences = spark.sparkContext.broadcast(event_types_occurrences)
    val activity_matrix: RDD[(String, String)] = Utilities.get_activity_matrix(event_types_occurrences)
    activity_matrix.persist(StorageLevel.MEMORY_AND_DISK)

    val bPrevMining = spark.sparkContext.broadcast(metaData.last_declare_mined)
    val new_events = all_events
      .filter(a => {
        if (bPrevMining.value == "") {
          true
        } else {
          // the events that we need to keep are after the previous timestamp
          //            true
          Timestamp.valueOf(bPrevMining.value).before(Timestamp.valueOf(a.ts))
        }
      })

    println(new_events.count())
    println(bPrevMining.value)

    //maintain the traces that changed
    val changed_traces: scala.collection.Map[String, (Int, Int)] = new_events
      .groupBy("trace_id")
      .agg(functions.min("pos"), functions.max("pos"))
      .map(x => (x.getAs[String]("trace_id"), (x.getAs[Int]("min(pos)"), x.getAs[Int]("max(pos)"))))
      .rdd
      .keyBy(_._1)
      .mapValues(_._2)
      .collectAsMap()

    val bChangedTraces = spark.sparkContext.broadcast(changed_traces)

    val changedTraces = bChangedTraces.value.keys.toSeq
    //maintain from the original events those that belong to a trace that changed
    val complete_traces_that_changed = all_events
      .filter(functions.col("trace_id").isin(changedTraces: _*))
    complete_traces_that_changed.count()

    complete_traces_that_changed.persist(StorageLevel.MEMORY_AND_DISK)

    incrementally_maintain_unorder_state(metaData, bChangedTraces, new_events, event_types_occurrences.keySet.toSet,
      complete_traces_that_changed)

    extract_unordered_constraints(metaData)

    if (!new_events.isEmpty) {
      val last_ts = new_events.rdd
        .map(x => Timestamp.valueOf(x.ts)).reduce((x, y) => {
          if (x.after(y)) {
            x
          } else {
            y
          }
        })
      metaData.last_declare_mined = last_ts.toString
      s3Connector.write_metadata(metaData)
    }

  }

  def incrementally_maintain_unorder_state(metaData: MetaData,
                                           bChangedTraces: Broadcast[scala.collection.Map[String, (Int, Int)]],
                                           new_events: Dataset[Event],
                                           all_event_types: Set[String],
                                           complete_traces_that_changed: Dataset[Event]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //  extract new ex-choices and co-existances based on the newly appeared distinct
    val new_existence_records: Dataset[ExChoiceRecord] = complete_traces_that_changed
      .groupByKey(x => x.trace_id)
      .flatMapGroups((trace_id, events) => {
        val events_seq = events.toSeq.toList
        val positions = bChangedTraces.value.getOrElse(trace_id, (0, events.size - 1))
        val new_event_types: Set[String] = events_seq.filter(x => x.pos >= positions._1).map(_.event_type)
          .distinct.sorted.toSet
        val prev_event_types: Set[String] = events_seq.filter(x => x.pos < positions._1).map(_.event_type)
          .distinct.sorted.toSet

        val unseen_et: Set[String] = all_event_types
          .filter(et => !prev_event_types.contains(et) && !new_event_types.contains(et))

        val data = new_event_types.diff(prev_event_types).toSeq
          .flatMap(new_activity => {
            unseen_et
              .map(et => {
                if (new_activity < et) {
                  ExChoiceRecord(trace_id, new_activity, et, 0)
                } else {
                  ExChoiceRecord(trace_id, et, new_activity, 1)
                }
              }).toSeq
          })

        val co_existence = new_event_types.flatMap(x1 => {
          (new_event_types ++ prev_event_types).filter(x2 => x2 != x1)
            .map(x2 => {
              if (x1 < x2) {
                ExChoiceRecord(trace_id, x1, x2, 2)
              } else {
                ExChoiceRecord(trace_id, x2, x1, 2)
              }
            })
        })

        data ++ co_existence
      })


//    println("New Existence Records:")
//    new_existence_records.show()

    //    define table names
    val ex_choice_table = s"""s3a://siesta/${metaData.log_name}/exChoiceTable.parquet/"""
    val co_existence_table = s"""s3a://siesta/${metaData.log_name}/coExistenceTable.parquet/"""

    // Extract previous ex-choice records if they exist
    val prev_ex_choices: Dataset[ExChoiceRecord] = try {
      spark.read
        .parquet(ex_choice_table)
        .withColumn("found", col("found").cast("int"))
        .as[ExChoiceRecord]
    } catch {
      case _ =>
        spark.createDataset(Seq.empty[ExChoiceRecord])
    }
    // Detect previous ex-choice records that are now completed
    val ex_choices_to_co_existence = (if (!prev_ex_choices.isEmpty) {
      prev_ex_choices.rdd
        .groupBy(_.trace_id)
        .join(complete_traces_that_changed.rdd.groupBy(_.trace_id))
        .flatMap(x => {
          val positions = bChangedTraces.value.getOrElse(x._1, (0, x._2._2.size - 1))
          val new_event_types: Set[String] = x._2._2.toSeq.filter(x => x.pos >= positions._1).map(_.event_type)
            .distinct.sorted.toSet
          x._2._1.toSeq.filter(ex => {
            (ex.found == 0 && new_event_types.contains(ex.ev_b)) || (ex.found == 1 && new_event_types.contains(ex.ev_a))
          })
        }).toDF()
        .select("ev_a", "ev_b", "found", "trace_id") // reorder columns to match
    } else {
      spark.sparkContext.emptyRDD[ExChoiceRecord].toDF()
        .select("ev_a", "ev_b", "found", "trace_id") // reorder columns to match
    })



    // calculate override ex_choice records that correspond to the changed trace_ids -> that should modify only the changed
    // traces and not the entire db
    val override_ex_choices_temp = if(ex_choices_to_co_existence.isEmpty) {
      prev_ex_choices
    }else {
      prev_ex_choices.toDF()
        .except(ex_choices_to_co_existence)
    }

    val override_ex_choices = override_ex_choices_temp.toDF()
      .select("trace_id","Ev_a","ev_b","found")
      .union(new_existence_records.filter(x => x.found != 2).toDF().select("trace_id","ev_a","ev_b","found"))


    // make the override
    override_ex_choices
      .as[ExChoiceRecord]
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("trace_id")
      .parquet(ex_choice_table)

    //      append co-existence records
    val co_existence_records = ex_choices_to_co_existence
      .union(new_existence_records.filter(_.found == 2).as[ExChoiceRecord].toDF())
      .select("trace_id","ev_a","ev_b")
      .as[CoExistenceRecord]
      .toDF()

//    co_existence_records.show()
    // append co-existence records
    co_existence_records
      .write
      .mode(SaveMode.Append)
      .partitionBy("trace_id")
      .parquet(co_existence_table)
  }

  def extract_unordered_constraints(metaData: MetaData): Unit = {
    val ex_choice_table = s"""s3a://siesta/${metaData.log_name}/exChoiceTable.parquet/"""
    val co_existence_table = s"""s3a://siesta/${metaData.log_name}/coExistenceTable.parquet/"""

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val ex_choice_records = spark.read
      .parquet(ex_choice_table).as[ExChoiceRecord]
    val co_existence_records = spark.read.parquet(co_existence_table)
      .as[CoExistenceRecord]

    // calculating ex-choices
    println("Ex Choice Records:")
    val ex_choices = ex_choice_records
      .groupBy("ev_a", "ev_b")
      .agg(functions.collect_list("trace_id").alias("trace_ids"))
      .select("ev_a", "ev_b", "trace_ids")
    ex_choices.persist(StorageLevel.MEMORY_AND_DISK)
    ex_choices.show()

    // calculating response
    println("Response records:")
    val response = co_existence_records
      .groupBy("ev_a", "ev_b")
      .agg(functions.collect_list("trace_id").alias("trace_ids"))
      .select("ev_a", "ev_b", "trace_ids")
    response.persist(StorageLevel.MEMORY_AND_DISK)
    response.show()

    println("Choice records:")
    val choice = response
      .unionByName(ex_choices)
      .groupBy("ev_a", "ev_b")
      .agg(functions.flatten(functions.collect_list("trace_ids")).alias("combined_trace_ids"))
      .select(
        functions.col("ev_a"),
        functions.col("ev_b"),
        functions.array_distinct(functions.col("combined_trace_ids")).alias("trace_ids")
      )
    choice.persist(StorageLevel.MEMORY_AND_DISK)
    choice.show()

    //  For the next we need the negatives, so we need a complete list of all the available traces
    val seq_table = s"""s3a://siesta/${metaData.log_name}/seq.parquet/"""
    val all_traces = spark.read.parquet(seq_table)
      .select("trace_id")
      .distinct()
      .collect()
      .map(_.getString(0))
      .toSet
    val allTracesBroadcast = spark.sparkContext.broadcast(all_traces)

    // UDF to subtract the trace_ids from the full set, used in co-exist and not-co-exist
    val substract_traces = udf { (traceList: Seq[String]) =>
      allTracesBroadcast.value.diff(traceList.toSet).toSeq
    }

    // Co-exist records is calculated by removing for each pair, the traces that exist in the ex-choice records
    println("Co-exist records:")
    val co_exist = ex_choices
      .withColumn("trace_ids", substract_traces(col("trace_ids")))
      .select(
        col("ev_a"),
        col("ev_b"),
        col("trace_ids")
      )
    co_exist.persist(StorageLevel.MEMORY_AND_DISK)
    co_exist.show()

    println("Not co-exist records:")
    val not_co_exist = response
      .withColumn("trace_ids", substract_traces(col("trace_ids")))
      .select(
        col("ev_a"),
        col("ev_b"),
        col("trace_ids")
      )
    not_co_exist.persist(StorageLevel.MEMORY_AND_DISK)
    not_co_exist.show()
    
  }

}
