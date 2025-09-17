package auth.datalab.siesta

import auth.datalab.siesta.Structs._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{
  col,
  collect_list,
  concat_ws,
  count,
  lit,
  sum,
  udf
}
import org.apache.spark.sql.{
  Dataset,
  Encoders,
  SaveMode,
  SparkSession,
  functions
}
import org.apache.spark.storage.StorageLevel
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.mutable.ListBuffer
import java.io.{File, FileWriter, BufferedWriter}
import java.nio.file.{Files, Paths}
import scala.io.Source

object DeclareMining {

  /** Centralized mining method that extracts all constraint types
    * @param config
    *   The configuration containing all mining parameters
    * @param context
    *   The mining context with all common data structures
    */
  def mine(config: Config, context: MiningContext): Unit = {

    val affectedEvents = context.affectedEvents.cache()

    // Extract position constraints
    extractPositionConstraints(
      logName = context.metaData.log_name,
      affectedEvents = affectedEvents,
      bEvolvedTracesBounds = context.bEvolvedTracesBounds,
      supportThreshold = config.support,
      totalTraces = context.totalTraces,
      branchingPolicy = config.getEffectiveBranchingPolicy,
      branchingBound = config.branchingBound,
      filterRare = config.filterRare,
      dropFactor = config.dropFactor,
      filterUnderBound =
        if (config.branchingBound > 0) config.filterUnderBound else false,
      hardRediscover = config.hardRediscovery,
      outputPath = config.outputPath
    )

    // Extract existence constraints
    extractExistenceConstraints(
      logName = context.metaData.log_name,
      affectedEvents = affectedEvents,
      bEvolvedTracesBounds = context.bEvolvedTracesBounds,
      supportThreshold = config.support,
      totalTraces = context.totalTraces,
      bTraceIds = context.bTraceIds,
      branchingPolicy = config.getEffectiveBranchingPolicy,
      branchingBound = config.branchingBound,
      filterRare = config.filterRare,
      dropFactor = config.dropFactor,
      filterUnderBound =
        if (config.branchingBound > 0) config.filterUnderBound else false,
      hardRediscover = config.hardRediscovery,
      outputPath = config.outputPath
    )

    // Maintain unordered state and extract unordered constraints
    incrementally_maintain_unorder_state(
      context.metaData,
      context.bEvolvedTracesBounds,
      context.newEvents,
      context.allEventTypes,
      affectedEvents
    )
    val unordered = extractUnordered(context.metaData, outputPath = config.outputPath)

    // Extract ordered constraints
    extractOrdered(
      logName = context.metaData.log_name,
      affectedEvents = affectedEvents,
      bEvolvedTracesBounds = context.bEvolvedTracesBounds,
      bTraceIds = context.bTraceIds,
      totalTraces = context.totalTraces,
      supportThreshold = config.support,
      branchingPolicy = config.getEffectiveBranchingPolicy,
      branchingType = config.getEffectiveBranchingType,
      branchingBound = config.branchingBound,
      filterRare = config.filterRare,
      dropFactor = config.dropFactor,
      filterBounded =
        if (config.branchingBound > 0) config.filterUnderBound else false,
      hardRediscover = config.hardRediscovery,
      outputPath = config.outputPath
    )
    
    // Merge all individual JSON files into a single consolidated file
    mergeConstraintJsonFiles(config, context.metaData.log_name)

    affectedEvents.unpersist()
  }

  /** Extracts position constraints from the new events and merges them with the
    * existing ones.
    *
    * @param logName
    *   The name of the log.
    * @param affectedEvents
    *   The new events to process.
    * @param bEvolvedTracesBounds
    *   Broadcast variable containing the bounds of evolved traces.
    * @param totalTraces
    *   Total number of traces in the log.
    * @param supportThreshold
    *   Support threshold for filtering constraints.
    * @param branchingPolicy
    *   Branching policy for constraint extraction.
    * @param branchingBound
    *   Branching bound for constraint extraction.
    * @param dropFactor
    *   Drop factor for constraint extraction.
    * @param filterRare
    *   Flag to filter rare constraints.
    * @param filterUnderBound
    *   Flag to filter under-bound constraints.
    * @return
    *   An array of extracted position constraints.
    */
  def extractPositionConstraints(
      logName: String,
      affectedEvents: Dataset[Event],
      bEvolvedTracesBounds: Broadcast[scala.collection.Map[String, (Int, Int)]],
      totalTraces: Long,
      supportThreshold: Double,
      branchingPolicy: String,
      branchingBound: Int,
      dropFactor: Double,
      filterRare: Boolean,
      filterUnderBound: Boolean,
      hardRediscover: Boolean,
      outputPath: String
  ): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Gather previous position constraints if exist
    val positionConstraintsPath =
      s"""s3a://siesta/$logName/declare/position.parquet/"""
    val oldConstraints = if (!hardRediscover) try {
      spark.read.parquet(positionConstraintsPath).as[PositionConstraintRow]
    } catch {
      case _: org.apache.spark.sql.AnalysisException =>
        spark.emptyDataset[PositionConstraintRow]
    }
    else spark.emptyDataset[PositionConstraintRow]

    // Filter out oldConstraints to exclude existence measurements for the traces
    // that have evolved and keep only the unrelated ones
    val fixedOldConstraints = oldConstraints
      .filter(x =>
        x.rule == "first" || bEvolvedTracesBounds.value
          .getOrElse(x.trace_id, (-1, -1))
          ._2 == -1
      )

    // Find the first and last position constraints for the new events
    val newEventsConstraints: Dataset[PositionConstraintRow] = affectedEvents
      .map(x => {
        if (x.pos == 0)
          Some(
            PositionConstraintRow("first", x.event_type, x.trace_id)
          ) // a new trace is initiated by this new event
        else if (
          bEvolvedTracesBounds.value.getOrElse(x.trace_id, (-1, -1))._2 == x.pos
        ) // this new event is the last event of the evolved trace
          Some(PositionConstraintRow("last", x.event_type, x.trace_id))
        else null // this new event is intermediate in an evolved trace
      })
      .filter(_.isDefined)
      .map(_.get)

    // Merge the new constraints with the fixed old ones
    val constraints = fixedOldConstraints
      .union(newEventsConstraints)

    constraints.count()
    constraints.write
      .mode(SaveMode.Overwrite)
      .parquet(positionConstraintsPath)

    val result = constraints.rdd
      .map(x => PositionConstraint(x.rule, x.event_type, Set(x.trace_id)))
      .keyBy(x => (x.rule, x.event_type))
      .reduceByKey((x, y) =>
        PositionConstraint(x.rule, x.event_type, x.traces ++ y.traces)
      )
      .map(_._2)
      .toDS()

    // if (!Utilities.isBranchingEnabled(branchingPolicy))
    result
      .map(c =>
        (c.rule, c.event_type, c.traces, c.traces.size.toDouble / totalTraces)
      )
      .toDF("rule", "event_type", "traces", "support")
      .write
      .mode(SaveMode.Overwrite)
      .json(Paths.get(outputPath, logName, "position.json").toString)

    // else {
    //   BranchedDeclare.extractBranchedSingleConstraints(result, totalTraces, supportThreshold, branchingPolicy,
    //     branchingBound, dropFactor = dropFactor, filterRare = filterRare, filterUnderBound = filterUnderBound)
    // }
  }

  def extractExistenceConstraints(
      logName: String,
      affectedEvents: Dataset[Event],
      bEvolvedTracesBounds: Broadcast[scala.collection.Map[String, (Int, Int)]],
      supportThreshold: Double,
      totalTraces: Long,
      bTraceIds: Broadcast[Set[String]],
      branchingPolicy: String,
      branchingBound: Int,
      dropFactor: Double,
      filterRare: Boolean,
      filterUnderBound: Boolean,
      hardRediscover: Boolean,
      outputPath: String
  ): Unit = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // get previous data if exist
    val existencePath = s"""s3a://siesta/$logName/declare/existence.parquet/"""
    val oldConstraints =
      try {
        spark.read.parquet(existencePath).as[ExactlyConstraintRow]
      } catch {
        case _: org.apache.spark.sql.AnalysisException =>
          spark.emptyDataset[ExactlyConstraintRow]
      }

    // Take the affected events and re-evaluate the existence on their traces
    val newConstraints = affectedEvents
      .groupBy($"event_type", $"trace_id")
      .agg(count("*").as("instances"))
      .withColumn("rule", lit("exactly"))
      .select($"rule", $"event_type", $"instances", $"trace_id")
      .as[ExactlyConstraintRow]

    // Filter out oldConstraints to exclude existence measurements for the traces
    // that have evolved and keep only the unrelated ones
    val finalConstraints = oldConstraints
      .filter(x =>
        bEvolvedTracesBounds.value.getOrElse(x.trace_id, (-1, -1))._2 == -1
      )
      .union(newConstraints)
//      .join(newConstraints.select($"event_type", $"trace_id").distinct(), Seq("event_type", "trace_id"), "left_anti")
//      .select($"rule", $"event_type", $"instances", $"trace_id")
      .as[ExactlyConstraintRow]

//    val finalConstraints = newConstraints.union(filteredPreviously.select($"rule", $"event_type", $"instances", $"trace_id").as[ExactlyConstraintRow])

    finalConstraints.count()
    finalConstraints.persist(StorageLevel.MEMORY_AND_DISK)
    finalConstraints.write.mode(SaveMode.Overwrite).parquet(existencePath)

    val response: Dataset[ExactlyConstraint] = finalConstraints.rdd
      .map(x => {
        ExactlyConstraint(x.rule, x.event_type, x.instances, Set(x.trace_id))
      })
      .keyBy(x => (x.rule, x.event_type, x.instances))
      .reduceByKey((x, y) =>
        ExactlyConstraint(
          x.rule,
          x.event_type,
          x.instances,
          x.traces ++ y.traces
        )
      )
      .map(_._2)
      .toDS()

    val completeSingleConstraints =
      this.extractAllExistenceConstraints(response, bTraceIds)

    // if (!Utilities.isBranchingEnabled(branchingPolicy))
    completeSingleConstraints
      .map(c =>
        (
          c.rule,
          c.source,
          c.target,
          c.traces,
          c.traces.size.toDouble / totalTraces
        )
      )
      .toDF("rule", "event_type", "instances", "traces", "support")
      .write
      .mode(SaveMode.Overwrite)
      .json(Paths.get(outputPath, logName, "existence.json").toString)

    // else {
    // We consider the existence constraints implicitly as pair constraints (target = instances),
    // and we use the same extraction method as for pair constraints, but we branch always for the
    // same target (instances) -> source branching
    //   val dummyImplicit = response.map(x => PairConstraint(x.rule, x.event_type, x.instances.toString, x.traces))
    //   result = BranchedDeclare.extractBranchedPairConstraints(dummyImplicit, totalTraces = totalTraces, support = supportThreshold,
    //     policy = branchingPolicy, branchingType = "SOURCE", branchingBound = branchingBound,
    //     dropFactor = dropFactor, filterRare = filterRare, filterUnderBound = filterUnderBound)
    // }
    finalConstraints.unpersist()
    // result
  }

  def extractAllExistenceConstraints(
      exactly: Dataset[ExactlyConstraint],
      bTraceIds: Broadcast[Set[String]]
  ): Dataset[PairConstraint] = {
    val spark = exactly.sparkSession
    import spark.implicits._

    exactly.rdd
      .groupBy(_.event_type)
      .flatMap { case (event_type, activities) =>
        val l = ListBuffer[PairConstraint]()

        val sortedActivities = activities.toList.sortBy(_.instances)
        var cumulativeAbsence =
          bTraceIds.value diff activities.flatMap(_.traces).toSet
        var cumulativeExistence =
          bTraceIds.value diff activities.flatMap(_.traces).toSet

        sortedActivities.foreach { activity =>
          // Exactly constraint
          l += PairConstraint(
            "exactly",
            event_type,
            activity.instances.toString,
            activity.traces
          )

          // Existence constraint
          l += PairConstraint(
            "existence",
            event_type,
            activity.instances.toString,
            bTraceIds.value diff cumulativeExistence
          )

          cumulativeExistence ++= activity.traces

          // Absence constraint
          l += PairConstraint(
            "absence",
            event_type,
            activity.instances.toString,
            cumulativeAbsence
          )

          cumulativeAbsence ++= activity.traces
        }

        l += PairConstraint(
          "absence",
          event_type,
          (sortedActivities.last.instances + 1).toString,
          bTraceIds.value
        )
        l.toList
      }
      .filter(_.traces.nonEmpty)
      .toDS()
  }

  /** Incrementally maintain the unordered state (ex-choice and co-existence
    * tables) based on the newly arrived events.
    *
    * @param metaData
    *   Metadata of the log.
    * @param bChangedTraces
    *   Broadcast variable containing the bounds of changed traces.
    * @param new_events
    *   Dataset of newly arrived events.
    * @param all_event_types
    *   Set of all event types in the log.
    * @param complete_traces_that_changed
    *   Dataset of complete traces that have changed.
    */
  def incrementally_maintain_unorder_state(
      metaData: MetaData,
      bChangedTraces: Broadcast[scala.collection.Map[String, (Int, Int)]],
      new_events: Dataset[Event],
      all_event_types: Set[String],
      complete_traces_that_changed: Dataset[Event]
  ): Unit = {
    if (complete_traces_that_changed.isEmpty)
      return

    // Cache complete_traces_that_changed since it's used multiple times
    // Check if it's already cached to avoid warnings
    val wasCached =
      complete_traces_that_changed.storageLevel != StorageLevel.NONE
    if (!wasCached) {
      complete_traces_that_changed.cache()
    }

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //    define table names
    val ex_choice_table =
      s"""s3a://siesta/${metaData.log_name}/exChoiceTable.parquet/"""
    val co_existence_table =
      s"""s3a://siesta/${metaData.log_name}/coExistenceTable.parquet/"""

    // identify event types that did not exist in the previous batches (only if exist previous batches)
    val unseen_event_types_till_now =
      try {
        val existing_event_types = spark.read
          .parquet(ex_choice_table)
          .select("source", "target")
          .distinct()
          .collect()
          .flatMap(x => {
            Seq(x.getString(0), x.getString(1))
          })
          .toSet
        all_event_types.diff(existing_event_types)
      } catch {
        case _: org.apache.spark.sql.AnalysisException => Set[String]()
      }
//    println("Unseen event types: ", unseen_event_types_till_now)

    //  extract new ex-choices and co-existances based on the newly appeared distinct
    val new_existence_records: Dataset[ExChoiceRecord] =
      complete_traces_that_changed
        .groupByKey(x => x.trace_id)
        .flatMapGroups((trace_id, events) => {
          val events_seq = events.toSeq.toList
          // positions of the new events in the trace, if they do not exist => consider the whole trace
          val positions =
            bChangedTraces.value.getOrElse(trace_id, (0, events.size - 1))
          // event types that exist in the new part of the trace
          val new_event_types: Set[String] = events_seq
            .filter(x => x.pos >= positions._1)
            .map(_.event_type)
            .distinct
            .toSet
          // event types that exist in the previous part of the trace
          val prev_event_types: Set[String] = events_seq
            .filter(x => x.pos < positions._1)
            .map(_.event_type)
            .distinct
            .toSet
          // event types that doesn't exist in the trace
          val unseen_et: Set[String] = all_event_types
            .filter(et =>
              !prev_event_types.contains(et) && !new_event_types.contains(et)
            )

          // Extract the ex-choice and co-existence based on the newly arrived event types
          val data = new_event_types
            .diff(prev_event_types)
            .toSeq // event types that appeared in the new part of the trace
            .flatMap(new_activity => {
              unseen_et // joined with the events that didn't appear in this trace to create ex-choice records
                .map(et => {
                  if (new_activity < et) {
                    ExChoiceRecord(trace_id, new_activity, et, 0)
                  } else {
                    ExChoiceRecord(trace_id, et, new_activity, 1)
                  }
                })
                .toSeq
            })
          val co_existence = new_event_types
            .diff(
              prev_event_types
            ) // event types that appeared in the new part of the trace
            .flatMap(x1 => { // joined with all the unique event types of this trace to create co-existence records
              (new_event_types ++ prev_event_types)
                .filter(x2 => x2 != x1)
                .map(x2 => {
                  if (x1 < x2) {
                    // set to exChoice records with 2 as found (since both are found) => it is a Co-Existence record,
                    // but it is required since it is combined with the data
                    ExChoiceRecord(trace_id, x1, x2, 2)
                  } else {
                    ExChoiceRecord(trace_id, x2, x1, 2)
                  }
                })
            })
          data ++ co_existence
        })

    // Extract previous ex-choice records if they exist
    val prev_ex_choices: Dataset[ExChoiceRecord] =
      try {
        spark.read
          .parquet(ex_choice_table)
          .withColumn("found", col("found").cast("int"))
          .as[ExChoiceRecord]
      } catch {
        case _: org.apache.spark.sql.AnalysisException =>
          spark.createDataset(Seq.empty[ExChoiceRecord])
      }
    prev_ex_choices.count()
    // Detect previous ex-choice records that are now completed
    val ex_choices_to_co_existence = if (!prev_ex_choices.isEmpty) {
      prev_ex_choices.rdd
        .groupBy(_.trace_id)
        .join(
          complete_traces_that_changed.rdd.groupBy(_.trace_id)
        ) // join with the traces that changed
        .flatMap(x => {
          val positions =
            bChangedTraces.value.getOrElse(x._1, (0, x._2._2.size - 1))
          val new_event_types: Set[String] = x._2._2.toSeq
            .filter(x => x.pos >= positions._1)
            .map(_.event_type)
            .distinct
            .sorted
            .toSet
          x._2._1.toSeq.filter(ex => {
            (ex.found == 0 && new_event_types.contains(
              ex.target
            )) || (ex.found == 1 && new_event_types.contains(ex.source))
          })
        })
        .toDF()
        .select(
          "source",
          "target",
          "found",
          "trace_id"
        ) // reorder columns to match
    } else {
      spark.sparkContext
        .emptyRDD[ExChoiceRecord]
        .toDF()
        .select(
          "source",
          "target",
          "found",
          "trace_id"
        ) // reorder columns to match
    }

    // calculate override ex_choice records that correspond to the changed trace_ids -> that should modify only the changed
    // traces and not the entire db
    val override_ex_choices_temp = if (ex_choices_to_co_existence.isEmpty) {
      prev_ex_choices
    } else {
      prev_ex_choices
        .toDF()
        .except(ex_choices_to_co_existence)
    }

    val override_ex_choices = override_ex_choices_temp
      .toDF()
      .select("trace_id", "source", "target", "found")
      .union(
        new_existence_records
          .filter(x => x.found != 2)
          .toDF()
          .select("trace_id", "source", "target", "found")
      )

    // make the override
    overwriteParquetAtomic(
      override_ex_choices
        .as[ExChoiceRecord]
        .withColumnRenamed("source", "ev_a")
        .withColumnRenamed("target", "ev_b"),
      ex_choice_table
    )

    //      append co-existence records
    val co_existence_records =
      ex_choices_to_co_existence
        .select("trace_id", "source", "target")
        .union(
          new_existence_records
            .filter(_.found == 2)
            .as[ExChoiceRecord]
            .toDF()
            .select("trace_id", "source", "target")
        )
        .as[CoExistenceRecord]
        .withColumnRenamed("source", "ev_a")
        .withColumnRenamed("target", "ev_b")
        .toDF()

    // append co-existence records
    co_existence_records.write
      .mode(SaveMode.Append)
      .parquet(co_existence_table)

    // there is a case where new even types appear in this batch and they haven't appeared in the previous batches
    // in that case all unchanged event types should create new ex-choices records for each unique event_type they have

    // already identified previous event types
    // get from the seq table all unique event types per trace_id that does not contain the new event type
    val seq_table = s"""s3a://siesta/${metaData.log_name}/seq.parquet/"""
    val additional_ex_choice = spark.read
      .parquet(seq_table)
      .select("trace_id", "event_type")
      .distinct()
      .groupBy("trace_id")
      .agg(functions.collect_list("event_type").alias("event_types"))
      .filter(row => {
        val eventTypes = row.getAs[Seq[String]]("event_types")
        !unseen_event_types_till_now.exists(eventTypes.contains)
      })
      .flatMap(row => {
        val traceId = row.getAs[String]("trace_id")
        val eventTypes = row.getAs[Seq[String]]("event_types")
        eventTypes.flatMap(et =>
          unseen_event_types_till_now.map(unseen =>
            if (et < unseen) {
              ExChoiceRecord(traceId, et, unseen, 0)
            } else {
              ExChoiceRecord(traceId, unseen, et, 1)
            }
          )
        )
      })
    if (!additional_ex_choice.isEmpty) {
      additional_ex_choice
        .as[ExChoiceRecord]
        .write
        .mode(SaveMode.Append)
        .parquet(ex_choice_table)
    }

    // Unpersist the cached complete_traces_that_changed only if we cached it here
    if (!wasCached) {
      complete_traces_that_changed.unpersist()
    }
  }

  def extractUnordered(metaData: MetaData, outputPath: String): Unit = {
    val ex_choice_table =
      s"""s3a://siesta/${metaData.log_name}/exChoiceTable.parquet/"""
    val co_existence_table =
      s"""s3a://siesta/${metaData.log_name}/coExistenceTable.parquet/"""

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val ex_choice_records = spark.read
      .parquet(ex_choice_table)
      .withColumnRenamed("ev_a", "source")
      .withColumnRenamed("ev_b", "target")
      .as[ExChoiceRecord]
    val co_existence_records = spark.read
      .parquet(co_existence_table)
      .withColumnRenamed("ev_a", "source")
      .withColumnRenamed("ev_b", "target")
      .as[CoExistenceRecord]

    // calculating ex-choices
    val ex_choices = ex_choice_records
      .groupBy("source", "target")
      .agg(functions.collect_list("trace_id").alias("trace_ids"))
      .select("source", "target", "trace_ids")

    // calculating response
    val response = co_existence_records
      .groupBy("source", "target")
      .agg(functions.collect_list("trace_id").alias("trace_ids"))
      .select("source", "target", "trace_ids")

    val choice = response
      .unionByName(ex_choices)
      .groupBy("source", "target")
      .agg(
        functions
          .flatten(functions.collect_list("trace_ids"))
          .alias("combined_trace_ids")
      )
      .select(
        functions.col("source"),
        functions.col("target"),
        functions
          .array_distinct(functions.col("combined_trace_ids"))
          .alias("trace_ids")
      )

    //  For the next we need the negatives, so we need a complete list of all the available traces
    val seq_table = s"""s3a://siesta/${metaData.log_name}/seq.parquet/"""
    val all_traces = spark.read
      .parquet(seq_table)
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
    val co_exist = ex_choices
      .withColumn("trace_ids", substract_traces(col("trace_ids")))
      .select(
        col("source"),
        col("target"),
        col("trace_ids")
      )

    val not_co_exist = response
      .withColumn("trace_ids", substract_traces(col("trace_ids")))
      .select(
        col("source"),
        col("target"),
        col("trace_ids")
      )

    ex_choices
      .map(x =>
        ("ex-choice", x.getString(0), x.getString(1), x.getSeq[String](2).toSet)
      )
      .union(
        co_exist.map(x =>
          (
            "co-existence",
            x.getString(0),
            x.getString(1),
            x.getSeq[String](2).toSet
          )
        )
      )
      .union(
        not_co_exist.map(x =>
          (
            "not co-existence",
            x.getString(0),
            x.getString(1),
            x.getSeq[String](2).toSet
          )
        )
      )
      .union(
        choice.map(x =>
          ("choice", x.getString(0), x.getString(1), x.getSeq[String](2).toSet)
        )
      )
      .union(
        response.map(x =>
          (
            "responded existence",
            x.getString(0),
            x.getString(1),
            x.getSeq[String](2).toSet
          )
        )
      )
      .map(c => (c._1, c._2, c._3, c._4, c._4.size.toDouble / all_traces.size))
      .toDF("rule", "source", "target", "traces", "support")
      .write
      .mode(SaveMode.Overwrite)
      .json(Paths.get(outputPath, metaData.log_name, "unordered.json").toString)
  }

  private def overwriteParquetAtomic(
      df: Dataset[_],
      finalPathStr: String
  ): Unit = {
    val spark = df.sparkSession
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(new java.net.URI(finalPathStr), hadoopConf)

    val finalPath = new Path(finalPathStr)
    val tmpPath = new Path(
      finalPath.getParent,
      finalPath.getName + "_tmp_" + System.currentTimeMillis()
    )

    // 1. Write to a fresh temporary directory
    df.write.mode(SaveMode.Overwrite).parquet(tmpPath.toString)

    // 2. Delete the old directory if it exists
    if (fs.exists(finalPath)) {
      fs.delete(finalPath, true)
    }

    // 3. Move the tmp dir into place
    fs.rename(tmpPath, finalPath)
  }

  def extractOrdered(
      logName: String,
      affectedEvents: Dataset[Event],
      bEvolvedTracesBounds: Broadcast[scala.collection.Map[String, (Int, Int)]],
      bTraceIds: Broadcast[Set[String]],
      totalTraces: Long,
      supportThreshold: Double,
      branchingPolicy: String,
      branchingType: String,
      branchingBound: Int,
      dropFactor: Double,
      filterRare: Boolean,
      filterBounded: Boolean,
      hardRediscover: Boolean,
      outputPath: String
  ): Unit = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // get previous data if exist
    val orderPath = s"""s3a://siesta/$logName/declare/order.parquet/"""

    val oldConstraints = if (!hardRediscover) try {
      spark.read.parquet(orderPath).as[PairConstraintRow]
    } catch {
      case _: org.apache.spark.sql.AnalysisException =>
        spark.emptyDataset[PairConstraintRow]
    }
    else spark.emptyDataset[PairConstraintRow]

    // Cache the oldConstraints for efficient lookups without broadcasting large data
    oldConstraints.cache()
    val oldConstraintsLookup = oldConstraints.rdd
      .filter(_.rule == "response")
      .map(c => ((c.trace_id, c.source, c.target), true))
      .collectAsMap()
    val bOldConstraintsLookup =
      spark.sparkContext.broadcast(oldConstraintsLookup)

    // Collect old precedence constraints for local access
    val oldPrecedenceConstraints = oldConstraints
      .filter(_.rule == "precedence")
      .collect()
      .groupBy(_.trace_id)

    val evolvedTraces = affectedEvents.rdd.groupBy(_.trace_id)

    def extractResponseRelations(
        traceId: String,
        orderedEvents: Seq[Event]
    ): Seq[PairConstraintRow] = {
      val n = orderedEvents.length
      val suffixFutureEvents = Array.fill(n)(Set.empty[String])
      var futureEvents = Set.empty[String]

      // Traverse in reverse and populate suffixFutureEvents
      for (i <- (n - 1) to 0 by -1) {
        suffixFutureEvents(i) = futureEvents
        futureEvents += orderedEvents(i).event_type
      }

      // Get all distinct event types
      val eventTypes = orderedEvents.map(_.event_type).distinct
      val results = collection.mutable.ListBuffer.empty[PairConstraintRow]

      for {
        a <- eventTypes
        b <- eventTypes
        aPositions = orderedEvents.zipWithIndex.collect {
          case (e, idx) if e.event_type == a => idx
        }
        if aPositions.nonEmpty
        if aPositions.forall(idx => suffixFutureEvents(idx).contains(b))
      } {
        results += PairConstraintRow("response", a, b, traceId)
      }

      results
    }

    def extractPrecedenceRelations(
        trace: Seq[Event],
        responseRelations: Seq[PairConstraintRow]
    ) = {
      responseRelations.flatMap {
        case PairConstraintRow(_, eventA, eventB, traceId) =>
          var aSeen = false
          var isValid = true

          for (e <- trace if isValid) {
            if (e.event_type == eventA) {
              aSeen = true
            }
            if (e.event_type == eventB && !aSeen) {
              isValid = false // A `B` occurred before any `A`
            }
          }

          if (isValid)
            Some(PairConstraintRow("precedence", eventA, eventB, traceId))
          else None
      }
    }

    def extractChainResponse(
        traceId: String,
        stats: TraceStats,
        alternateResponse: Seq[PairConstraintRow]
    ): Seq[PairConstraintRow] =
      alternateResponse.map { case PairConstraintRow(_, a, b, _) =>
        val countA = stats.totalCount.getOrElse(a, 0)
        val adjAB = stats.adjacentCount.getOrElse((a, b), 0)
        val isChainResp = countA > 0 && adjAB == countA

        if (isChainResp)
          PairConstraintRow("chain-response", a, b, traceId)
        else
          PairConstraintRow("not-chain-succession", a, b, traceId)
      }

    def extractChainPrecedence(
        traceId: String,
        stats: TraceStats,
        alternatePrecedence: Seq[PairConstraintRow]
    ): Seq[PairConstraintRow] =
      alternatePrecedence.flatMap { case PairConstraintRow(_, a, b, _) =>
        val countB = stats.totalCount.getOrElse(b, 0)
        val adjAB = stats.adjacentCount.getOrElse((a, b), 0)
        // every B must have an A immediately before it:
        val isChainPrec = countB > 0 && adjAB == countB

        if (isChainPrec)
          Some(PairConstraintRow("chain-precedence", a, b, traceId))
        else None
      }

    val newConstraints = evolvedTraces
      .flatMap { case (traceId, events) =>
        val orderedEvents = events.toSeq.sortBy(_.pos)
        var bounds = bEvolvedTracesBounds.value.getOrElse(traceId, (-1, -1))
        // adjust bounds to include the previously last event in the new response relations
        if (bounds._1 > 0) bounds = (bounds._1 - 1, bounds._2)

        val evolvedTracePart =
          orderedEvents.filter(x => x.pos >= bounds._1 && x.pos <= bounds._2)

        val oldEventTypes =
          if (bounds._1 > 0)
            orderedEvents
              .filter(x => x.pos <= bounds._1 - 1)
              .map(_.event_type)
              .toSet
          else Set.empty[String]
        val evolvedEventTypes = evolvedTracePart.map(_.event_type).toSet

        // Response relations are extracted from the evolved part of the trace
        // and the old response relations are updated with the new ones
        // to include the new event types that were not present in the old response relations.
        val newResponses =
          extractResponseRelations(traceId, evolvedTracePart).distinct
            .filterNot { x =>
              oldEventTypes.contains(x.source) &&
              oldEventTypes.contains(x.target) &&
              !bOldConstraintsLookup.value.contains(
                (traceId, x.source, x.target)
              )
            }
        val newEventTypes = evolvedEventTypes diff oldEventTypes
        val crossNewResponses = oldEventTypes.flatMap { eventA =>
          newEventTypes.map { eventB =>
            PairConstraintRow("response", eventA, eventB, traceId)
          }
        }.toSeq
        val selfNewResponses = oldEventTypes.flatMap { eventA =>
          if (evolvedEventTypes.contains(eventA)) {
            Some(PairConstraintRow("response", eventA, eventA, traceId))
          } else None
        }.toSeq
        // Exclude new constraints that are violated in the old part of the trace
        val response = newResponses
          .union(selfNewResponses)
          .union(crossNewResponses)

        // Precedence relations are extracted from the whole trace
        val newPrecedences =
          extractPrecedenceRelations(evolvedTracePart, newResponses).distinct
            .filterNot { x =>
              oldEventTypes.contains(x.source) &&
              oldEventTypes.contains(x.target) &&
              !bOldConstraintsLookup.value.contains(
                (traceId, x.source, x.target)
              )
            }
        val validOldPrecedences = oldPrecedenceConstraints
          .getOrElse(traceId, Array.empty)
          .map { case PairConstraintRow(_, source, target, _) =>
            if (
              !evolvedEventTypes.contains(target) || !evolvedEventTypes
                .contains(source)
            )
              Some(PairConstraintRow("precedence", source, target, traceId))
//              else if (newPrecedences.exists(p => p.source == source && p.target == target && p.trace == traceId))
//                None // newPrecedences will determine if the precedence relation is valid in the evolved part
            else
              None
          }
          .filter(_.isDefined)
          .map(_.get)
          .toSeq
        val crossPrecedences = oldEventTypes
          .filter(!evolvedEventTypes.contains(_))
          .flatMap { eventA =>
            newEventTypes.map { eventB =>
              PairConstraintRow("precedence", eventA, eventB, traceId)
            }
          }
          .toSeq
        val precedence = newPrecedences
          .union(validOldPrecedences)
          .union(crossPrecedences)

        // Succession relations are extracted from the response and precedence relations
        val succession: Seq[PairConstraintRow] =
          ((response
            .map(r => (r.source, r.target, r.trace_id))
            .toSet intersect precedence
            .map(p => (p.source, p.target, p.trace_id))
            .toSet).map { case (source, target, trace_id) =>
            PairConstraintRow("succession", source, target, trace_id)
          }).toSeq

        val alternateResponse: Seq[PairConstraintRow] =
          response.flatMap {
            case PairConstraintRow(_, eventA, eventB, traceId) =>
              var aOpen = false // whether an A is waiting for a B
              var isSatisfied = true

              for (
                e <- orderedEvents
                if isSatisfied && (e.event_type == eventA || e.event_type == eventB)
              ) {
                e.event_type match {
                  case `eventA` =>
                    if (aOpen)
                      isSatisfied =
                        false // Previous A didn't get a B before this A
                    else aOpen = true // Start waiting for a B
                  case `eventB` =>
                    if (aOpen) aOpen = false // B satisfied the last A
                  // else ignore this B (not between two As)
                }
              }

              // After loop, if any A is still open, it's a violation
              if (isSatisfied && !aOpen)
                Some(
                  PairConstraintRow(
                    "alternate-response",
                    eventA,
                    eventB,
                    traceId
                  )
                )
              else
                None // Some(PairConstraintRow("not-chain-succession", eventA, eventB, traceId))
          }

        val alternatePrecedence: Seq[PairConstraintRow] =
          precedence.flatMap {
            case PairConstraintRow(_, eventA, eventB, traceId) =>
              var aSeen = false // waiting for a B to close the A
              var isSatisfied = true

              for (
                e <- orderedEvents
                if isSatisfied && (e.event_type == eventA || e.event_type == eventB)
              ) {
                e.event_type match {
                  case `eventA` =>
                    aSeen =
                      true // an A opens a precedence "slot" waiting for a B
                  case `eventB` =>
                    if (aSeen) aSeen = false // B closes the open A slot
                    else isSatisfied = false // B occurred without preceding A
                }
              }

              if (isSatisfied)
                Some(
                  PairConstraintRow(
                    "alternate-precedence",
                    eventA,
                    eventB,
                    traceId
                  )
                )
              else
                None // Some(PairConstraintRow("not-chain-succession", eventA, eventB, traceId))
          }

        val alternateSuccession: Seq[PairConstraintRow] =
          (alternateResponse
            .map(r => (r.source, r.target, r.trace_id))
            .toSet intersect alternatePrecedence
            .map(p => (p.source, p.target, p.trace_id))
            .toSet).map { case (source, target, trace_id) =>
            PairConstraintRow("alternate-succession", source, target, trace_id)
          }.toSeq

        val stats = computeTraceStats(orderedEvents)
        val chainResponse =
          extractChainResponse(traceId, stats, alternateResponse)
        val chainPrecedence =
          extractChainPrecedence(traceId, stats, alternatePrecedence)

        val chainSuccession: Seq[PairConstraintRow] =
          (chainResponse
            .map(r => (r.source, r.target, r.trace_id))
            .toSet intersect chainPrecedence
            .map(p => (p.source, p.target, p.trace_id))
            .toSet).map { case (source, target, trace_id) =>
            PairConstraintRow("chain-succession", source, target, trace_id)
          }.toSeq

        response ++ precedence ++ succession ++
          alternateResponse ++ alternatePrecedence ++ alternateSuccession ++
          chainResponse ++ chainPrecedence ++ chainSuccession
      }
      .toDS()

    // Find negative constraints
    val notSuccession: Dataset[PairConstraintRow] = newConstraints.rdd
      .filter(_.rule == "response")
      .groupBy(x => (x.source, x.target))
      .map(x => (x._1._1, x._1._2, x._2.map(_.trace_id).toSet))
      .map(x => (x._1, x._2, bTraceIds.value diff x._3))
      .flatMap(x =>
        x._3.map(y => PairConstraintRow("not-succession", x._1, x._2, y))
      )
      .toDS()

    val unchangedOldConstraints: Dataset[PairConstraintRow] = oldConstraints
      .filter(x =>
        bEvolvedTracesBounds.value.getOrElse(x.trace_id, (-1, -1))._2 == -1
      )
      .as[PairConstraintRow]

    // Write updated constraints back to s3
    val updatedConstraints: Dataset[PairConstraintRow] = newConstraints
      .union(unchangedOldConstraints)
      .union(notSuccession)
    updatedConstraints.count()
    updatedConstraints.persist(StorageLevel.MEMORY_AND_DISK)
    updatedConstraints.write.mode(SaveMode.Overwrite).parquet(orderPath)

    // Group by rule, source, target and collect traces
    val pairConstraints: Dataset[PairConstraint] = updatedConstraints
      .union(notSuccession)
      .groupBy("rule", "source", "target")
      .agg(collect_list($"trace_id").as("traces"))
      .as[PairConstraint]
    pairConstraints.persist(StorageLevel.MEMORY_AND_DISK)
    updatedConstraints.unpersist()

    // compute constraints using support and branching and collect them
    (if(Utilities.isBranchingEnabled(branchingPolicy))
      AndBranchingMiner.andMine(pairConstraints, supportThreshold * totalTraces, branchingBound, swap = branchingType.toLowerCase == "source")
    else 
      pairConstraints
    ).map(c => (
      c.rule,
      c.source,
      c.target,
      c.traces,
      c.traces.size.toDouble / totalTraces))
    .toDF("rule", "source", "target", "traces", "support")
    .filter(row => row.getAs[Double]("support") >= supportThreshold)
    .write
    .mode(SaveMode.Overwrite)
    .json(Paths.get(outputPath, logName, "ordered.json").toString)

    pairConstraints.unpersist()
    bOldConstraintsLookup.unpersist()
    oldConstraints.unpersist()
  }

  private def computeTraceStats(orderedEvents: Seq[Event]): TraceStats = {
    // total occurrences of each event_type
    val totalCount =
      orderedEvents.foldLeft(Map.empty[String, Int].withDefaultValue(0)) {
        case (counts, e) =>
          counts.updated(e.event_type, counts(e.event_type) + 1)
      }

    // adjacent (x,y) counts
    val adjacentCount = orderedEvents
      .map(_.event_type)
      .sliding(2)
      .foldLeft(Map.empty[(String, String), Int].withDefaultValue(0)) {
        case (counts, Seq(a, b)) =>
          counts.updated((a, b), counts((a, b)) + 1)
        case (counts, _) =>
          counts
      }

    TraceStats(totalCount, adjacentCount)
  }

  /** Merges all individual JSON constraint files into a single consolidated
    * JSON file and removes the individual files and directories created by
    * Spark Uses streaming mode to handle large files without loading everything
    * into memory
    *
    * @param config
    *   The configuration containing all mining parameters
    * @param logName
    *   The name of the log being processed
    */
  def mergeConstraintJsonFiles(config: Config, logName: String): Unit = {
    val outputDir = new File(config.outputPath, logName)
    val constraintTypes = List("position", "existence", "unordered", "ordered")

    // Generate the consolidated filename using JsonOutputWriter
    val writer = new JsonOutputWriter()
    val consolidatedFilename = writer.generateFileName(
      logName = logName,
      support = config.support,
      branchingBound = config.branchingBound,
      branchingPolicy = config.getEffectiveBranchingPolicy,
      config = config,
      outputPath = config.outputPath
    )

    // Ensure output directory exists
    val consolidatedFile = new File(consolidatedFilename)
    val parentDir = consolidatedFile.getParentFile
    if (parentDir != null && !parentDir.exists()) {
      parentDir.mkdirs()
    }

    var totalConstraints = 0
    var bufferedWriter: BufferedWriter = null

    try {
      bufferedWriter = new BufferedWriter(new FileWriter(consolidatedFilename))
      bufferedWriter.write("[\n")

      var isFirstConstraint = true

      constraintTypes.foreach { constraintType =>
        val constraintDir = new File(outputDir, s"$constraintType.json")
        if (constraintDir.exists() && constraintDir.isDirectory) {
          // Process spark's files one by one
          val partFiles =
            constraintDir.listFiles().filter(_.getName.startsWith("part-"))

          partFiles.foreach { partFile =>
            try {
              val source = Source.fromFile(partFile)
              try {
                // Process file line by line
                source.getLines().foreach { line =>
                  val trimmedLine = line.trim
                  if (trimmedLine.nonEmpty) {
                    // Add comma separator if not the first constraint
                    if (!isFirstConstraint) {
                      bufferedWriter.write(",\n")
                    } else {
                      isFirstConstraint = false
                    }

                    // Write the constraint with proper indentation
                    bufferedWriter.write(s"  $trimmedLine")
                    totalConstraints += 1

                    // Flush periodically to avoid large buffers
                    if (totalConstraints % 1000 == 0)
                      bufferedWriter.flush()
                  }
                }
              } finally {
                source.close()
              }
            } catch {
              case e: Exception =>
                println(s"Warning: Failed to read $partFile: ${e.getMessage}")
            }
          }
        }
      }

      bufferedWriter.write("\n]") // End JSON array
      bufferedWriter.flush()

      println(s"Consolidated constraints written to: $consolidatedFilename")
      println(s"Total constraints: $totalConstraints")

    } catch {
      case e: Exception =>
        println(s"Error writing consolidated file: ${e.getMessage}")
        throw e
    } finally {
      if (bufferedWriter != null) {
        bufferedWriter.close()
      }
    }

    // Clean up individual directories created by Spark
    constraintTypes.foreach { constraintType =>
      val constraintDir = new File(outputDir, s"$constraintType.json")
      if (constraintDir.exists() && constraintDir.isDirectory) {
        deleteDirectory(constraintDir)
      }
    }
    if (outputDir.exists() && outputDir.isDirectory)
      deleteDirectory(outputDir)
  }

  /** Recursively deletes a directory and all its contents
    *
    * @param directory
    *   The directory to delete
    */
  private def deleteDirectory(directory: File): Unit = {
    if (directory.exists()) {
      if (directory.isDirectory) {
        directory.listFiles().foreach(deleteDirectory)
      }
      directory.delete()
    }
  }

}
