package auth.datalab.siesta

import auth.datalab.siesta.Structs._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, count, lit, sum, udf}
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession, functions}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

object DeclareMining {

  /**
   * Extracts position constraints from the new events and merges them with the existing ones.
   *
   * @param logName              The name of the log.
   * @param affectedEvents       The new events to process.
   * @param bEvolvedTracesBounds Broadcast variable containing the bounds of evolved traces.
   * @param totalTraces          Total number of traces in the log.
   * @param supportThreshold     Support threshold for filtering constraints.
   * @param branchingPolicy      Branching policy for constraint extraction.
   * @param branchingBound       Branching bound for constraint extraction.
   * @param dropFactor           Drop factor for constraint extraction.
   * @param filterRare           Flag to filter rare constraints.
   * @param filterUnderBound     Flag to filter under-bound constraints.
   * @return An array of extracted position constraints.
   */
  def extractPositionConstraints(logName: String,
                                 affectedEvents: Dataset[Event],
                                 bEvolvedTracesBounds: Broadcast[scala.collection.Map[String, (Int, Int)]],
                                 totalTraces: Long,
                                 supportThreshold: Double,
                                 branchingPolicy: String,
                                 branchingBound: Int,
                                 dropFactor: Double,
                                 filterRare: Boolean,
                                 filterUnderBound: Boolean,
                                 hardRediscover: Boolean
                                ): Array[(String, String, Array[String])] = {

    if (affectedEvents.isEmpty)
      return Array.empty[(String, String, Array[String])]

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Gather previous position constraints if exist
    val positionConstraintsPath = s"""s3a://siesta/$logName/declare/position.parquet/"""
    val oldConstraints = if (!hardRediscover) try {
      spark.read.parquet(positionConstraintsPath).as[PositionConstraintRow]
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[PositionConstraintRow]
    } else spark.emptyDataset[PositionConstraintRow]

    // Filter out oldConstraints to exclude existence measurements for the traces
    // that have evolved and keep only the unrelated ones
    val fixedOldConstraints = oldConstraints
      .join(affectedEvents.select($"eventType", $"trace").distinct(), Seq("eventType", "trace"), "left_anti")
      .select("rule", "eventType", "trace")
      .as[PositionConstraintRow]

    // Find the first and last position constraints for the new events
    val newEventsConstraints: Dataset[PositionConstraintRow] = affectedEvents.map(x => {
        if (x.pos == 0) Some(PositionConstraintRow("first", x.eventType, x.trace)) // a new trace is initiated by this new event
        else if (bEvolvedTracesBounds.value.getOrElse(x.trace, (-1, -1))._2 == x.pos) // this new event is the last event of the evolved trace
          Some(PositionConstraintRow("last", x.eventType, x.trace))
        else null // this new event is intermediate in an evolved trace
      }).filter(_.isDefined)
      .map(_.get)

    // Merge the new constraints with the fixed old ones
    val constraints = fixedOldConstraints
      .union(newEventsConstraints)
      .rdd
      .keyBy(x => (x.rule, x.eventType))
      .map(_._2)
      .toDS()


    constraints.count()
    constraints.persist(StorageLevel.MEMORY_AND_DISK)
    constraints
      .write
      .mode(SaveMode.Overwrite)
      .parquet(positionConstraintsPath)

    val response = constraints
      .rdd
      .map(x => PositionConstraint(x.rule, x.eventType, Array(x.trace)))
      .keyBy(x => (x.rule, x.eventType))
      .reduceByKey((x, y) => PositionConstraint(x.rule, x.eventType, x.traces ++ y.traces))
      .map(_._2)
      .toDS()

    response.count()
    response.persist(StorageLevel.MEMORY_AND_DISK)

    var result = Array.empty[(String, String, Array[String])]

    if (branchingPolicy == null || branchingPolicy.isEmpty)
      response.collect().foreach { x =>
        val support = x.traces.length.toDouble / totalTraces
        if (support > supportThreshold) {
          result = result :+ ((x.rule, x.eventType, x.traces))
        }
      }
    else {
      BranchedDeclare.extractBranchedSingleConstraints(response, totalTraces, supportThreshold, branchingPolicy,
        branchingBound, dropFactor = dropFactor, filterRare = filterRare, filterUnderBound = filterUnderBound)
    }
    constraints.unpersist()
    result
  }

  def extractExistenceConstraints(logName: String,
                                  affectedEvents: Dataset[Event],
                                  supportThreshold: Double,
                                  totalTraces: Long,
                                  bTraceIds: Broadcast[Set[String]],
                                  branchingPolicy: String,
                                  branchingBound: Int,
                                  dropFactor: Double,
                                  filterRare: Boolean,
                                  filterUnderBound: Boolean
                                 ): Array[(String, String, Array[String])] = {
    if (affectedEvents.isEmpty)
      return Array.empty[(String, String, Array[String])]

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    //get previous data if exist
    val existencePath = s"""s3a://siesta/$logName/declare/existence.parquet/"""
    val oldConstraints = try {
      spark.read.parquet(existencePath).as[ExactlyConstraintRow]
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[ExactlyConstraintRow]
    }

    // Take the affected events and re-evaluate the existence on their traces
    val newConstraints = affectedEvents
      .groupBy($"eventType", $"trace")
      .agg(count("*").as("instances"))
      .withColumn("rule", lit("exactly"))
      .select($"rule", $"eventType", $"instances", $"trace")
      .as[ExactlyConstraintRow]

    // Filter out oldConstraints to exclude existence measurements for the traces
    // that have evolved and keep only the unrelated ones
    val filteredPreviously = oldConstraints
      .join(newConstraints.select($"eventType", $"trace").distinct(), Seq("eventType", "trace"), "left_anti")
      .as[ExactlyConstraintRow]

    val finalConstraints = newConstraints.union(filteredPreviously.select($"rule", $"eventType", $"instances", $"trace").as[ExactlyConstraintRow])

    finalConstraints.count()
    finalConstraints.persist(StorageLevel.MEMORY_AND_DISK)
    finalConstraints.write.mode(SaveMode.Overwrite).parquet(existencePath)

    val response: Dataset[ExactlyConstraint] = finalConstraints.rdd
      .map(x => {
        ExactlyConstraint(x.rule, x.eventType, x.instances, Array(x.trace))
      })
      .keyBy(x => (x.rule, x.eventType, x.instances))
      .reduceByKey((x, y) => ExactlyConstraint(x.rule, x.eventType, x.instances, x.traces ++ y.traces))
      .map(_._2)
      .toDS()

    val completeSingleConstraints = this.extractAllExistenceConstraints(response, bTraceIds)

    var result = Array.empty[(String, String, Array[String])]
    if (branchingPolicy == null || branchingPolicy.isEmpty)
      completeSingleConstraints.foreach { x =>
        val support = Set(x.traces).size.toDouble / totalTraces
        if (support > supportThreshold) {
          result = result :+ (x.rule, x.eventA + "|" + x.eventB, x.traces)
        }
      }
    else {
      // We consider the existence constraints implicitly as pair constraints (eventB = instances),
      // and we use the same extraction method as for pair constraints, but we branch always for the
      // same eventB (instances) -> source branching
      val dummyImplicit = response.map(x => PairConstraint(x.rule, x.eventType, x.instances.toString, x.traces))
      result = BranchedDeclare.extractBranchedPairConstraints(dummyImplicit, totalTraces = totalTraces, support = supportThreshold,
        policy = branchingPolicy, branchingType = "SOURCE", branchingBound = branchingBound,
        dropFactor = dropFactor, filterRare = filterRare, filterUnderBound = filterUnderBound)
    }
    finalConstraints.unpersist()
    result
  }

  def extractAllExistenceConstraints(existences: Dataset[ExactlyConstraint],
                                     bTraceIds: Broadcast[Set[String]]): Array[PairConstraint] = {
    existences
      .rdd
      .groupBy(_.eventType)
      .flatMap { case (event_type, activities) =>
        val l = ListBuffer[PairConstraint]()

        val sortedActivities = activities.toList.sortBy(_.instances)
        var cumulativeAbsence = bTraceIds.value diff activities.flatMap(_.traces).toSet
        var cumulativeExistence = bTraceIds.value diff activities.flatMap(_.traces).toSet

        sortedActivities.foreach { activity =>
          // Exactly constraint
          l += PairConstraint("exactly",
            event_type,
            activity.instances.toString,
            activity.traces)

          // Existence constraint
          l += PairConstraint("existence",
            event_type,
            activity.instances.toString,
            (bTraceIds.value diff cumulativeExistence).toArray)

          cumulativeExistence ++= activity.traces

          // Absence constraint
          l += PairConstraint("absence", event_type, activity.instances.toString, cumulativeAbsence.toArray)

          cumulativeAbsence ++= activity.traces
        }

        l += PairConstraint("absence", event_type, (sortedActivities.last.instances + 1).toString, bTraceIds.value.toArray)
        l.toList
      }.collect()
  }

  def extractUnordered(logName: String,
                       affectedEvents: Dataset[Event],
                       bEvolvedTracesBounds: Broadcast[scala.collection.Map[String, (Int, Int)]],
                       bTraceIds: Broadcast[Set[String]],
                       activityMatrix: RDD[((String, Set[String]), (String, Set[String]))],
                       allEventOccurrences: RDD[(String, Set[String])],
                       supportThreshold: Double,
                       branchingPolicy: String,
                       branchingType: String,
                       branchingBound: Int,
                       dropFactor: Double,
                       filterRare: Boolean,
                       filterUnderBound: Boolean): Array[(String, String, Array[String])] = {

    if (affectedEvents.isEmpty)
      return Array.empty[(String, String, Array[String])]

    // Update unordered state for incremental mining
    updateUnorderedState(logName, bEvolvedTracesBounds, allEventOccurrences.keys.collect().toSet, affectedEvents)

    val exChoiceTable = s"""s3a://siesta/$logName/exChoiceTable.parquet/"""
    val coExistenceTable = s"""s3a://siesta/$logName/coExistenceTable.parquet/"""

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val exChoiceRecords = spark.read
      .parquet(exChoiceTable).as[ExChoiceRecord]
    val coExistenceRecords = spark.read.parquet(coExistenceTable)
      .as[CoExistenceRecord]

    val exChoice = exChoiceRecords
      .groupBy("eventA", "eventB")
      .agg(functions.collect_list("traceId").alias("traceIds"))
      .select("eventA", "eventB", "traceIds")
//    exChoice.persist(StorageLevel.MEMORY_AND_DISK)

    val response = coExistenceRecords
      .groupBy("eventA", "eventB")
      .agg(functions.collect_list("traceId").alias("traceIds"))
      .select("eventA", "eventB", "traceIds")
//    response.persist(StorageLevel.MEMORY_AND_DISK)

    val choice = response
      .unionByName(exChoice)
      .groupBy("eventA", "eventB")
      .agg(functions.flatten(functions.collect_list("traceIds")).alias("combined_traceIds"))
      .select(
        functions.col("eventA"),
        functions.col("eventB"),
        functions.array_distinct(functions.col("combined_traceIds")).alias("traceIds")
      )
//    choice.persist(StorageLevel.MEMORY_AND_DISK)

    // UDF to subtract the traceIds from the full set, used in co-exist and not-co-exist
    val subtractTracesFromAll = udf { (traceList: Seq[String]) =>
      bTraceIds.value.diff(traceList.toSet).toSeq
    }

    // Co-exist records is calculated by removing for each pair, the traces that exist in the ex-choice records
    val coExistence = exChoice
      .withColumn("traceIds", subtractTracesFromAll(col("traceIds")))
      .select(
        col("eventA"),
        col("eventB"),
        col("traceIds")
      )
//    coExistence.persist(StorageLevel.MEMORY_AND_DISK)

    val notCoExistence = response
      .withColumn("traceIds", subtractTracesFromAll(col("traceIds")))
      .select(
        col("eventA"),
        col("eventB"),
        col("traceIds")
      )
//    notCoExistence.persist(StorageLevel.MEMORY_AND_DISK)

    val completeSingleConstraints = choice.map(x => PairConstraint("choice", x.getAs[String]("eventA"), x.getAs[String]("eventB"), x.getAs[Seq[String]]("traceIds").toArray))
      .union(coExistence.map(x => PairConstraint("co-existence", x.getAs[String]("eventA"), x.getAs[String]("eventB"), x.getAs[Seq[String]]("traceIds").toArray)))
      .union(notCoExistence.map(x => PairConstraint("not co-existence", x.getAs[String]("eventA"), x.getAs[String]("eventB"), x.getAs[Seq[String]]("traceIds").toArray)))
      .union(response.map(x => PairConstraint("responded existence", x.getAs[String]("eventA"), x.getAs[String]("eventB"), x.getAs[Seq[String]]("traceIds").toArray)))
      .union(exChoice.map(x => PairConstraint("exclusive choice", x.getAs[String]("eventA"), x.getAs[String]("eventB"), x.getAs[Seq[String]]("traceIds").toArray)))
      .persist(StorageLevel.MEMORY_AND_DISK)

    if (branchingPolicy == null || branchingPolicy.isEmpty) {
      var result = ListBuffer.empty[(String, String, Array[String])]
      completeSingleConstraints.collect().foreach { x =>
        val support = x.traces.toSet.size.toDouble / bTraceIds.value.size
        if (support > supportThreshold) {
          result += ((x.rule, x.eventA + "|" + x.eventB, x.traces.distinct))
        }
      }
      completeSingleConstraints.unpersist()
      result.toArray
    }
    else {
      var result = Array.empty[(String, String, Array[String])]
      result = BranchedDeclare.extractBranchedPairConstraints(completeSingleConstraints, totalTraces = bTraceIds.value.size, support = supportThreshold,
        policy = branchingPolicy, branchingType = branchingType, branchingBound = branchingBound,
        dropFactor = dropFactor, filterRare = filterRare, filterUnderBound = filterUnderBound)
      completeSingleConstraints.unpersist()
      result
    }
  }

  private def updateUnorderedState(logName: String,
                                   bEvolvedTracesBounds: Broadcast[scala.collection.Map[String, (Int, Int)]],
                                   distinctEventTypes: Set[String],
                                   affectedEvents: Dataset[Event]): Unit = {
    val ex_choice_table = s"""s3a://siesta/$logName/exChoiceTable.parquet/"""
    val co_existence_table = s"""s3a://siesta/$logName/coExistenceTable.parquet/"""

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // identify event types that did not exist in the previous batches (only if exist previous batches)
    val unseen_event_types_till_now=
      try {
        val existing_event_types = spark.read.parquet(ex_choice_table)
          .select("eventA", "eventB")
          .distinct()
          .collect()
          .flatMap(x => {
            Seq(x.getString(0), x.getString(1))
          })
          .toSet
        distinctEventTypes.diff(existing_event_types)
      }catch {
        case _:org.apache.spark.sql.AnalysisException=> Set[String]()
      }

    //  extract new ex-choices and co-existances based on the newly appeared distinct
    val new_existence_records: Dataset[ExChoiceRecord] = affectedEvents
      .groupByKey(x => x.trace)
      .flatMapGroups((traceId, events) => {
        val events_seq = events.toSeq.toList
        val positions = bEvolvedTracesBounds.value.getOrElse(traceId, (0, events.size - 1))
        val new_event_types: Set[String] = events_seq.filter(x => x.pos >= positions._1).map(_.eventType)
          .distinct.toSet
        val prev_event_types: Set[String] = events_seq.filter(x => x.pos < positions._1).map(_.eventType)
          .distinct.toSet

        val unseen_et: Set[String] = distinctEventTypes
          .filter(et => !prev_event_types.contains(et) && !new_event_types.contains(et))

        val data = new_event_types.diff(prev_event_types).toSeq
          .flatMap(new_activity => {
            unseen_et
              .map(et => {
                if (new_activity < et) {
                  ExChoiceRecord(traceId, new_activity, et, 0)
                } else {
                  ExChoiceRecord(traceId, et, new_activity, 1)
                }
              }).toSeq
          })

        val co_existence = new_event_types.diff(prev_event_types).flatMap(x1 => {
          (new_event_types ++ prev_event_types).filter(x2 => x2 != x1)
            .map(x2 => {
              if (x1 < x2) {
                ExChoiceRecord(traceId, x1, x2, 2)
              } else {
                ExChoiceRecord(traceId, x2, x1, 2)
              }
            })
        })

        data ++ co_existence
      })

    // Extract previous ex-choice records if they exist
    val prev_ex_choices: Dataset[ExChoiceRecord] = try {
      spark.read
        .parquet(ex_choice_table)
        .withColumn("found", col("found").cast("int"))
        .as[ExChoiceRecord]
    } catch {
      case _: Throwable =>
        spark.createDataset(Seq.empty[ExChoiceRecord])
    }
    // Detect previous ex-choice records that are now completed
    val ex_choices_to_co_existence = (if (!prev_ex_choices.isEmpty) {
      prev_ex_choices.rdd
        .groupBy(_.traceId)
        .join(affectedEvents.rdd.groupBy(_.trace))
        .flatMap(x => {
          val positions = bEvolvedTracesBounds.value.getOrElse(x._1, (0, x._2._2.size - 1))
          val new_event_types: Set[String] = x._2._2.toSeq.filter(x => x.pos >= positions._1).map(_.eventType)
            .distinct.sorted.toSet
          x._2._1.toSeq.filter(ex => {
            (ex.found == 0 && new_event_types.contains(ex.eventB)) || (ex.found == 1 && new_event_types.contains(ex.eventA))
          })
        }).toDF()
        .select("eventA", "eventB", "found", "traceId") // reorder columns to match
    } else {
      spark.sparkContext.emptyRDD[ExChoiceRecord].toDF()
        .select("eventA", "eventB", "found", "traceId") // reorder columns to match
    })

    // calculate override ex_choice records that correspond to the changed traceIds -> that should modify only the changed
    // traces and not the entire db
    val override_ex_choices_temp = if(ex_choices_to_co_existence.isEmpty) {
      prev_ex_choices
    } else {
      prev_ex_choices.toDF()
        .except(ex_choices_to_co_existence)
    }

    val override_ex_choices = override_ex_choices_temp.toDF()
      .select("traceId","eventA","eventB","found")
      .union(new_existence_records.filter(x => x.found != 2).toDF().select("traceId","eventA","eventB","found"))

    // make the override
    override_ex_choices
      .as[ExChoiceRecord]
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("traceId")
      .parquet(ex_choice_table)

    //      append co-existence records
    val co_existence_records = ex_choices_to_co_existence
      .select("traceId","eventA","eventB")
      .union(new_existence_records.filter(_.found == 2).as[ExChoiceRecord].toDF().select("traceId","eventA","eventB"))
      .as[CoExistenceRecord]
      .toDF()

    // append co-existence records
    co_existence_records
      .write
      .mode(SaveMode.Append)
      .partitionBy("traceId")
      .parquet(co_existence_table)

    // there is a case where new even types appear in this batch, and they haven't appeared in the previous batches
    // in that case, all unchanged event types should create new ex-choices records for each unique event_type they have

    // already identified previous event types
    // get from the seq table all unique event types per traceId that does not contain the new event type
    val seq_table = s"""s3a://siesta/$logName/seq.parquet/"""
    val additional_ex_choice = spark.read.parquet(seq_table)
      .select("trace_id","event_type")
      .distinct()
      .groupBy("trace_id")
      .agg(functions.collect_list("event_type").alias("event_types"))
      .withColumnRenamed("trace_id", "traceId")
      .withColumnRenamed("event_types", "eventTypes")
      .filter(row => {
        val eventTypes = row.getAs[Seq[String]]("eventTypes")
        !unseen_event_types_till_now.exists(eventTypes.contains)
      })
      .flatMap(row => {
        val traceId = row.getAs[String]("traceId")
        val eventTypes = row.getAs[Seq[String]]("eventTypes")
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
    if(!additional_ex_choice.isEmpty){
      additional_ex_choice
        .as[ExChoiceRecord]
        .write
        .mode(SaveMode.Append)
        .partitionBy("traceId")
        .parquet(ex_choice_table)
    }

  }

  def extractOrdered(logName: String, affectedEvents: Dataset[Event],
                     bEvolvedTracesBounds: Broadcast[scala.collection.Map[String, (Int, Int)]],
                     bTraceIds: Broadcast[Set[String]],
                     activityMatrix: RDD[((String, Set[String]), (String, Set[String]))],
                     totalTraces: Long,
                     supportThreshold: Double,
                     branchingPolicy: String,
                     branchingType: String,
                     branchingBound: Int,
                     dropFactor: Double,
                     filterRare: Boolean,
                     filterBounded: Boolean,
                     hardRediscover: Boolean
                    ): Array[(String, String, Array[String])] = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    //    val s3Connector = new S3Connector()

    // get previous data if exist
    val orderPath = s"""s3a://siesta/$logName/declare/order.parquet/"""

    //    val oldConstraints = if (!hardRediscover) try {
    //      spark.read.parquet(orderPath).as[PairConstraintRow]
    //    } catch {
    //      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[PairConstraintRow]
    //    } else spark.emptyDataset[PairConstraintRow]

    val newTraces = bEvolvedTracesBounds.value.filter(_._2._1 == 0).keySet

    def extractResponseRelations(traceId: String, orderedEvents: Seq[Event]) = {
      (for {
        (e1, i) <- orderedEvents.zipWithIndex
        j <- (i + 1) until orderedEvents.length
      } yield PairConstraintRow("response", e1.eventType, orderedEvents(j).eventType, traceId)).distinct
    }

    val newConstraints: Dataset[PairConstraintRow] = affectedEvents.rdd
      .groupBy(_.trace)
      .filter(x => newTraces.contains(x._1))
      .flatMap { case (traceId, events) =>
        val orderedEvents = events.toSeq.sortBy(_.pos)

        val response: Seq[PairConstraintRow] = extractResponseRelations(traceId, orderedEvents)

        val precedence: Seq[PairConstraintRow] =
          response.flatMap {
            case PairConstraintRow(_, eventA, eventB, traceId) =>
              var aSeen = false
              var isValid = true

              for (e <- orderedEvents if isValid) {
                if (e.eventType == eventA) {
                  aSeen = true
                }
                if (e.eventType == eventB && !aSeen) {
                  isValid = false // A `B` occurred before any `A`
                }
              }

              if (isValid) Some(PairConstraintRow("precedence", eventA, eventB, traceId))
              else None
          }


        val succession: Seq[PairConstraintRow] =
          ((response.map(r => (r.eventA, r.eventB, r.trace)).toSet intersect precedence.map(p => (p.eventA, p.eventB, p.trace)).toSet)
            .map { case (eventA, eventB, trace) => PairConstraintRow("succession", eventA, eventB, trace) }).toSeq

        val alternateResponse: Seq[PairConstraintRow] =
          response.flatMap {
            case PairConstraintRow(_, eventA, eventB, traceId) =>
              var aOpen = false // whether an A is waiting for a B
              var isSatisfied = true

              for (e <- orderedEvents if isSatisfied && (e.eventType == eventA || e.eventType == eventB)) {
                e.eventType match {
                  case `eventA` =>
                    if (aOpen) isSatisfied = false // Previous A didn't get a B before this A
                    else aOpen = true // Start waiting for a B
                  case `eventB` =>
                    if (aOpen) aOpen = false // B satisfied the last A
                  // else ignore this B (not between two As)
                }
              }

              // After loop, if any A is still open, it's a violation
              if (isSatisfied && !aOpen) Some(PairConstraintRow("alternate-response", eventA, eventB, traceId))
              else None //Some(PairConstraintRow("not-chain-succession", eventA, eventB, traceId))
          }

        val alternatePrecedence: Seq[PairConstraintRow] =
          precedence.flatMap {
            case PairConstraintRow(_, eventA, eventB, traceId) =>
              var aSeen = false // waiting for a B to close the A
              var isSatisfied = true

              for (e <- orderedEvents if isSatisfied && (e.eventType == eventA || e.eventType == eventB)) {
                e.eventType match {
                  case `eventA` =>
                    aSeen = true // an A opens a precedence "slot" waiting for a B
                  case `eventB` =>
                    if (aSeen) aSeen = false // B closes the open A slot
                    else isSatisfied = false // B occurred without preceding A
                }
              }

              if (isSatisfied) Some(PairConstraintRow("alternate-precedence", eventA, eventB, traceId))
              else None //Some(PairConstraintRow("not-chain-succession", eventA, eventB, traceId))
          }

        val alternateSuccession: Seq[PairConstraintRow] =
          (alternateResponse.map(r => (r.eventA, r.eventB, r.trace)).toSet intersect alternatePrecedence.map(p => (p.eventA, p.eventB, p.trace)).toSet)
            .map { case (eventA, eventB, trace) => PairConstraintRow("alternate-succession", eventA, eventB, trace) }.toSeq

        val chainResponse: Seq[PairConstraintRow] =
          alternateResponse.flatMap {
            case PairConstraintRow(_, eventA, eventB, traceId) =>
              var isSatisfied = true

              for ((e, i) <- orderedEvents.zipWithIndex if isSatisfied && e.eventType == eventA) {
                if (i + 1 >= orderedEvents.length || orderedEvents(i + 1).eventType != eventB) {
                  isSatisfied = false // If the next event is not B, it's a violation
                }
              }

              // After loop, if there was a B not next to an A, it's a violation
              if (isSatisfied) Some(PairConstraintRow("chain-response", eventA, eventB, traceId))
              else Some(PairConstraintRow("not-chain-succession", eventA, eventB, traceId))
          }

        val chainPrecedence: Seq[PairConstraintRow] =
          alternatePrecedence.flatMap {
            case PairConstraintRow(_, eventA, eventB, traceId) =>
              var isSatisfied = true

              for ((e, i) <- orderedEvents.zipWithIndex if isSatisfied && e.eventType == eventB) {
                if (i - 1 < 0 || orderedEvents(i - 1).eventType != eventA)
                  isSatisfied = false // If the previous event is not A, it's a violation
              }

              // After loop, if there was a A not previous to a B, it's a violation
              if (isSatisfied) Some(PairConstraintRow("chain-precedence", eventA, eventB, traceId))
              else None
          }

        val chainSuccession: Seq[PairConstraintRow] =
          (chainResponse.map(r => (r.eventA, r.eventB, r.trace)).toSet intersect chainPrecedence.map(p => (p.eventA, p.eventB, p.trace)).toSet)
            .map { case (eventA, eventB, trace) => PairConstraintRow("chain-succession", eventA, eventB, trace) }.toSeq

        response ++ precedence ++ succession ++
          alternateResponse ++ alternatePrecedence ++ alternateSuccession ++
          chainResponse ++ chainPrecedence ++ chainSuccession
      }
      .toDS()


    // Find negative constraints
    val notSuccession: Dataset[PairConstraintRow] = newConstraints.rdd
      .filter(_.rule == "response")
      .groupBy(x => (x.eventA, x.eventB))
      .map(x => (x._1._1, x._1._2, x._2.map(_.trace).toSet))
      .map(x => (x._1, x._2, bTraceIds.value diff x._3))
      .flatMap(x => x._3.map(y => PairConstraintRow("not-succession", x._1, x._2, y)))
      .toDS()


    // Write updated constraints back to s3
    val updatedConstraints: Dataset[PairConstraintRow] = newConstraints.union(notSuccession)
    updatedConstraints.count()
    updatedConstraints.write.mode(SaveMode.Overwrite).parquet(orderPath)

    // Group by rule, eventA, eventB and collect traces
    val pairConstraints: Dataset[PairConstraint] = newConstraints
      .union(notSuccession)
      .groupBy("rule", "eventA", "eventB")
      .agg(collect_list($"trace").as("traces"))
      .as[PairConstraint]
    pairConstraints.persist(StorageLevel.MEMORY_AND_DISK)

    // compute constraints using support and branching and collect them
    var constraints: Array[(String, String, Array[String])] = pairConstraints.collect().flatMap { x =>
      val support = x.traces.toSet.size.toDouble / bTraceIds.value.size
      if (support > supportThreshold)
        Some((x.rule, x.eventA + "|" + x.eventB, x.traces.distinct))
      else
        None
    }

    if (branchingPolicy != null)
      constraints = BranchedDeclare.extractBranchedPairConstraints(pairConstraints,
        totalTraces,
        supportThreshold,
        branchingPolicy,
        branchingType,
        branchingBound,
        dropFactor = dropFactor,
        filterRare = filterRare,
        filterUnderBound = filterBounded)
    pairConstraints.unpersist()
    constraints
  }
}
