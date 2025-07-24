package auth.datalab.siesta

import auth.datalab.siesta.Structs._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{collect_list, count, lit}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

object QuickMining {

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
                                 hardRediscover: Boolean): Array[(String, String, Array[String])] = {
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

    response.collect().foreach { x =>
      val support = x.traces.length.toDouble / totalTraces
      if (support > supportThreshold) {
        result = result :+ ((x.rule, x.eventType, x.traces))
      }
    }
    constraints.unpersist()
    result
  }

  def extractExistenceConstraints(logName: String,
                                  affectedEvents: Dataset[Event],
                                  supportThreshold: Double,
                                  totalTraces: Long,
                                  bTraceIds: Broadcast[Set[String]]
                                 ): Array[(String, String, Array[String])] = {

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
    completeSingleConstraints.foreach { x =>
      val support = Set(x.traces).size.toDouble / totalTraces
      if (support > supportThreshold) {
        result = result :+ (x.rule, x.eventA + "|" + x.eventB, x.traces)
      }
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
                       bTraceIds: Broadcast[Set[String]],
                       activityMatrix: RDD[((String, Set[String]), (String, Set[String]))],
                       allEventOccurrences: RDD[(String, Set[String])],
                       supportThreshold: Double
                       ): Array[(String, String, Array[String])] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    //get previous data if exist
    val unorderedPath = s"""s3a://siesta/$logName/declare/unordered.parquet/"""
    val oldConstraints = try {
      spark.read.parquet(unorderedPath).as[PairConstraintRow]
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[PairConstraintRow]
    }

    // Co-existence constraints WITHOUT THE NON-EXISTENCE OF BOTH
    val coexistencePositive = affectedEvents.rdd
      .groupBy(_.trace)
      .flatMap { case (traceId, events) =>
        val eventAs = events.map(_.eventType).toSeq.distinct
        for {
          e1 <- eventAs
          e2 <- eventAs
        } yield (e1, e2, traceId)
      }.union(oldConstraints.rdd.filter(_.rule == "co-existence-positive")
        .map(x => (x.eventA, x.eventB, x.trace)))
      .distinct()
      .toDS()

    // Find all distinct traces from the log
    val allTraces = bTraceIds.value
    // Find the traces where each event does not exist
    val nonExistenceEvents = allEventOccurrences.map(x => (x._1, allTraces diff x._2))
    // Find the pairs of events that both do not exist in the same trace
    val coexistenceNegative = nonExistenceEvents.cartesian(nonExistenceEvents)
      .map { case ((k1, s1), (k2, s2)) => (k1, k2, s1.intersect(s2)) }
      .filter { case (_, _, traces) => traces.nonEmpty }
      .flatMap(x => {
        val (eventA, eventB, traces) = x
        traces.map(traceId => (eventA, eventB, traceId))
      })

    val coexistence = coexistenceNegative.union(coexistencePositive.rdd).map(x => PairConstraintRow("co-existence", x._1, x._2, x._3)).toDS()

    val notCoexistencePositive = activityMatrix.filter { case ((e1, _), (e2, _)) => e1 < e2 }
      .map { case ((e1, set1), (e2, set2)) =>
        val symmetricDiff = (set1 diff set2) union (set2 diff set1)
        (e1, e2, symmetricDiff) // converting to Set[String] if needed
      }.flatMap { case (e1, e2, diffSet) =>
        diffSet.map(traceId => (e1, e2, traceId))
      }

    val notCoexistence = notCoexistencePositive.union(coexistenceNegative.map(x => (x._1, x._2, x._3)))
      .filter { case (_, _, traces) => traces.nonEmpty }
      .distinct()
      .map(x => PairConstraintRow("not co-existence", x._1, x._2, x._3))
      .toDS()


    val choice = activityMatrix.filter { case ((e1, _), (e2, _)) => e1 < e2 }
      .flatMap { case ((e1, set1), (e2, set2)) =>
        val unionSet = set1 union set2
        unionSet.map(traceId => (e1, e2, traceId))
      }.distinct()
      .map(x => PairConstraintRow("choice", x._1, x._2, x._3))
      .toDS()


    val exclusiveChoice = notCoexistencePositive.map(x => PairConstraintRow("exclusive choice", x._1, x._2, x._3)).toDS()

    val respondedExistence = coexistencePositive.map(x => PairConstraintRow("responded existence", x._1, x._2, x._3))


    val constraintToWrite = coexistencePositive.map(x => PairConstraintRow("co-existence-positive", x._1, x._2, x._3))
      .union(respondedExistence).distinct().as[PairConstraintRow]
    constraintToWrite.count()
    constraintToWrite.persist(StorageLevel.MEMORY_AND_DISK)
    constraintToWrite.write.mode(SaveMode.Overwrite).parquet(unorderedPath)

    val completeSingleConstraints = coexistence
      .union(notCoexistence)
      .union(choice)
      .union(exclusiveChoice)
      .union(respondedExistence)
      .groupBy("rule", "eventA", "eventB")
      .agg(collect_list($"trace").as("traces"))
      .as[PairConstraint]



    var result = ListBuffer.empty[(String, String, Array[String])]
    completeSingleConstraints.collect().foreach { x =>
      val support = x.traces.toSet.size.toDouble / bTraceIds.value.size
      if (support > supportThreshold) {
        result += ((x.rule, x.eventA + "|" + x.eventB, x.traces.distinct))
      }
    }
    constraintToWrite.unpersist()
    result.toArray
  }

  def extractOrdered(logName: String, affectedEvents: Dataset[Event],
                     bEvolvedTracesBounds: Broadcast[scala.collection.Map[String, (Int, Int)]],
                     bTraceIds: Broadcast[Set[String]],
                     activityMatrix: RDD[((String, Set[String]), (String, Set[String]))],
                     totalTraces: Long,
                     supportThreshold: Double,
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

    pairConstraints.unpersist()
    constraints
  }
}