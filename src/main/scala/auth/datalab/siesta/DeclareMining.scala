package auth.datalab.siesta

import auth.datalab.siesta.Structs._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, count, lit, sum}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

object DeclareMining {

  /**
   * Extracts position constraints from the new events and merges them with the existing ones.
   *
   * @param logName The name of the log.
   * @param affectedEvents The new events to process.
   * @param bEvolvedTracesBounds Broadcast variable containing the bounds of evolved traces.
   * @param totalTraces Total number of traces in the log.
   * @param supportThreshold Support threshold for filtering constraints.
   * @param branchingPolicy Branching policy for constraint extraction.
   * @param branchingBound Branching bound for constraint extraction.
   * @param dropFactor Drop factor for constraint extraction.
   * @param filterRare Flag to filter rare constraints.
   * @param filterUnderBound Flag to filter under-bound constraints.
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
        if (x.pos == 0) Some(PositionConstraintRow("first", x.eventType, x.trace))    // a new trace is initiated by this new event
        else if (bEvolvedTracesBounds.value.getOrElse(x.trace, (-1, -1))._2 == x.pos) // this new event is the last event of the evolved trace
          Some(PositionConstraintRow("last", x.eventType, x.trace))
        else null                                                                     // this new event is intermediate in an evolved trace
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
      response.collect().foreach{ x =>
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
                                 ): Array[(String, String, Array[String])]  = {

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
      .map(x => {ExactlyConstraint(x.rule, x.eventType, x.instances, Array(x.trace)) })
      .keyBy(x => (x.rule, x.eventType, x.instances))
      .reduceByKey((x, y) => ExactlyConstraint(x.rule, x.eventType, x.instances, x.traces ++ y.traces))
      .map(_._2)
      .toDS()

    val completeSingleConstraints = this.extractAllExistenceConstraints(response, bTraceIds)

    var result = Array.empty[(String, String, Array[String])]
    if (branchingPolicy == null || branchingPolicy.isEmpty)
      completeSingleConstraints.foreach{ x =>
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
        policy = branchingPolicy, branchingType = "SOURCE" , branchingBound = branchingBound,
        dropFactor = dropFactor, filterRare = filterRare, filterUnderBound = filterUnderBound)
    }
    finalConstraints.unpersist()
    result
  }

  def extractAllExistenceConstraints (existences: Dataset[ExactlyConstraint],
                                      bTraceIds: Broadcast[Set[String]]): Array[PairConstraint]  = {
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
                       activityMatrix: RDD[((String, Set[String]),(String, Set[String]))],
                       allEventOccurrences: RDD[(String, Set[String])],
                       supportThreshold: Double,
                       branchingPolicy: String,
                       branchingType: String,
                       branchingBound: Int,
                       dropFactor: Double,
                       filterRare: Boolean,
                       filterUnderBound: Boolean): Array[(String, String, Array[String])] = {
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


    if (branchingPolicy == null || branchingPolicy.isEmpty) {
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
    else {
      var result = Array.empty[(String, String, Array[String])]
      result = BranchedDeclare.extractBranchedPairConstraints(completeSingleConstraints, totalTraces = bTraceIds.value.size, support = supportThreshold,
        policy = branchingPolicy, branchingType = branchingType, branchingBound = branchingBound,
        dropFactor = dropFactor, filterRare = filterRare, filterUnderBound = filterUnderBound)
      constraintToWrite.unpersist()
      result
    }
  }

//  def extractOrdered(logName: String, affectedEvents: Dataset[Event],
//                     bChangedTraces: Broadcast[scala.collection.Map[String, (Int, Int)]],
//                     bTraceIds: Broadcast[Set[String]],
//                     activityMatrix: RDD[(String, String)],
//                     totalTraces: Long,
//                     support: Double,
//                     policy: String,
//                     branchingType: String,
//                     branchingBound: Int,
//                     dropFactor: Double,
//                     filterRare: Boolean,
//                     filterBounded: Boolean,
//                     hardRediscover: Boolean
//                     ): Array[(String, String, Double)] = {
//
//    val spark = SparkSession.builder().getOrCreate()
//    import spark.implicits._
//    // get previous data if exist
//    val orderPath = s"""s3a://siesta/$logName/declare/order.parquet/"""
//
//    val oldConstraints = if (!hardRediscover) try {
//      spark.read.parquet(orderPath).as[PairConstraintRow]
//    } catch {
//      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[PairConstraintRow]
//    } else spark.emptyDataset[PairConstraintRow]
//
//    val newConstraints = affectedEvents.rdd
//      .groupBy(_.trace)
//      .flatMap{ case (traceId, events) =>
//        // gets first and last position of the new events of the current trace
//        val newEventBounds = bChangedTraces.value(traceId)
//
//        // Map: event type of current trace -> reverse sorted positions
//        val eventsPositionsMap: Map[String, Seq[Int]] = events.map(e => (e.eventType, e.pos))
//          .groupBy(_._1)
//          .mapValues(e => e.map(_._2).toSeq.sortWith((a, b) => a > b))
//
//        val l = ListBuffer[PairConstraintRow]()
//
//        // for each event type in the new section of the trace
//        for (i <- newEventBounds._1 to newEventBounds._2) {
//          val currentEvent = events.filter(_.pos == i).head.eventType
//
//          // find the last event type's existence in the old part of the trace
//          val eventPreviousPositions = eventsPositionsMap(currentEvent).filter(x => x < i)
//          val lastCurrentEventPosition = eventPreviousPositions match {
//            case Nil => 0
//            case _ => eventPreviousPositions.max
//          }
//
//          // precedence loop
//          eventsPositionsMap.keySet.filter(x => x != currentEvent)
//            .foreach(activityA => {
//              val activityAPrevPositions = eventsPositionsMap(activityA)
//                .filter(pos_a => pos_a < i)
//              if (activityAPrevPositions.nonEmpty) {
//                val pos_a = activityAPrevPositions.max
//                if (pos_a < i) {
//                  l += PairConstraintRow("precedence", activityA, currentEvent, traceId)
//                  if (pos_a >= lastCurrentEventPosition) {
//                    l += PairConstraintRow("alternate-precedence", activityA, currentEvent, traceId)
//                    if (pos_a == i - 1) {
//                      l += PairConstraintRow("chain-precedence", activityA, currentEvent, traceId)
//                    }
//                  }
//                }
//              }
//            })
//
//          // response loop
//          eventsPositionsMap.keySet.filter(x => x != currentEvent)
//            .foreach(activityA => {
//              var largest = true
//              eventsPositionsMap(activityA).foreach(pos_a => {
//                if (pos_a < i && pos_a >= lastCurrentEventPosition) {
//                  l += PairConstraintRow("response", activityA, currentEvent, (traceId))
//                  if (l.filter(_.rule == "precedence").filter(_.eventA == activityA).filter(_.eventB == currentEvent).exists(_.trace == (traceId)))
//                    l += PairConstraintRow("succession", activityA, currentEvent, (traceId))
//                  if (largest) {
//                    l += PairConstraintRow("alternate-response", activityA, currentEvent, (traceId))
//                    if (l.filter(_.rule == "alternate-precedence").filter(_.eventA == activityA).filter(_.eventB == currentEvent).exists(_.trace == (traceId)))
//                      l += PairConstraintRow("alternate-succession", activityA, currentEvent, (traceId))
//                    largest = false
//                    if (pos_a == i - 1) {
//                      l += PairConstraintRow("chain-response", activityA, currentEvent, (traceId))
//                      if (l.filter(_.rule == "chain-precedence").filter(_.eventA == activityA).filter(_.eventB == currentEvent).exists(_.trace == (traceId)))
//                        l += PairConstraintRow("chain-succession", activityA, currentEvent, (traceId))
//                    }
//                  }
//                }
//              })
//            })
//        }
//        l.toList
//      }
//
//    // Merge with all discovered constraints previous values if exist
//    val updatedConstraints = newConstraints.union(oldConstraints.rdd).toDS()
//
//    // Write updated constraints back to s3
//    updatedConstraints.count()
//    updatedConstraints.persist(StorageLevel.MEMORY_AND_DISK)
//    updatedConstraints.write.mode(SaveMode.Overwrite).parquet(orderPath)
//
//    val pairConstraints = updatedConstraints.rdd
//      .keyBy(x => (x.rule, x.eventA, x.eventB))
//      .map(x => PairConstraint(x._1._1, x._1._2, x._1._3, Array(x._2.trace) ++ Array(x._2.trace)))
//      .toDS()
//
//    pairConstraints.count()
//    pairConstraints.persist(StorageLevel.MEMORY_AND_DISK)
//
//    //compute constraints using support and collect them
//    val constraints = if (policy == null || policy.isEmpty)
//      this.extractAllOrderedConstraints (pairConstraints,
//                                         bTraceIds,
//                                         activityMatrix,
//                                         support)
//    else
//      BranchedDeclare.extractBranchedPairConstraints (pairConstraints,
//                                                    totalTraces,
//                                                    support,
//                                                    policy,
//                                                    branchingType,
//                                                    branchingBound,
//                                                    dropFactor = dropFactor,
//                                                    filterRare = filterRare,
//                                                    filterUnderBound = filterBounded)
//    pairConstraints.unpersist()
//    constraints
//  }

  def extractAllOrderedConstraints(constraints: Dataset[PairConstraint],
                                   bTraceIds: Broadcast[Set[String]],
                                   activity_matrix: RDD[(String, String)],
                                   support: Double): Array[(String, String, Double)] = {

    val not_chain_succession = activity_matrix
      .keyBy(x => {
        (x._1, x._2)
      }).subtractByKey(
        constraints
          .filter(_.rule.contains("chain"))
          .rdd
          .map(x => {
            (x.eventA, x.eventB)
          })
          .distinct()
          .keyBy(x => (x._1, x._2)))
      .map(x => ("not-chain-succession", x._2._1 + "|" + x._2._2, 1.0))
      .collect()


    val remaining = constraints.rdd.flatMap(c => {
        val l = ListBuffer[(String, String, Double)]()
        val sup = if (c.rule.contains("response")) {
          Set(c.traces).size.toDouble / bTraceIds.value.size
        } else {
          Set(c.traces).size.toDouble  / bTraceIds.value.size
        }

        l += ((c.rule, c.eventA + "|" + c.eventB, sup))

        if (c.rule.contains("chain")) {
          l += (("not-chain-succession", c.eventA + "|" + c.eventB, 1 - sup))
        } else if (!c.rule.contains("alternate")) {
          l += (("not-succession", c.eventA + "|" + c.eventB, 1 - sup))
        }

        l.toList
      })
      .keyBy(x => (x._1, x._2))
      .reduceByKey((x, y) => (x._1, x._2, x._3 * y._3))
      .map(_._2)
      .filter(_._3 > support) //calculate the final filtering at the end
      .collect()

    not_chain_succession ++ remaining
  }

  def handle_negatives(logName: String, activity_matrix: RDD[(String, String)]): Array[(String, String)] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //get previous data if exist
    val negative_path = s"""s3a://siesta/$logName/declare/negatives.parquet/"""

    val order_path = s"""s3a://siesta/$logName/declare/order.parquet/"""

    val ordered_response = try {
      spark.read.parquet(order_path)
        .filter(x => x.getString(0) == "response" && x.getDouble(3) > 0)
        .map(x => (x.getString(1), x.getString(2)))
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[(String, String)]
    }

    val new_negative_pairs = activity_matrix.subtract(ordered_response.rdd)
      .filter(x => x._1 != x._2)
      .toDS()

    new_negative_pairs.count()
    new_negative_pairs.persist(StorageLevel.MEMORY_AND_DISK)

    //    write new ones back to S3
    new_negative_pairs.write.mode(SaveMode.Overwrite).parquet(negative_path)

    val response = new_negative_pairs.collect()
    new_negative_pairs.unpersist()
    response
  }

}
