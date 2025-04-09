package auth.datalab.siesta

import auth.datalab.siesta.Structs._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

object DeclareMining {


  def extractPositionConstraints(logName: String,
                                 newEvents: Dataset[Event],
                                 bEvolvedTracesBounds: Broadcast[scala.collection.Map[String, (Int, Int)]],
                                 totalTraces: Long,
                                 supportThreshold: Double,
                                 branchingPolicy: String,
                                 branchingBound: Int,
                                 dropFactor: Double,
                                 filterRare: Boolean,
                                 filterUnderBound: Boolean
                                ): Array[(String, String, Double)] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Gather previous position constraints if exist
    val positionConstraintsPath = s"""s3a://siesta/$logName/declare/position.parquet/"""
    val oldConstraints = try {
      spark.read.parquet(positionConstraintsPath).as[PositionConstraintRow]
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[PositionConstraintRow]
    }

    // Discard old last-position constraints for the traces that have evolved
    val fixedOldConstraints = oldConstraints
      .filter{x => !(x.rule == "last" && bEvolvedTracesBounds.value.keySet.contains(x.trace))}

    // Find the fist and last position constraints for the new events
    val newEventsConstraints: Dataset[PositionConstraintRow] = newEvents.map(x => {
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

    constraints.toDS()
      .write
      .mode(SaveMode.Overwrite)
      .parquet(positionConstraintsPath)

    val response = constraints
      .map(x => PositionConstraint(x.rule, x.eventType, Array(x.trace)))
      .keyBy(x => (x.rule, x.eventType))
      .reduceByKey((x, y) => PositionConstraint(x.rule, x.eventType, x.traces ++ y.traces))
      .map(_._2)
      .toDS()
    constraints.count()
    constraints.persist(StorageLevel.MEMORY_AND_DISK)

    var result = Array.empty[(String, String, Double)]

    if (branchingPolicy == null || branchingPolicy.isEmpty)
      response.foreach{ x =>
        val support = x.traces.length.toDouble / totalTraces
        if (support > supportThreshold) {
          result = result :+ (x.rule, x.eventType, support)
        }
      }
    else {
      val branchedResponse = BranchedDeclare.extractBranchedPositionConstraints(response, totalTraces, supportThreshold, branchingPolicy,
        branchingBound, dropFactor = dropFactor, filterRare = filterRare, filterUnderBound = filterUnderBound)
      branchedResponse.foreach(x =>  result = result :+ (x._1, x._2.mkString(","), x._3))
    }
    result
  }




  def extractExistence(logname: String, complete_traces_that_changed: Dataset[Event],
                       bChangedTraces: Broadcast[scala.collection.Map[String, (Int, Int)]],
                       support: Double, total_traces: Long, policy: String): Array[PairConstraintSupported] = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //get previous data if exist
    val existence_path = s"""s3a://siesta/$logname/declare/existence.parquet/"""

    var previously = try {
      spark.read.parquet(existence_path).as[ActivityExactly]
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[ActivityExactly]
    }

    //extract changes from the new ones: add news and remove previous records
    val changesInConstraints = complete_traces_that_changed
      .rdd
      .groupBy(_.trace)
      .flatMap{ case (id, events) =>
        // gets first and last position of the new events of the current trace
        val newEventsBounds: (Int, Int) = bChangedTraces.value(id)

        // counts each event type's instances in the current trace
        val newEventInstancesMap: Map[String, Int] = events.map(e => (e.eventType, 1)).groupBy(_._1).mapValues(_.size)

        // if the first new event isn't in position 0 in the current trace
        // then also count the event type occurrences for the previous events
        val oldEventInstancesMap: Map[String, Int] = if (newEventsBounds._1 != 0) {
          events.filter(_.pos < newEventsBounds._1).map(e => (e.eventType, 1)).groupBy(_._1).mapValues(_.size)
        } else {
          Map.empty[String, Int]
        }

        val l = ListBuffer[ActivityExactly]()
        newEventInstancesMap.foreach{ case (eventType, inst) =>
          var instances: Long = inst

          // if the eventType existed at least once in the previous trace
          // then an ActivityExactly constraint has surely been created
          // and must be eliminated
          if (oldEventInstancesMap.contains(eventType)) {
            // count how many instances existed in the old part of the current trace
            instances += previously.filter(_.event_type == eventType)
              .filter(_.traces.contains(id)).first().instances

            previously = previously.filter(_.event_type == eventType)
              .filter(_.traces.contains(id))
              .map(constraint => {
                ActivityExactly(constraint.event_type,constraint.instances,constraint.traces.filter(_ != id))
              })
          }
          l += ActivityExactly(eventType, instances, Array(id))
        }
        l.toList
     }

    val merged_constraints = changesInConstraints
      .keyBy(x => (x.event_type, x.instances))
      .reduceByKey((x, y) => ActivityExactly(x.event_type, x.instances, x.traces ++ y.traces))
      .map(_._2)
      .toDS()

    merged_constraints.count()
    merged_constraints.persist(StorageLevel.MEMORY_AND_DISK)

    merged_constraints.toDF()
      .write.mode(SaveMode.Overwrite)
      .parquet(existence_path)

    val response = this.extractAllExistenceConstraints(merged_constraints, support, total_traces)
    merged_constraints.unpersist()

    response
  }




  def extractUnordered(logname: String, complete_traces_that_changed: Dataset[Event],
                       bChangedTraces: Broadcast[scala.collection.Map[String, (Int, Int)]],
                       activity_matrix: RDD[(String, String)],
                       support: Double, total_traces: Long, policy: String): Either[Array[PairConstraintSupported], Array[TargetBranchedPairConstraint]] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //get previous U[x] if exist
    val u_path = s"""s3a://siesta/$logname/declare/unorder/u.parquet/"""

    val previously = try {
      spark.read.parquet(u_path)
        .map(x => (x.getString(0), x.getLong(1)))
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[(String, Long)]
    }

    //calculate new event types in the traces that changed
    val u_new: RDD[(String, Long)] = complete_traces_that_changed //this will produce rdd (event_type, #new occurrences)
      .rdd
      .groupBy(_.trace)
      .flatMap(t => { //find new event types per traces
        val new_positions = bChangedTraces.value(t._1)
        val prevEvents: Set[String] = if (new_positions._1 != 0) {
          t._2.map(_.eventType).toArray.slice(0, new_positions._1 - 1).toSet
        } else {
          Set.empty[String]
        }
        t._2.toArray
          .slice(new_positions._1, new_positions._2 + 1) //+1 because last one is exclusive
          .map(_.eventType)
          .filter(e => !prevEvents.contains(e))
          .distinct
          .map(e => (e, 1L)) //mapped to (event type, 1) (they will be counted later)
      })
      .keyBy(_._1)
      .reduceByKey((a, b) => (a._1, a._2 + b._2))
      .map(_._2)

    val merge_u = previously.rdd.fullOuterJoin(u_new)
      .map(x => {
        val total = x._2._1.getOrElse(0L) + x._2._2.getOrElse(0L)
        (x._1, total)
      })
      .toDS()

    merge_u.count()
    merge_u.persist(StorageLevel.MEMORY_AND_DISK)
    merge_u.write.mode(SaveMode.Overwrite).parquet(u_path)

    //get previous |I[a,b]UI[b,a]| if exist
    val i_path = s"""s3a://siesta/$logname/declare/unorder/i.parquet/"""

    val previously_i = try {
      spark.read.parquet(i_path)
        .map(x => (x.getString(0), x.getString(1), x.getLong(2)))
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[(String, String, Long)]
    }

    val new_pairs = complete_traces_that_changed
      .rdd
      .groupBy(_.trace)
      .flatMap(t => { // find new event types per traces
        val new_positions = bChangedTraces.value(t._1)
        var prevEvents: Set[String] = if (new_positions._1 != 0) {
          t._2.map(_.eventType).toArray.slice(0, new_positions._1 - 1).toSet
        } else {
          Set.empty[String]
        }
        val new_events = t._2.toArray
          .slice(new_positions._1, new_positions._2 + 1) // +1 because last one is exclusive
          .map(_.eventType)
          .toSet

        val l = ListBuffer[(String, String, Long)]()

        // Iterate over new events and previous events
        for (e1 <- new_events if !prevEvents.contains(e1)) {
          for (e2 <- prevEvents) {
            if (e1 < e2) {
              l += ((e1, e2, 1L))
            } // Add to the list if e1 < e2 and they're unique
            else if (e2 > e1) {
              l += ((e2, e1, 1L))
            }
          }
          prevEvents = prevEvents + e1 // Return a new Set with e1 added
        }

        l.toList
      })
      .keyBy(x => (x._1, x._2))
      .reduceByKey((x, y) => (x._1, x._2, x._3 + y._3))

    val merge_i = new_pairs
      .fullOuterJoin(previously_i.rdd.keyBy(x => (x._1, x._2)))
      .map(x => {
        val total = x._2._1.getOrElse(("", "", 0L))._3 + x._2._2.getOrElse(("", "", 0L))._3
        (x._1._1, x._1._2, total)
      })
      .toDS()
    merge_i.count()
    merge_i.persist(StorageLevel.MEMORY_AND_DISK)
    merge_i.write.mode(SaveMode.Overwrite).parquet(i_path)

    val c =
//    if (policy.isEmpty)
        Left(this.extractAllUnorderedConstraints(merge_u, merge_i, activity_matrix, support, total_traces))
//    else
//      Right(BranchedDeclare.extractAllUnorderedConstraints(merge_u, merge_i, activity_matrix, support, total_traces))

    merge_u.unpersist()
    merge_i.unpersist()
    new_pairs.unpersist()

    c
  }

  def extractOrdered(logName: String, complete_traces_that_changed: Dataset[Event],
                     bChangedTraces: Broadcast[scala.collection.Map[String, (Int, Int)]],
                     bUnique_traces_to_event_types: Broadcast[scala.collection.Map[String, Long]],
                     activity_matrix: RDD[(String, String)],
                     total_traces: Long,
                     support: Double,
                     policy: String,
                     branchingType: String,
                     branchingBound: Int,
                     dropFactor: Double,
                     filterRare: Boolean,
                     filterBounded: Boolean,
                     ): Array[(String, String, Double)] = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    // get previous data if exist
    val order_path = s"""s3a://siesta/$logName/declare/order.parquet/"""

    val previously = try {
      spark.read.parquet(order_path).as[PairConstraintRow]
        .groupByKey(row => (row.rule, row.eventA, row.eventB))
        .mapGroups { case ((rule, eventA, eventB), rows) =>
          PairConstraint(rule, eventA, eventB, rows.map(_.trace).toArray)
        }
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[PairConstraint]
    }

    val new_ordered_relations = complete_traces_that_changed.rdd
      .groupBy(_.trace)
      .flatMap{ case (id, events) =>
        // gets first and last position of the new events of the current trace
        val newEventBounds = bChangedTraces.value(id)

        // activities -> reverse sorted positions of the events that correspond to them
        val eventsPositionsMap: Map[String, Seq[Int]] = events.map(e => (e.eventType, e.pos))
          .groupBy(_._1)
          .mapValues(e => e.map(_._2).toSeq.sortWith((a, b) => a > b))

        val l = ListBuffer[PairConstraint]()

        // for each event type in the new section of the trace
        for (i <- newEventBounds._1 to newEventBounds._2) {
          val current_event_type = events.toSeq(i).eventType

          // find the last type's existence in the old part of it
          val eventPreviousPositions = eventsPositionsMap(current_event_type).filter(x => x < i)
          val lastCurrentEventPosition = eventPreviousPositions match {
            case Nil => 0
            case _ => eventPreviousPositions.max
          }

          // precedence loop
          eventsPositionsMap.keySet.filter(x => x != current_event_type)
            .foreach(activityA => {
              val activityAPrevPositions = eventsPositionsMap(activityA)
                .filter(pos_a => pos_a < i)
              if (activityAPrevPositions.nonEmpty) {
                val pos_a = activityAPrevPositions.max
                if (pos_a < i) {
                  l += PairConstraint("precedence", activityA, current_event_type, Array(id))
                  if (pos_a >= lastCurrentEventPosition) {
                    l += PairConstraint("alternate-precedence", activityA, current_event_type, Array(id))
                    if (pos_a == i - 1) {
                      l += PairConstraint("chain-precedence", activityA, current_event_type, Array(id))
                    }
                  }
                }
              }
            })

          // response loop
          eventsPositionsMap.keySet.filter(x => x != current_event_type)
            .foreach(activity_a => {
              var largest = true
              eventsPositionsMap(activity_a).foreach(pos_a => {
                if (pos_a < i && pos_a >= lastCurrentEventPosition) {
                  l += PairConstraint("response", activity_a, current_event_type, Array(id))
                  if (largest) {
                    l += PairConstraint("alternate-response", activity_a, current_event_type, Array(id))
                    largest = false
                    if (pos_a == i - 1) {
                      l += PairConstraint("chain-response", activity_a, current_event_type, Array(id))
                    }
                  }
                }
              })
            })
        }
        l.toList
      }
      .keyBy(x => (x.rule, x.eventA, x.eventB))
      .reduceByKey((x, y) => PairConstraint(x.rule, x.eventA, x.eventB, x.traces ++ y.traces))
      .map(_._2)

    val succession_constraints = new_ordered_relations.toDS()
      .filter(pc => pc.rule == "precedence" || pc.rule == "response")
      .groupByKey(pc => (pc.eventA, pc.eventB))
      .mapGroups { case ((eventA, eventB), constraints) =>
        val tracesWithPrecedence = constraints.filter(_.rule == "precedence").flatMap(_.traces).toSet
        val tracesWithResponse = constraints.filter(_.rule == "response").flatMap(_.traces).toSet
        val commonTraces = tracesWithPrecedence.intersect(tracesWithResponse)
        if (commonTraces.nonEmpty) Some(PairConstraint("succession", eventA, eventB, commonTraces.toArray))
        else None }
      .filter(_.isDefined).map(_.get)

    // Extract chain succession constraints
    val chain_succession_constraints = new_ordered_relations.toDS()
      .filter(pc => pc.rule == "chain-precedence" || pc.rule == "chain-response")
      .groupByKey(pc => (pc.eventA, pc.eventB))
      .mapGroups { case ((eventA, eventB), constraints) =>
        val tracesWithChainPrecedence = constraints.filter(_.rule == "chain-precedence").flatMap(_.traces).toSet
        val tracesWithChainResponse = constraints.filter(_.rule == "chain-response").flatMap(_.traces).toSet
        val commonTraces = tracesWithChainPrecedence.intersect(tracesWithChainResponse)
        if (commonTraces.nonEmpty) Some(PairConstraint("chain-succession", eventA, eventB, commonTraces.toArray))
        else None }
      .filter(_.isDefined).map(_.get)

    // Extract alternate succession constraints
    val alternate_succession_constraints = new_ordered_relations.toDS()
      .filter(pc => pc.rule == "alternate-precedence" || pc.rule == "alternate-response")
      .groupByKey(pc => (pc.eventA, pc.eventB))
      .mapGroups { case ((eventA, eventB), constraints) =>
        val tracesWithAltPrecedence = constraints.filter(_.rule == "alternate-precedence").flatMap(_.traces).toSet
        val tracesWithAltResponse = constraints.filter(_.rule == "alternate-response").flatMap(_.traces).toSet
        val commonTraces = tracesWithAltPrecedence.intersect(tracesWithAltResponse)
        if (commonTraces.nonEmpty) Some(PairConstraint("alternate-succession", eventA, eventB, commonTraces.toArray))
        else None }
      .filter(_.isDefined).map(_.get)

    // Merge with all discovered constraints previous values if exist
    val updated_constraints = new_ordered_relations
      .union(succession_constraints.rdd)
      .union(chain_succession_constraints.rdd)
      .union(alternate_succession_constraints.rdd)
      .keyBy(x => (x.rule, x.eventA, x.eventB))
      .fullOuterJoin(previously.rdd.keyBy(x => (x.rule, x.eventA, x.eventB)))
      .map(x => PairConstraint(x._1._1, x._1._2, x._1._3,
        x._2._1.getOrElse(PairConstraint("", "", "", Array.empty)).traces ++
          x._2._2.getOrElse(PairConstraint("", "", "", Array.empty)).traces))
      .toDS()

    updated_constraints.count()
    updated_constraints.persist(StorageLevel.MEMORY_AND_DISK)

    // Write updated constraints back to s3
    updated_constraints.flatMap(pc => pc.traces.map(trace => PairConstraintRow(pc.rule, pc.eventA, pc.eventB, trace)))
      .write.mode(SaveMode.Overwrite).parquet(order_path)

    //compute constraints using support and collect them
    val constraints = if (policy == null || policy.isEmpty)
      this.extractAllOrderedConstraints (updated_constraints,
                                        bUnique_traces_to_event_types,
                                        activity_matrix,
                                        support)
    else
      BranchedDeclare.extractAllOrderedConstraints (updated_constraints,
                                                    total_traces,
                                                    support,
                                                    policy,
                                                    branchingType,
                                                    branchingBound,
                                                    dropFactor = dropFactor,
                                                    filterRare = Some(filterRare),
                                                    filterBounded = filterBounded)

    updated_constraints.unpersist()
    constraints
  }

  def handle_negatives(logname: String, activity_matrix: RDD[(String, String)]): Array[(String, String)] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //get previous data if exist
    val negative_path = s"""s3a://siesta/$logname/declare/negatives.parquet/"""

    val order_path = s"""s3a://siesta/$logname/declare/order.parquet/"""

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


  def extractAllExistenceConstraints (existences: Dataset[ActivityExactly],
                                      support: Double,
                                      total_traces: Long): Array[PairConstraintSupported] = {
    existences
      .rdd
      .groupBy(_.event_type)
      .flatMap { case (event_type, activities) =>
        val l = ListBuffer[PairConstraintSupported]()

        val sortedActivities = activities.toList.sortBy(_.instances)
        var cumulativeAbsence = total_traces - activities.map(_.traces.length).sum
        var cumulativeExistence = total_traces - activities.map(_.traces.length).sum

        sortedActivities.foreach { activity =>

          //Exactly constraint
          if ((activity.traces.length.toDouble / total_traces) >= support) {
            l += PairConstraintSupported("exactly",
                                          event_type,
                                          activity.instances.toString,
                                          activity.traces.length.toDouble / total_traces)
          }
          //Existence constraint
          val s = (total_traces - cumulativeExistence).toDouble / total_traces
          if (s >= support) {
            l += PairConstraintSupported("existence",
                                          event_type,
                                          activity.instances.toString,
                                          s)
          }
          cumulativeExistence += activity.traces.length


          val as = cumulativeAbsence.toDouble / total_traces
          if (as >= support) {
            l += PairConstraintSupported("absence", event_type, activity.instances.toString, as)
          }
          cumulativeAbsence += activity.traces.length

        }

        l += PairConstraintSupported("absence", event_type, (sortedActivities.last.instances + 1).toString, 1)
        l.toList
      }.collect()
  }

  def extractAllUnorderedConstraints(merge_u: Dataset[(String, Long)],
                                     merge_i: Dataset[(String, String, Long)],
                                     activity_matrix: RDD[(String, String)],
                                     support: Double,
                                     total_traces: Long): Array[PairConstraintSupported] = {
    activity_matrix
      .map(x => (x._1, x._2))
      .keyBy(_._1)
      //key by the first activity and bring the new U[a]
      .leftOuterJoin(merge_u.rdd.keyBy(_._1))
      .map(x => {
        val eventA = x._2._1._1
        val eventB = x._2._1._2
        val key = if (eventA < eventB) eventA + eventB
        else eventB + eventA
        UnorderedHelper(eventA, eventB, x._2._2.getOrElse("", 0L)._2, 0L, 0L, key)
      })
      .keyBy(_.eventB) //group by the second activity and bring U[B]
      .leftOuterJoin(merge_u.rdd.keyBy(_._1))
      .map(x => {
        UnorderedHelper(x._2._1.eventA, x._2._1.eventB, x._2._1.ua, x._2._2.getOrElse("", 0L)._2, 0L, x._2._1.key)
      })
      .keyBy(_.key)
      .leftOuterJoin(merge_i.rdd.keyBy(x => x._1 + x._2))
      .map(x => {
        val p = x._2._1
        UnorderedHelper(p.eventA, p.eventB, p.ua, p.ub, x._2._2.getOrElse("", "", 0L)._3, p.key)
      })
      .distinct()
      .flatMap(x => parseUnorderedConstraints(x, total_traces))
      .filter(x => (x.support / total_traces) >= support)
      .map(x => PairConstraintSupported(x.rule, x.activation, x.target, x.support / total_traces))
      .collect()
  }


  private def parseUnorderedConstraints(u: UnorderedHelper,
                                        total_traces: Long): TraversableOnce[PairConstraintSupported] = {
    val l = ListBuffer[PairConstraintSupported]()
    var r: Long = total_traces - u.ua + u.pairs
    l += PairConstraintSupported("responded existence", u.eventA, u.eventB, r)
    if (u.eventA < u.eventB) { //that means that we have to calculate all rules
      r = u.ua + u.ub - u.pairs
      l += PairConstraintSupported("choice", u.eventA, u.eventB, r)
      r = total_traces - u.ua - u.ub + 2 * u.pairs
      l += PairConstraintSupported("co-existence", u.eventA, u.eventB, r)
      //exclusive_choice = total - co-existence
      l += PairConstraintSupported("exclusive choice", u.eventA, u.eventB, total_traces - r)
      //not-existence : traces where a exist and not b, traces where b exists and not a, traces where neither occur
      r = total_traces - u.pairs
      l += PairConstraintSupported("not co-existence", u.eventA, u.eventB, r)
    }
    l.toList
  }

  def extractAllOrderedConstraints(constraints: Dataset[PairConstraint],
                                   bUnique_traces_to_event_types: Broadcast[scala.collection.Map[String, Long]],
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
          c.traces.length.toDouble / bUnique_traces_to_event_types.value(c.eventA)
        } else {
          c.traces.length.toDouble  / bUnique_traces_to_event_types.value(c.eventB)
        }
        l += (c.rule, c.eventA + "|" + c.eventB, sup) // add the support to the constraint
        if (c.rule.contains("chain")) {
//          l += Constraint("chain-succession", c.eventA, c.eventB, sup)
          l += ("not-chain-succession", c.eventA + "|" + c.eventB, 1 - sup)
        } else if (c.rule.contains("alternate")) {
//          l += Constraint("alternate-succession", c.eventA, c.eventB, sup)
        } else {
//          l += Constraint("succession", c.eventA, c.eventB, sup)
          l += ("not-succession", c.eventA + "|" + c.eventB, 1 - sup)
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

}
