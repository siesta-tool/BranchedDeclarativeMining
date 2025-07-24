package auth.datalab.siesta

import auth.datalab.siesta.QuickStructs._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel


import scala.collection.mutable.ListBuffer

object QuickMining {


  def extract_positions(new_events: Dataset[Event], logname: String, complete_traces_that_changed: Dataset[Event],
                        bChangedTraces: Broadcast[scala.collection.Map[String, (Int, Int)]],
                        support: Double, total_traces: Long): Array[PositionConstraint] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //get previous data if exist
    val position_path = s"""s3a://siesta/$logname/declare_quick/position.parquet/"""

    val previously = try {
      spark.read.parquet(position_path).as[PositionConstraint]
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[PositionConstraint]
    }

    val plusConstraint: Dataset[PositionConstraint] = new_events.map(x => {
        if (x.pos == 0) {
          Some(PositionConstraint("first", x.event_type, 1L))
        }
        else if (bChangedTraces.value.getOrElse(x.trace_id, (-1, -1))._2 == x.pos) { //this is the last event
          Some(PositionConstraint("last", x.event_type, 1L))
        }
        else {
          null
        }
      }).filter(_.isDefined)
      .map(_.get)

    val endPosMinus = complete_traces_that_changed
      .filter(x => {
        bChangedTraces.value(x.trace_id)._1 - 1 == x.pos
      })
      .map(x => PositionConstraint("last", x.event_type, -1L))

    val new_positions_constraints = plusConstraint.union(endPosMinus)
      .union(previously)
      .rdd
      .keyBy(x => (x.rule, x.event_type))
      .reduceByKey((x, y) => PositionConstraint(x.rule, x.event_type, x.occurrences + y.occurrences))
      .map(_._2)

    new_positions_constraints.count()
    new_positions_constraints.persist(StorageLevel.MEMORY_AND_DISK)

    new_positions_constraints.toDS()
      .write
      .mode(SaveMode.Overwrite)
      .parquet(position_path)

    val response = this.extract_all_position_constraints(new_positions_constraints, support, total_traces)

    new_positions_constraints.unpersist()
    response
  }


  def extract_existence(logname: String, complete_traces_that_changed: Dataset[Event],
                        bChangedTraces: Broadcast[scala.collection.Map[String, (Int, Int)]],
                        support: Double, total_traces: Long): Array[ExistenceConstraint] = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //get previous data if exist
    val existence_path = s"""s3a://siesta/$logname/declare_quick/existence.parquet/"""

    val previously = try {
      spark.read.parquet(existence_path)
        .map(x => ActivityExactly(x.getString(0), x.getInt(1), x.getLong(2)))
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[ActivityExactly]
    }

    //extract changes from the new ones: add news and remove previous records
    val changesInConstraints = complete_traces_that_changed
      .rdd
      .groupBy(_.trace_id)
      .flatMap(t => {
        val added_events = bChangedTraces.value(t._1) //gets starting and ending position of the new events

        val all_trace: Map[String, Int] = t._2.map(e => (e.event_type, 1)).groupBy(_._1).mapValues(_.size)

        val previousValues: Map[String, Int] = if (added_events._1 != 0) { //there are previous events from this trace
          t._2.filter(_.pos < added_events._1).map(e => (e.event_type, 1)).groupBy(_._1).mapValues(_.size)
        } else {
          Map.empty[String, Int]
        }
        val l = ListBuffer[ActivityExactly]()
        all_trace.foreach(aT => {
          l += ActivityExactly(aT._1, aT._2, 1L)
          if (previousValues.contains(aT._1)) { // this activity existed at least once in the previous trace
            l += ActivityExactly(aT._1, previousValues(aT._1), -1L)
          }
        })
        l.toList
      })

    val merged_constraints = changesInConstraints.union(previously.rdd)
      .keyBy(x => (x.event_type, x.occurrences))
      .reduceByKey((x, y) => ActivityExactly(x.event_type, x.occurrences, x.contained + y.contained))
      .map(_._2)
      .toDS()

    merged_constraints.count()
    merged_constraints.persist(StorageLevel.MEMORY_AND_DISK)

    merged_constraints.toDF()
      .write.mode(SaveMode.Overwrite)
      .parquet(existence_path)

    val response = this.extract_all_existence_constraints(merged_constraints, support, total_traces)
    merged_constraints.unpersist()

    response
  }

  def extract_unordered(logname: String, complete_traces_that_changed: Dataset[Event],
                        bChangedTraces: Broadcast[scala.collection.Map[String, (Int, Int)]],
                        activity_matrix: RDD[(String, String)],
                        support: Double, total_traces: Long): Array[PairConstraint] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //get previous U[x] if exist
    val u_path = s"""s3a://siesta/$logname/declare_quick/unorder/u.parquet/"""

    val previously = try {
      spark.read.parquet(u_path)
        .map(x => (x.getString(0), x.getLong(1)))
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[(String, Long)]
    }

    //calculate new event types in the traces that changed
    val u_new: RDD[(String, Long)] = complete_traces_that_changed //this will produce rdd (event_type, #new occurrences)
      .rdd
      .groupBy(_.trace_id)
      .flatMap(t => { //find new event types per traces
        val new_positions = bChangedTraces.value(t._1)
        val prevEvents: Set[String] = if (new_positions._1 != 0) {
          t._2.map(_.event_type).toArray.slice(0, new_positions._1 - 1).toSet
        } else {
          Set.empty[String]
        }
        t._2.toArray
          .slice(new_positions._1, new_positions._2 + 1) //+1 because last one is exclusive
          .map(_.event_type)
          .filter(e => !prevEvents.contains(e))
          .distinct
          .map(e => (e, 1L)) //mapped to event type,1 (they will be counted later)
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
    val i_path = s"""s3a://siesta/$logname/declare_quick/unorder/i.parquet/"""

    val previously_i = try {
      spark.read.parquet(i_path)
        .map(x => (x.getString(0), x.getString(1), x.getLong(2)))
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[(String, String, Long)]
    }

    val new_pairs = complete_traces_that_changed
      .rdd
      .groupBy(_.trace_id)
      .flatMap(t => { // find new event types per traces
        val new_positions = bChangedTraces.value(t._1)
        var prevEvents: Set[String] = if (new_positions._1 != 0) {
          t._2.map(_.event_type).toArray.slice(0, new_positions._1 - 1).toSet
        } else {
          Set.empty[String]
        }
        val new_events = t._2.toArray
          .slice(new_positions._1, new_positions._2 + 1) // +1 because last one is exclusive
          .map(_.event_type)
          .toSet

        val l = ListBuffer[(String, String, Long)]()

        // Iterate over new events and previous events
        for (e1 <- new_events if !prevEvents.contains(e1)) {
          for (e2 <- prevEvents) {
            if (e1 < e2) { l += ((e1, e2, 1L)) } // Add to the list if e1 < e2 and they're unique
            else if (e2 > e1) { l += ((e2,e1,1L)) }
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


    //    //extract pairs that didn't exist before in the traces
    //    val new_pairs = complete_pairs_that_changed
    //      .rdd
    //      .filter(x => x.eventA != x.eventB)
    //      .groupBy(_.trace_id)
    //      .flatMap(t => {
    //        val new_positions = bChangedTraces.value(t._1)
    //        val prev_pairs = t._2. //pairs that concluded in the previous batches
    //          filter(_.positionB < new_positions._1)
    //          .map(x => (x.eventA, x.eventB))
    //          .toSet
    //        val news = t._2 //new pairs that concluded in this batch
    //          .filter(_.positionB >= new_positions._1) //for  new events
    //          .map(x => { //swap positions in order to search for new traces that contain either (a,b) and (b,a)
    //            if (x.eventA > x.eventB) {
    //              PairFull(x.eventB, x.eventA, x.trace_id, x.positionB, x.positionA)
    //            } else {
    //              x
    //            }
    //          })
    //          .filter(x => !prev_pairs.contains((x.eventA, x.eventB)) && !prev_pairs.contains((x.eventB, x.eventA)))
    //        news
    //      })
    //      .map(x => (x.eventA, x.eventB, x.trace_id))
    //      .distinct()
    //
    //    new_pairs.count()
    //    new_pairs.persist(StorageLevel.MEMORY_AND_DISK)
    //
    //    val merge_i = new_pairs
    //      .map(x => (x._1, x._2, 1L))
    //      .keyBy(x => (x._1, x._2))
    //      .reduceByKey((x, y) => (x._1, x._2, x._3 + y._3))
    //      .fullOuterJoin(previously_i.rdd.keyBy(x => (x._1, x._2)))
    //      .map(x => {
    //        val total = x._2._1.getOrElse(("", "", 0L))._3 + x._2._2.getOrElse(("", "", 0L))._3
    //        (x._1._1, x._1._2, total)
    //      })
    //      .toDS()

    merge_i.count()
    merge_i.persist(StorageLevel.MEMORY_AND_DISK)
    merge_i.write.mode(SaveMode.Overwrite).parquet(i_path)

    val c = activity_matrix
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
      .flatMap(x => extract_unordered_constraints(x, total_traces))
      .filter(x => (x.occurrences / total_traces) >= support)
      .map(x => PairConstraint(x.rule, x.eventA, x.eventB, x.occurrences / total_traces))
      .collect()

    merge_u.unpersist()
    merge_i.unpersist()
    new_pairs.unpersist()

    c


  }

  def extract_ordered(logname: String, complete_traces_that_changed: Dataset[Event],
                      bChangedTraces: Broadcast[scala.collection.Map[String, (Int, Int)]],
                      bUnique_traces_to_event_types: Broadcast[scala.collection.Map[String, Long]],
                      activity_matrix: RDD[(String, String)],
                      support: Double): Array[PairConstraint] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //get previous data if exist
    val order_path = s"""s3a://siesta/$logname/declare_quick/order.parquet/"""

    val previously = try {
      spark.read.parquet(order_path)
        .map(x => PairConstraint(x.getString(0), x.getString(1), x.getString(2), x.getDouble(3)))
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[PairConstraint]
    }

    val new_ordered_relations = complete_traces_that_changed.rdd
      .groupBy(_.trace_id)
      .flatMap(t => {
        val new_pos = bChangedTraces.value(t._1)
        //activities -> reverse sorted positions of the events that correspond to them
        val events: Map[String, Seq[Int]] = t._2.map(e => (e.event_type, e.pos))
          .groupBy(_._1)
          .mapValues(e => e.map(_._2).toSeq.sortWith((a, b) => a > b))
        val l = ListBuffer[PairConstraint]()
        // new events are the ones that will extract the new rules
        // precedence loop
        for (i <- new_pos._1 to new_pos._2) {
          val current_event_type = t._2.toSeq(i).event_type
          val all_previous = events(current_event_type).filter(x => x < i)
          val prev = all_previous match {
            case Nil => 0
            case _ => all_previous.max
          }
          // precedence loop
          events.keySet.filter(x => x != current_event_type)
            .foreach(activity_a => {
              val valid_a = events(activity_a)
                .filter(pos_a => pos_a < i)
              if (valid_a.nonEmpty) {
                val pos_a = valid_a.max
                if (pos_a < i) {
                  l += PairConstraint("precedence", activity_a, current_event_type, 1.0)
                  if (pos_a >= prev) {
                    l += PairConstraint("alternate-precedence", activity_a, current_event_type, 1.0)
                    if (pos_a == i - 1) {
                      l += PairConstraint("chain-precedence", activity_a, current_event_type, 1.0)
                    }
                  }
                }
              }
            })

          //response loop
          events.keySet.filter(x => x != current_event_type)
            .foreach(activity_a => {
              var largest = true
              events(activity_a).foreach(pos_a => {
                if (pos_a < i && pos_a >= prev) {
                  l += PairConstraint("response", activity_a, current_event_type, 1.0)
                  if (largest) {
                    l += PairConstraint("alternate-response", activity_a, current_event_type, 1.0)
                    largest = false
                    if (pos_a == i - 1) {
                      l += PairConstraint("chain-response", activity_a, current_event_type, 1.0)
                    }
                  }
                }
              })
            })
        }
        l.toList
      })
      .keyBy(x => (x.rule, x.eventA, x.eventB))
      .reduceByKey((x, y) => PairConstraint(x.rule, x.eventA, x.eventB, x.occurrences + y.occurrences))
      .map(_._2)

    //merge with previous values if exist
    val updated_constraints = new_ordered_relations
      .keyBy(x => (x.rule, x.eventA, x.eventB))
      .fullOuterJoin(previously.rdd.keyBy(x => (x.rule, x.eventA, x.eventB)))
      .map(x => PairConstraint(x._1._1, x._1._2, x._1._3,
        x._2._1.getOrElse(PairConstraint("", "", "", 0L)).occurrences +
          x._2._2.getOrElse(PairConstraint("", "", "", 0L)).occurrences))
      .toDS()

    updated_constraints.count()
    updated_constraints.persist(StorageLevel.MEMORY_AND_DISK)

    //    write updated constraints back to s3
    updated_constraints.write.mode(SaveMode.Overwrite).parquet(order_path)

    //compute constraints using support and collect them
    val constraints = this.extract_all_ordered_constraints(updated_constraints, bUnique_traces_to_event_types, activity_matrix, support)

    updated_constraints.unpersist()
    constraints

  }

  def handle_negatives(logname: String, activity_matrix: RDD[(String, String)]): Array[(String, String)] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //get previous data if exist
    val negative_path = s"""s3a://siesta/$logname/declare_quick/negatives.parquet/"""

    val order_path = s"""s3a://siesta/$logname/declare_quick/order.parquet/"""

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

  private def extract_unordered_constraints(u: UnorderedHelper, total_traces: Long): TraversableOnce[PairConstraint] = {
    val l = ListBuffer[PairConstraint]()
    var r: Long = total_traces - u.ua + u.pairs
    l += PairConstraint("responded existence", u.eventA, u.eventB, r)
    if (u.eventA < u.eventB) { //that means that we have to calculate all rules
      r = u.ua + u.ub - u.pairs
      l += PairConstraint("choice", u.eventA, u.eventB, r)
      r = total_traces - u.ua - u.ub + 2 * u.pairs
      l += PairConstraint("co-existence", u.eventA, u.eventB, r)
      //exclusive_choice = total - co-existence
      l += PairConstraint("exclusive choice", u.eventA, u.eventB, total_traces - r)
      //not-existence : traces where a exist and not b, traces where b exists and not a, traces where neither occur
      r = total_traces - u.pairs
      l += PairConstraint("not co-existence", u.eventA, u.eventB, r)
    }
    l.toList
  }

  private def extract_all_position_constraints(constraints: RDD[PositionConstraint], support: Double, total_traces: Long): Array[PositionConstraint] = {
    constraints
      .filter(x => (x.occurrences / total_traces) >= support)
      .map(x => PositionConstraint(x.rule, x.event_type, x.occurrences / total_traces))
      .collect()
  }

  private def extract_all_existence_constraints(existences: Dataset[ActivityExactly], support: Double, total_traces: Long): Array[ExistenceConstraint] = {
    existences
      .rdd
      .groupBy(_.event_type)
      .flatMap { case (event_type, activities) =>
        val l = ListBuffer[ExistenceConstraint]()

        val sortedActivities = activities.toList.sortBy(_.occurrences)
        var cumulativeAbsence = total_traces - activities.map(_.contained).sum
        var cumulativeExistence = total_traces - activities.map(_.contained).sum

        sortedActivities.foreach { activity =>

          //Exactly constraint
          if ((activity.contained.toDouble / total_traces) >= support) {
            l += ExistenceConstraint("exactly", event_type, activity.occurrences, activity.contained.toDouble / total_traces)
          }
          //Existence constraint
          val s = (total_traces - cumulativeExistence).toDouble / total_traces
          if (s >= support) {
            l += ExistenceConstraint("existence", event_type, activity.occurrences, s)
          }
          cumulativeExistence += activity.contained


          val as = cumulativeAbsence.toDouble / total_traces
          if (as >= support) {
            l += ExistenceConstraint("absence", event_type, activity.occurrences, as)
          }
          cumulativeAbsence += activity.contained

        }

        l += ExistenceConstraint("absence", event_type, sortedActivities.last.occurrences + 1, 1)
        l.toList
      }.collect()
  }

  private def extract_all_unorder_constraints(constraints: Dataset[PairConstraint], support: Double, total_traces: Long): Array[PairConstraint] = {
    constraints
      .rdd
      .flatMap(x => {
        val l = ListBuffer[PairConstraint]()
        if (x.rule == "co-existence") {
          l += PairConstraint("exclusive choice", x.eventA, x.eventB, total_traces - x.occurrences)
        }
        l += x
        l.toList
      }).filter(x => (x.occurrences / total_traces) >= support)
      .map(x => PairConstraint(x.rule, x.eventA, x.eventB, x.occurrences / total_traces))
      .collect()


  }

  private def extract_all_ordered_constraints(constraints: Dataset[PairConstraint],
                                              bUnique_traces_to_event_types: Broadcast[scala.collection.Map[String, Long]],
                                              activity_matrix: RDD[(String, String)],
                                              support: Double): Array[PairConstraint] = {


    val not_chain_succession = activity_matrix
      .filter(x => x._1 != x._2)
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
      .map(x => PairConstraint("not-chain-succession", x._2._1, x._2._2, 1.0))
      .collect()

    val remaining = constraints.rdd.flatMap(c => {
        val l = ListBuffer[PairConstraint]()
        val sup = if (c.rule.contains("response")) {
          c.occurrences / bUnique_traces_to_event_types.value(c.eventA)
        } else {
          c.occurrences / bUnique_traces_to_event_types.value(c.eventB)
        }
        l += PairConstraint(c.rule, c.eventA, c.eventB, sup) // add the support to the constraint
        if (c.rule.contains("chain")) {
          l += PairConstraint("chain-succession", c.eventA, c.eventB, sup)
          l += PairConstraint("not-chain-succession", c.eventA, c.eventB, 1 - sup)
        } else if (c.rule.contains("alternate")) {
          l += PairConstraint("alternate-succession", c.eventA, c.eventB, sup)
        } else {
          l += PairConstraint("succession", c.eventA, c.eventB, sup)
          l += PairConstraint("not-succession", c.eventA, c.eventB, 1 - sup)
        }
        l.toList
      })
      .keyBy(x => (x.rule, x.eventA, x.eventB))
      .reduceByKey((x, y) => PairConstraint(x.rule, x.eventA, x.eventB, x.occurrences * y.occurrences))
      .map(_._2)
      .filter(_.occurrences >= support) //calculate the final filtering at the end
      .collect()

    not_chain_succession ++ remaining

  }

}