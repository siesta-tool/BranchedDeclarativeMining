package auth.datalab.siesta

import auth.datalab.siesta.Structs._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, count, lit, sum}
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

object LooseDeclareMining {


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

              for (e <- orderedEvents) {
                if (e.eventType == eventA) {
                  aSeen = true
                }
                else if (e.eventType == eventB && aSeen) {
                  isValid = true // A occurred before B
                  aSeen = false // Reset A seen state after B
                } else
                if (e.eventType == eventB && !aSeen) {
                  isValid = false // `B` occurred before any `A`
                  aSeen = false // Reset A seen state
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

              for (e <- orderedEvents if (e.eventType == eventA || e.eventType == eventB)) {
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

              for (e <- orderedEvents if (e.eventType == eventA || e.eventType == eventB)) {
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
              var isSatisfied = false

              for ((e, i) <- orderedEvents.zipWithIndex if e.eventType == eventA) {
                if (i + 1 >= orderedEvents.length) {
                  if (orderedEvents(i + 1).eventType == eventB) {
                  isSatisfied = true // If the next event is not B, it's a violation
                  }
                }
              }

              // After loop, if there was a B not next to an A, it's a violation
              if (isSatisfied) Some(PairConstraintRow("chain-response", eventA, eventB, traceId))
              else Some(PairConstraintRow("not-chain-succession", eventA, eventB, traceId))
          }

        val chainPrecedence: Seq[PairConstraintRow] =
          alternatePrecedence.flatMap {
            case PairConstraintRow(_, eventA, eventB, traceId) =>
              var isSatisfied = false

              for ((e, i) <- orderedEvents.zipWithIndex if e.eventType == eventB) {
                if (i - 1 < 0)
                  if (orderedEvents(i - 1).eventType == eventA)
                    isSatisfied = true // If the previous event is not A, it's a violation
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