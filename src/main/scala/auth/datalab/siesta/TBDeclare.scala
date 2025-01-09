package auth.datalab.siesta

import auth.datalab.siesta.Structs.{Constraint, PairConstraint, PositionConstraint, TargetBranchedConstraint}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, collect_list, collect_set, countDistinct, explode, struct}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object TBDeclare {

  //////////////////////////////////////////////////////////////////////
  //               Policy-based constraints extraction                //
  //////////////////////////////////////////////////////////////////////

  def getORBranchedConstraints(constraints: Dataset[PairConstraint],
                               totalTraces: Long): Dataset[TargetBranchedConstraint] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val traceCounts: DataFrame = explodePairConstraints(constraints)

    // Group by (rule, eventA), calculate optimal suffixes
    val grouped = traceCounts
      .as[(String, String, String, Seq[String])]
      .groupByKey { case (rule, eventA, _, _) => (rule, eventA) }
      .mapGroups { case ((rule, eventA), rows) =>

        // Create a struct for each target event (eventB) and each list of traces, w.r.t. activation (eventA)
        val eventBData = rows.toSeq.map { case (_, _, eventB, uniqueTraces) =>
          (eventB, uniqueTraces)
        }

        // Find the optimal subset of eventBs
        val optimalSuffixes = findORTargets(eventBData)

        // Compute total unique traces for the optimal suffixes
        val totalUniqueTraces = optimalSuffixes.flatMap(_._2).distinct.size.toDouble

        // Create TargetBranchedConstraint
        TargetBranchedConstraint(
          rule = rule,
          activation = eventA,
          targets = optimalSuffixes.map(_._1).toArray,
          support = totalUniqueTraces / totalTraces
        )
      }

    grouped
  }

//  def getANDBranchedConstraints(constraints: Dataset[PairConstraint],
//                                totalTraces: Long): Dataset[TargetBranchedConstraint] = {
//    val spark = SparkSession.builder().getOrCreate()
//    import spark.implicits._
//
////    val traceCounts: DataFrame = explodePairConstraints(constraints)
//
//    // Explode the traces into individual trace IDs
//    val exploded = constraints.withColumn("trace", explode(col("traces")))
//
//    // Group by (rule, eventA) to create a map-like structure
//    val targetsMap = exploded
//      .groupBy("rule", "eventA")
//      .agg(collect_list(struct($"eventB", $"trace")).as("eventBTraces"))
//
//    // Define a function to compute the most frequent traces and apply findAndTargetSet
//    def processGroup(
//                      rule: String,
//                      eventA: String,
//                      eventBTraces: Seq[Row]
//                    ): Seq[(String, String, Set[String], Double)] = {
//      val traceGroups = eventBTraces.groupBy(row => row.getString(0)).map {
//        case (eventB, rows) =>
//          val traces = rows.map(_.getString(1)).toSet
//          (eventB, traces)
//      }.toSeq
//
//      // Determine the most frequent traces
//      val allTraces = traceGroups.flatMap(_._2)
//      val traceFrequencies = allTraces.groupBy(identity).view.mapValues(_.size)
//      val maxFrequency = traceFrequencies.values.max
//      val frequentTraces = traceFrequencies.filter(_._2 == maxFrequency).keys.toSet
//
//      // Prepare the targets input for findAndTargetSet
//      val targets = traceGroups.map { case (eventB, traces) => (eventB, traces) }
//
//      // Find AND-Target sets
//      val targetSets = AndTargetSetFinder.findAndTargetSet(targets, frequentTraces, threshold)
//
//      // Compute support and construct the result
//      targetSets.map { case (eventSet, commonTraces) =>
//        val support = commonTraces.size.toDouble / totalTraces
//        (rule, eventA, eventSet, support)
//      }
//    }
//
//    // Apply the processGroup function to each group
//    val result = targetsMap.flatMap { row =>
//      val rule = row.getString(0)
//      val eventA = row.getString(1)
//      val eventBTraces = row.getSeqprocessGroup(rule, eventA, eventBTraces)
//    }
//
//    result.toDS()
//  }


  //////////////////////////////////////////////////////////////////////
  //                Optimal target extraction and helpers             //
  //////////////////////////////////////////////////////////////////////

  private def explodePairConstraints(constraints: Dataset[PairConstraint]): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Explode traces to create individual rows for (rule, eventA, eventB, trace)
    val exploded = constraints
      .withColumn("trace", explode($"traces"))
      .select($"rule", $"eventA", $"eventB", $"trace")

    // Compute unique traces for each (rule, eventA, eventB)
    val traceCounts = exploded
      .groupBy($"rule", $"eventA", $"eventB")
      .agg(collect_set($"trace").as("uniqueTraces"))
    traceCounts
  }

  private def findORTargets(eventBData: Seq[(String, Seq[String])]): Seq[(String, Seq[String])] = {
    var suffixes = eventBData // Start with all eventBs
    var uniqueTraces = suffixes.flatMap(_._2).distinct.size // Total unique traces

    var sumOfReductions = 0.0
    var sumOfReductions2 = 0.0
    var countOfReductions = 0
    var reductionStd = 0.0

    // Iteratively remove eventBs causing the least reduction in unique traces
    while (suffixes.size > 1) {
      // Find the eventB that causes the smallest reduction in unique traces
      val candidateToRemove = suffixes
        .map { suffix =>
          val updatedSuffixes = suffixes.filterNot(_ == suffix) // Remove this targets
          val updatedTraces = updatedSuffixes.flatMap(_._2).distinct.size // Recalculate unique traces
          (suffix, updatedSuffixes, updatedTraces)
        }
        .minBy { case (_, _, updatedTraces) => uniqueTraces - updatedTraces }

      // Calculate the reduction in unique traces
      val reduction = uniqueTraces - candidateToRemove._3

      countOfReductions += 1
      sumOfReductions += reduction
      sumOfReductions2 += Math.pow(reduction, 2)
      // Calculate mean and standard deviation of reductions
      val reductionMean = sumOfReductions / countOfReductions
      reductionStd = Math.sqrt((sumOfReductions2 / countOfReductions) - Math.pow(reductionMean, 2))

      // If the current reduction exceeds the mean by more than 2.5 times the standard deviation, stop
      if (reduction > reductionMean + 2.5 * reductionStd) {
        return suffixes // Return the suffixes before the reduction was too large
      }

      // Update suffixes and unique trace count
      suffixes = candidateToRemove._2
      uniqueTraces = candidateToRemove._3
    }

    suffixes
  }

  private  def findANDTargets(targets: Seq[(String, Set[String])],
                                frequentTraces: Set[String],
                                threshold: Int): Seq[(Set[String], Set[String])] = {

    // Filter out the traces that are not in frequentTraces
    // and keep only targets related to at least one frequentTrace
    val filteredTargets = targets
      .map { x => (x._1, x._2.intersect(frequentTraces)) }
      .filter { x => x._2.nonEmpty }

    // Create event pairs along with their associated trace lists for quicker access later
    val eventPairs = for {
      (e1, t1) <- filteredTargets
      (e2, t2) <- filteredTargets if e1 != e2
    } yield (Set(e1, e2), t1, t2)

    val combinedTargets = mutable.Buffer.empty[(Set[String], Set[String])]
    var searchSpace = eventPairs


    while (searchSpace.nonEmpty) {
      // Calculate the support for each event pair concurrently
      val supportResults = searchSpace.map { case (pair, t1, t2) =>
        val commonTraces = t1 intersect t2
        (pair, commonTraces)
      }

      // Find the pair with the maximum support
      val maxSupportPair = supportResults.maxBy(_._2.size)

      if (maxSupportPair._2.size >= threshold) {
        // Expand the search space with new event combinations from maxSupportPair
        val expandedSearchSpace = {
          val remainingTargets = filteredTargets.map(_._1).toSet.diff(maxSupportPair._1)
          remainingTargets.map(newEvent =>
            (maxSupportPair._1 + newEvent, maxSupportPair._2, filteredTargets.find(_._1 == newEvent).get._2))
        }

        // Update searchSpace to only contain the new expanded pairs
        searchSpace = expandedSearchSpace.toSeq
      } else {
        // Add the combination with the common traces to the result
        combinedTargets += (maxSupportPair._1, maxSupportPair._2)
        searchSpace = Seq.empty // End the loop
      }
    }

    combinedTargets.toSeq
  }

  //////////////////////////////////////////////////////////////////////
  //    Extraction of branched Constraints with calculated support    //
  //////////////////////////////////////////////////////////////////////

  def extractAllOrderedConstraints (constraints: Dataset[PairConstraint],
                                    bUnique_traces_to_event_types: Broadcast[scala.collection.Map[String, Long]],
                                    activity_matrix: RDD[(String, String)],
                                    totalTraces: Long,
                                    support: Double,
                                    policy: String): Array[TargetBranchedConstraint] = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Refactor the following w.r.t. policy
    val branchedConstraints: Dataset[TargetBranchedConstraint] =
      if (policy == "OR") {
        getORBranchedConstraints(constraints, totalTraces)
      } else {
        spark.emptyDataset[TargetBranchedConstraint]
      }

    val not_chain_succession = activity_matrix.keyBy(x => {(x._1, x._2)})
      // Subtract branched constraints
      .subtractByKey(
        branchedConstraints
          .filter(_.rule.contains("chain")) // Keep only "chain" constraints
          .rdd
          .flatMap { bc =>
            // Create pairs (eventA, each eventB in the array) for subtraction
            bc.targets.map(eventB => (bc.activation, eventB))
          }
          .distinct()
          .keyBy(x => (x._1, x._2))
      )
      // Group remaining pairs by eventA
      .groupBy(_._2._1) // Group by eventA
      .map { case (eventA, pairs) =>
        // Collect all eventBs that are not chain-succeeded by eventA
        val remainingEventBs = pairs.map(_._2._2).toArray
        TargetBranchedConstraint("not-chain-succession", eventA, remainingEventBs, 1.0) // TODO: reconsider sup
      }
      .collect()

    val remaining = branchedConstraints.rdd.flatMap(c => {
        val l = ListBuffer[TargetBranchedConstraint]()

        // Calculate support based on branching rationale
        val sup = if (c.rule.contains("response")) {
          c.targets.length.toDouble / bUnique_traces_to_event_types.value(c.activation)
        } else {
          c.targets.length.toDouble / c.targets.map(bUnique_traces_to_event_types.value).sum
        }

        // Add the original constraint with support
        l += TargetBranchedConstraint(c.rule, c.activation, c.targets, sup)

        // Handle branching-specific rules
        if (c.rule.contains("chain")) {
          l += TargetBranchedConstraint("chain-succession", c.activation, c.targets, sup)
          l += TargetBranchedConstraint("not-chain-succession", c.activation, c.targets, 1 - sup)
        } else if (c.rule.contains("alternate")) {
          l += TargetBranchedConstraint("alternate-succession", c.activation, c.targets, sup)
        } else {
          l += TargetBranchedConstraint("succession", c.activation, c.targets, sup)
          l += TargetBranchedConstraint("not-succession", c.activation, c.targets, 1 - sup)
        }

        l.toList
      })
      // Key by rule, activation (eventA), and the full array of eventBs
      .keyBy(x => (x.rule, x.activation, x.targets))
      .reduceByKey((x, y) =>
        // Merge constraints by multiplying support
        TargetBranchedConstraint(x.rule, x.activation, x.targets, x.support * y.support)   // TODO: reconsider sup
      )
      // Filter out constraints below the support threshold
      .map(_._2)
      .filter(_.support >= support)
      .collect()

    not_chain_succession ++ remaining
  }

  def extractAllUnorderedConstraints(merge_u: Dataset[(String, Long)], merge_i: Dataset[(String, String, Long)],
                                     activity_matrix: RDD[(String, String)], support: Double, total_traces: Long)
  : Any = { }
}
