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
//    grouped.foreach{ x => if (x.targets.length > 2) println(x.toString) }
    grouped
  }

  def getANDBranchedConstraints(constraints: Dataset[PairConstraint],
                                totalTraces: Long,
                                threshold: Double,
                                xor: Boolean = false): Dataset[TargetBranchedConstraint] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val branchedConstraints = mutable.Buffer[TargetBranchedConstraint]()

    // Explode the traces into individual trace IDs
    val exploded = constraints.withColumn("trace", explode(col("traces")))

    // Group by (rule, eventA) to create a map-like structure
    val targetsMap = exploded
      .groupBy("rule", "eventA")
      .agg(collect_list(struct($"eventB", $"trace")).as("eventBTraces"))

    // Collect grouped data for iteration
    val groupedData = targetsMap.collect()

    groupedData.foreach { row =>
      val frequentTraces = computeFrequentTraces(row)

      val targetSuperSet: Seq[(String, Set[String])] = {
        row.getAs[Seq[Row]]("eventBTraces").map { entry =>
            val eventB = entry.getAs[String]("eventB")
            val trace = entry.getAs[String]("trace")
            (eventB, Set(trace)) // Initialize each trace as a Set
          }
          .groupBy(_._1) // Group by eventB
          .map { case (eventB, entries) =>
            (eventB, entries.flatMap(_._2).toSet) // Merge sets of traces
          }
          .toSeq
      }



      val Targets = if (xor) findXORTargets(targetSuperSet, frequentTraces, threshold)
                    else findANDTargets(targetSuperSet, frequentTraces, threshold)

      // Map results into TargetBranchedConstraint objects
      Targets.map { case (targetSet, associatedTraces) =>
        val support = associatedTraces.size.toDouble / totalTraces
        val tbConstraint =
          TargetBranchedConstraint (
            rule = row.getString(0),
            activation = row.getString(1),
            targets = targetSet.toArray,
            support = support
          )
        branchedConstraints += tbConstraint
      }
    }
    val bc = spark.createDataset(branchedConstraints)
    bc.show()
    bc
  }

  def getXORBranchedConstraints(constraints: Dataset[PairConstraint],
                                totalTraces: Long,
                                threshold: Double): Dataset[TargetBranchedConstraint] = {
    getANDBranchedConstraints(constraints,totalTraces,threshold,xor=true)
  }

  //////////////////////////////////////////////////////////////////////
  //                Optimal target extraction and helpers             //
  //////////////////////////////////////////////////////////////////////

  private def computeFrequentTraces(row: Row): Set[String] = {
    val eventBTraces = row.getAs[Seq[Row]]("eventBTraces")
    // Extract traces from eventBTraces
    val traces = eventBTraces.map(_.getAs[String]("trace"))
    // Compute trace frequencies
    val traceFrequencies = traces.groupBy(identity).mapValues(_.size)
    // Find the maximum frequency
    val maxFrequency = traceFrequencies.values.max
    // Find traces with the maximum frequency
    traceFrequencies.filter(_._2 == maxFrequency).keys.toSet
  }

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
                                threshold: Double): Seq[(Set[String], Set[String])] = {

    // Filter out the traces that are not in frequentTraces
    // and keep only targets related to at least one frequentTrace
    val filteredTargets = targets
      .map { x => (x._1, x._2.intersect(frequentTraces)) }
      .filter { x => x._2.nonEmpty }

    for (x <- filteredTargets) {
      for (y <- targets) {
        if (x._1 == y._1) {
          println(x._1 + " " + x._2.size + " " + y._2.size)
        }
      }
    }

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
        combinedTargets += Tuple2(maxSupportPair._1, maxSupportPair._2)
        // Expand the search space with new event combinations from maxSupportPair
        val expandedSearchSpace = {
          val remainingTargets = filteredTargets.map(_._1).toSet.diff(maxSupportPair._1)
          remainingTargets.map(newEvent =>
            (maxSupportPair._1 + newEvent, maxSupportPair._2, filteredTargets.find(_._1 == newEvent).get._2))
        }

        // Update searchSpace to only contain the new expanded pairs
        searchSpace = expandedSearchSpace.toSeq
      } else {
        searchSpace = Seq.empty // End the loop
      }
    }

    combinedTargets
  }

  private def findXORTargets(targets: Seq[(String, Set[String])],
                             frequentTraces: Set[String],
                             threshold: Double): Seq[(Set[String], Set[String])] = {

    // Assign a unique index to each trace in frequentTraces
    val traceToIndex: Map[String, Int] = frequentTraces.toSeq.zipWithIndex.toMap
    val indexToTrace: Map[Int, String] = traceToIndex.map(_.swap)

    // Convert each target's trace set to a bitmap
    val filteredTargets = targets
      .map { case (target, traces) =>
        val bitmap = new java.util.BitSet()
        traces.intersect(frequentTraces).foreach { trace =>
          bitmap.set(traceToIndex(trace))
        }
        (target, bitmap)
      }
      .filter { case (_, bitmap) => !bitmap.isEmpty }

    // Create event pairs along with their associated bitmaps for quicker access later
    val eventPairs = for {
      (e1, b1) <- filteredTargets
      (e2, b2) <- filteredTargets if e1 != e2
    } yield (Set(e1, e2), b1, b2)

    val combinedTargets = mutable.Buffer.empty[(Set[String], Set[String])]
    var searchSpace = eventPairs

    while (searchSpace.nonEmpty) {
      // Calculate the exclusive traces for each event pair concurrently
      val exclusiveResults = searchSpace.map { case (pair, b1, b2) =>
        val exclusiveBitmap = new java.util.BitSet()
        exclusiveBitmap.or(b1)
        exclusiveBitmap.xor(b2)
        (pair, exclusiveBitmap)
      }

      // Find the pair with the maximum exclusive traces
      val maxExclusivePair = exclusiveResults.maxBy { case (_, bitmap) => bitmap.cardinality() }

      if (maxExclusivePair._2.cardinality() >= threshold) {
        combinedTargets += Tuple2(maxExclusivePair._1, maxExclusivePair._2.stream().toArray.map(indexToTrace).toSet)
        // Expand the search space with new event combinations from maxExclusivePair
        val expandedSearchSpace = {
          val remainingTargets = filteredTargets.map(_._1).toSet.diff(maxExclusivePair._1)
          remainingTargets.flatMap { newEvent =>
            val newEventBitmap = filteredTargets.find(_._1 == newEvent).get._2
              Some((maxExclusivePair._1 + newEvent, maxExclusivePair._2, newEventBitmap))
          }
        }
        // Update searchSpace to only contain the new expanded pairs
        searchSpace = expandedSearchSpace.toSeq
      } else {
        searchSpace = Seq.empty
      }
    }

    combinedTargets
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
      } else if (policy == "AND") {
        getANDBranchedConstraints(constraints, totalTraces, support)
      } else if (policy == "XOR") {
        getXORBranchedConstraints(constraints, totalTraces, support)
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
  : Array[TargetBranchedConstraint] = {
    Array.empty[auth.datalab.siesta.Structs.TargetBranchedConstraint]
  }
}
