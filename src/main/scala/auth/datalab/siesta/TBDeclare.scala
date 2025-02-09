package auth.datalab.siesta

import auth.datalab.siesta.Structs.{Constraint, PairConstraint, PositionConstraint, TargetBranchedConstraint}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, collect_list, collect_set, countDistinct, explode, struct}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

import scala.collection.mutable

object TBDeclare {

  //////////////////////////////////////////////////////////////////////
  //               Policy-based constraints extraction                //
  //////////////////////////////////////////////////////////////////////

  def getORBranchedConstraints(constraints: Dataset[PairConstraint],
                               totalTraces: Long,
                               branchingBound: Int = 0): Dataset[TargetBranchedConstraint] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val traceCounts: DataFrame = explodePairConstraints(constraints)

    // Group by (rule, eventA), calculate optimal suffixes
    val grouped = traceCounts
      .as[(String, String, String, Seq[String])]
      .groupByKey { case (rule, eventA, _, _) => (rule, eventA) }
      .mapGroups { case ((rule, eventA), rows) =>

        // Create a struct for each target event (eventB) and its list of traces w.r.t. activation (eventA)
        val eventBData = rows.toSeq.map { case (_, _, eventB, uniqueTraces) =>
          (eventB, uniqueTraces.toSet) // Ensure trace sets are properly handled
        }

        // Find the optimal subset of eventBs
        val optimalResult = findORTargets(eventBData, threshold = 0.0, branchingBound) // Ensure threshold is passed

        // Extract targets and traces from the result
        val optimalTargets = optimalResult.flatMap(_._1) // Extract event names
        val optimalTraces = optimalResult.flatMap(_._2) // Extract associated traces

        // Compute total unique traces for the optimal suffixes
        val totalUniqueTraces = optimalTraces.distinct.size.toDouble

        // Create TargetBranchedConstraint
        TargetBranchedConstraint(
          rule = rule,
          activation = eventA,
          targets = optimalTargets.toArray,
          support = totalUniqueTraces / totalTraces
        )
      }
//    grouped.show()
    grouped
  }


  def getANDBranchedConstraints(constraints: Dataset[PairConstraint],
                                totalTraces: Long,
                                threshold: Double,
                                branchingBound: Int = 0,
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



      val Targets = if (xor) findXORTargets(targetSuperSet, frequentTraces, threshold, branchingBound)
                    else findANDTargets(targetSuperSet, frequentTraces, threshold, branchingBound)

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
//    bc.show()
    bc
  }

  def getXORBranchedConstraints(constraints: Dataset[PairConstraint],
                                totalTraces: Long,
                                threshold: Double,
                                branchingBound: Int = 0): Dataset[TargetBranchedConstraint] = {
    getANDBranchedConstraints(constraints,totalTraces,threshold,branchingBound,xor=true)
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

  private def findORTargets(targets: Seq[(String, Set[String])],
                            threshold: Double,
                            branchingBound: Int = 0): Seq[(Set[String], Set[String])] = {

    var targetSuperSet = targets.map(_._1).toSet // Start with all targets
    var traceCoverage = targets.flatMap(_._2).toSet // Total unique traces covered
    var lastSafeTargetSet = targetSuperSet

    var sumOfReductions = 0.0
    var sumOfReductions2 = 0.0
    var countOfReductions = 0
    var reductionStd = 0.0

    while (targetSuperSet.size > 1) {
      // Find the target whose removal causes the smallest reduction in unique traces
      val candidateToRemove = targetSuperSet
        .map { target =>
          val updatedSet = targetSuperSet - target
          val updatedTraces = targets.filter(t => updatedSet.contains(t._1)).flatMap(_._2).toSet
          (target, updatedSet, updatedTraces)
        }
        .minBy { case (_, _, updatedTraces) => traceCoverage.size - updatedTraces.size }

      val reduction = traceCoverage.size - candidateToRemove._3.size

      countOfReductions += 1
      sumOfReductions += reduction
      sumOfReductions2 += Math.pow(reduction, 2)

      val reductionMean = sumOfReductions / countOfReductions
      reductionStd = Math.sqrt((sumOfReductions2 / countOfReductions) - Math.pow(reductionMean, 2))

      // Stop at branching bound
      if (branchingBound > 0 && targetSuperSet.size <= branchingBound) {
        return Seq((targetSuperSet, traceCoverage))
      }

      // Stop before a major drop
      if (branchingBound == 0 && reduction > reductionMean + 2.5 * reductionStd) {
        return Seq((lastSafeTargetSet, traceCoverage))
      }

      lastSafeTargetSet = targetSuperSet
      targetSuperSet = candidateToRemove._2
      traceCoverage = candidateToRemove._3
    }

    Seq((targetSuperSet, traceCoverage))
  }


  private  def findANDTargets(targets: Seq[(String, Set[String])],
                                frequentTraces: Set[String],
                                threshold: Double,
                                branchingBound: Int = 0): Seq[(Set[String], Set[String])] = {

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
      combinedTargets += Tuple2(maxSupportPair._1, maxSupportPair._2)

      if (maxSupportPair._1.size > branchingBound && maxSupportPair._2.size >= threshold) {
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
                             threshold: Double,
                             branchingBound: Int = 0): Seq[(Set[String], Set[String])] = {

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
      combinedTargets += Tuple2(maxExclusivePair._1, maxExclusivePair._2.stream().toArray.map(indexToTrace).toSet)

      if (maxExclusivePair._1.size > branchingBound && maxExclusivePair._2.cardinality() >= threshold) {
//        combinedTargets += Tuple2(maxExclusivePair._1, maxExclusivePair._2.stream().toArray.map(indexToTrace).toSet)
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
                                    policy: String,
                                    branchingBound: Int = 0): Array[TargetBranchedConstraint] = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    if (policy == "OR") {
      getORBranchedConstraints(constraints, totalTraces, branchingBound).collect()
    } else if (policy == "AND") {
      getANDBranchedConstraints(constraints, totalTraces, support, branchingBound).collect()
    } else if (policy == "XOR") {
      getXORBranchedConstraints(constraints, totalTraces, support, branchingBound).collect()
    } else {
      spark.emptyDataset[TargetBranchedConstraint].collect()
    }
  }

  def extractAllUnorderedConstraints(merge_u: Dataset[(String, Long)], merge_i: Dataset[(String, String, Long)],
                                     activity_matrix: RDD[(String, String)], support: Double, total_traces: Long)
  : Array[TargetBranchedConstraint] = {
    Array.empty[auth.datalab.siesta.Structs.TargetBranchedConstraint]
  }
}
