package auth.datalab.siesta.mining

import auth.datalab.siesta.model.Structs.{PairConstraint, TargetBranchedPairConstraint, PairConstraintBits}
import org.apache.spark.sql.{Dataset, SparkSession}
import java.util.BitSet
import scala.jdk.CollectionConverters._

object OrBranchingMiner {

  /**
   * Case class to maintain incremental statistics for support drops.
   * Uses online algorithms to efficiently update mean and standard deviation.
   * 
   * @param count Number of drops observed
   * @param sum Sum of all drops
   * @param sumOfSquares Sum of squares of all drops (for variance calculation)
   */
  private case class DropStats(count: Int, sum: Double, sumOfSquares: Double) {
    def mean: Double = if (count > 0) sum / count else 0.0
    
    def variance: Double = if (count > 1) {
      (sumOfSquares - (sum * sum / count)) / count
    } else 0.0
    
    def standardDeviation: Double = math.sqrt(variance)
    
    def addDrop(drop: Double): DropStats = {
      DropStats(count + 1, sum + drop, sumOfSquares + (drop * drop))
    }
    
    def shouldStop(latestDrop: Double, dropFactor: Double): Boolean = {
      if (count < 2) {
        false  // Need at least 2 drops to calculate statistics
      } else {
        val threshold = mean + (dropFactor * standardDeviation)
        latestDrop > threshold && latestDrop > 0
      }
    }
  }
  
  /**
   * Determines if target set reduction should stop due to a major support drop.
   * Uses incremental statistical analysis of support drops between levels to detect when
   * the drop becomes significantly larger than the historical pattern.
   * 
   * @param prevSupports Previous level's support values for all candidates
   * @param currentSupports Current level's support values for all candidates
   * @param dropStats Current statistics for drops (maintained incrementally)
   * @param dropFactor Multiplier for standard deviation in threshold calculation
   * @return (shouldStop, updatedDropStats) tuple
   */
  private def shouldStopExtension(
      prevSupports: Seq[Double],
      currentSupports: Seq[Double], 
      dropStats: DropStats,
      dropFactor: Double
    ): (Boolean, DropStats) = {
    
    if (prevSupports.isEmpty || currentSupports.isEmpty) {
      return (false, dropStats)
    }
    
    // Use median support as it's more robust to outliers than max
    val prevMedian = if (prevSupports.length % 2 == 0) {
      val sorted = prevSupports.sorted
      (sorted(sorted.length / 2 - 1) + sorted(sorted.length / 2)) / 2.0
    } else {
      val sorted = prevSupports.sorted
      sorted(sorted.length / 2)
    }
    
    val currentMedian = if (currentSupports.length % 2 == 0) {
      val sorted = currentSupports.sorted
      (sorted(sorted.length / 2 - 1) + sorted(sorted.length / 2)) / 2.0
    } else {
      val sorted = currentSupports.sorted
      sorted(sorted.length / 2)
    }
    
    val currentDrop = prevMedian - currentMedian
    val updatedStats = dropStats.addDrop(currentDrop)
    val shouldStop = updatedStats.shouldStop(currentDrop, dropFactor)
    (shouldStop, updatedStats)
  }

  private def topDownMining(
      rule: String, 
      source: String, 
      targetToBits: Map[String, BitSet], 
      minSupport: Double, 
      maxTargets: Int, 
      bcIntToTrace: org.apache.spark.broadcast.Broadcast[Array[String]],
      dropFactor: Option[Double]
    ): Option[TargetBranchedPairConstraint] = {
    
    // Helper: from BitSet -> (support, Set[String])
    def supportAndTraces(bits: BitSet): (Int, Set[String]) = {
      val idxs: Array[Int] = bits.stream().toArray 
      val traces: Set[String] = idxs.map(i => bcIntToTrace.value(i)).toSet
      (idxs.length, traces)
    }

    // Check if we're in unbounded mode
    val isUnbounded = maxTargets == Int.MaxValue
    
    // Track incremental drop statistics (only when needed for unbounded mode with drop monitoring)
    var dropStats = DropStats(0, 0.0, 0.0)
    var prevSupports: Option[Seq[Double]] = None
    
    // Start with the best target set of appropriate size
    val allTargets = targetToBits.keys.toList.sorted
    
    // Start from the best target set and work down
    var currentLevel: Seq[(List[String], BitSet)] = if (allTargets.length <= maxTargets) {
      // If we have fewer targets than maxTargets, use all targets
      val fullSet = targetToBits.values.reduce { (a, b) =>
        val union = a.clone().asInstanceOf[BitSet]
        union.or(b)
        union
      }
      Seq((allTargets, fullSet))
    } else {
      // If we have more targets than maxTargets, find the best combination of size maxTargets
      val bestCombination = allTargets.combinations(maxTargets)
        .map { targets =>
          val combinedBits = targets.map(targetToBits).reduce { (a, b) =>
            val union = a.clone().asInstanceOf[BitSet]
            union.or(b)
            union
          }
          (targets, combinedBits)
        }
        .filter { case (_, bits) => bits.cardinality() > minSupport }
        .toSeq // Convert to Seq first
        .sortBy { case (_, bits) => -bits.cardinality() } // Sort by support descending
        .headOption // Take the best one
      
      bestCombination.toSeq
    }

    // Record initial support only for unbounded mode with drop monitoring
    if (isUnbounded && dropFactor.isDefined && currentLevel.nonEmpty) {
      prevSupports = Some(currentLevel.map(_._2.cardinality().toDouble))
    }

    // Keep all valid candidates across levels so we can pick best later
    var allCandidates = currentLevel.toBuffer
    var k = currentLevel.headOption.map(_._1.length).getOrElse(0) - 1

    // Reduce levels by removing one target at a time (top-down approach)
    while (currentLevel.nonEmpty && k >= 1) {
      val nextBuilder = scala.collection.mutable.ArrayBuffer.empty[(List[String], BitSet)]

      // For each current target set, try removing each target to create smaller sets
      currentLevel.foreach { case (targets, _) =>
        targets.foreach { targetToRemove =>
          val reducedTargets = targets.filter(_ != targetToRemove)
          if (reducedTargets.nonEmpty) {
            // Calculate union of remaining targets
            val combinedBits = reducedTargets.map(targetToBits).reduce { (a, b) =>
              val union = a.clone().asInstanceOf[BitSet]
              union.or(b)
              union
            }
            nextBuilder += ((reducedTargets, combinedBits))
          }
        }
      }

      // Deduplicate candidate target lists, prune by minSupport, and keep only the best one
      val candidates: Seq[(List[String], BitSet)] = nextBuilder
        .groupBy(_._1.sorted) // group by target-list (sorted for consistency)
        .map { case (targetsList, seq) =>
          // Since we're dealing with the same target combination, take the first bitset
          // (they should all be identical for the same target combination)
          (targetsList, seq.head._2)
        }
        .toSeq
        .filter { case (_, bits) => bits.cardinality() > minSupport }
      
      // Keep only the best candidate (highest support, then largest target set, then lexicographic)
      val pruned: Seq[(List[String], BitSet)] = if (candidates.nonEmpty) {
        val best = candidates.maxBy { case (targets, bits) => 
          (bits.cardinality(), targets.length, targets.mkString(","))
        }
        Seq(best)
      } else {
        Seq.empty
      }

      // Add pruned to allCandidates and continue
      allCandidates ++= pruned
      currentLevel = pruned
      
      // For unbounded mode with drop monitoring, track support and check for major drops
      if (isUnbounded && currentLevel.nonEmpty && dropFactor.isDefined && prevSupports.isDefined) {
        val currentSupports = currentLevel.map(_._2.cardinality().toDouble)
        
        // Update drop statistics incrementally and check if we should stop
        val (shouldStop, updatedStats) = shouldStopExtension(
          prevSupports.get, currentSupports, dropStats, dropFactor.get
        )
        dropStats = updatedStats
        prevSupports = Some(currentSupports)
        
        if (shouldStop) {
          currentLevel = Seq.empty  // Stop reduction
        }
      }
      
      k -= 1
    }

    if (allCandidates.isEmpty) {
      None
    } else {
      // Convert all candidates to TargetBranchedPairConstraint objects
      val allConstraints = allCandidates
        .map { case (targetsList, bits) =>
          val (sup, traces) = supportAndTraces(bits)
          TargetBranchedPairConstraint(rule, source, targetsList.toArray, traces)
        }
        
      // Since we're already keeping only the best at each level, we can simply pick the overall best
      // Tie-breaking: support -> largest target set -> lexicographic
      Some(allConstraints.maxBy(bc => (bc.traces.size, bc.targets.length, bc.targets.mkString(","))))
    }
  }

  /**
   * OR mining with support for both bounded and unbounded target set reduction.
   * Uses top-down approach starting from superset of targets and eliminating.
   * 
   * @param constraints Input constraints dataset
   * @param minSupport Minimum support threshold for valid constraints
   * @param maxTargets Maximum number of targets in a constraint. Use Int.MaxValue for unbounded reduction.
   * @param swap Whether to swap source and target for source-branching (default: false)
   * @param dropFactor Optional factor controlling major drop detection in unbounded mode.
   *                   If None, unbounded reduction continues until support threshold or no more candidates.
   *                   If Some(value), uses drop monitoring with threshold = avg_drop + (value * std_dev_of_drops)
   * @return Dataset of mined OR-branched constraints
   * 
   * Usage examples:
   * - Bounded: orMine(constraints, 0.1, maxTargets = 5)
   * - Unbounded (traditional): orMine(constraints, 0.1, Int.MaxValue) 
   * - Unbounded (with drop monitoring): orMine(constraints, 0.1, Int.MaxValue, dropFactor = Some(2.0))
   */
  def orMine(
      constraints: Dataset[PairConstraint],
      minSupport: Double,
      maxTargets: Int,
      swap: Boolean = false,
      dropFactor: Option[Double] = None,
      isUnary: Option[Boolean] = Some(false)
    ): Dataset[PairConstraint] = {
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    if (swap) {
      // Swap source and target in constraints for source-branching
      val swapped = constraints.map(c => PairConstraint(c.rule, c.target, c.source, c.traces))
      return orMine(swapped, minSupport, maxTargets, swap = false, dropFactor).map(c => PairConstraint(c.rule, c.target, c.source, c.traces))
    }

    if (isUnary.getOrElse(false)) {
      // For unary constraints, we treat source as none
      val unaryConstraints = constraints.map(c => PairConstraint(c.rule, "", c.source, c.traces))
      return orMine(unaryConstraints, minSupport, maxTargets, swap = false, dropFactor).map(c => PairConstraint(c.rule, c.target, c.source, c.traces))
    }

    // Collect all distinct trace IDs and assign integer indices once (driver).
    val allTraces: Array[String] = constraints.flatMap(_.traces).distinct.collect()
    val traceToInt: Map[String, Int] = allTraces.zipWithIndex.toMap
    val intToTrace: Array[String] = allTraces

    // Broadcast dictionaries for use in executors
    val bcTraceToInt = spark.sparkContext.broadcast(traceToInt)
    val bcIntToTrace = spark.sparkContext.broadcast(intToTrace)

    // For OR branching, chain rules don't need special handling unlike XOR and AND
    // They can be processed with the same top-down approach
    val results = constraints
      .groupByKey(c => (c.rule, c.source))
      .mapGroups { case ((rule, source), iter) =>
        val singles = iter.toSeq

        // Build target -> java.util.BitSet using the broadcasted dictionary
        val targetToBits: Map[String, BitSet] = singles.map { c =>
          val bits = new BitSet()
          // map string trace ids to ints (skip missing ones just in case)
          val idxs = c.traces.flatMap(t => bcTraceToInt.value.get(t))
          idxs.foreach(bits.set)
          c.target -> bits
        }.toMap

        topDownMining(rule, source, targetToBits, minSupport, maxTargets, bcIntToTrace, dropFactor)
      }
      .filter(_.isDefined)
      .map(_.get)
      .map(bc => PairConstraint(bc.rule, bc.source, bc.targets.mkString(","), bc.traces))

    results
  }
}