package auth.datalab.siesta.mining


import auth.datalab.siesta.model.Structs.{PairConstraint, TargetBranchedPairConstraint, PairConstraintBits}
import org.apache.spark.sql.{Dataset, SparkSession}
import java.util.BitSet
import scala.jdk.CollectionConverters._


object XORBranchingMiner {

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
   * Determines if target set extension should stop due to a major support drop.
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

  private def regularMining(
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
    
    // Level-1 candidates: single-target lists 
    var level: Seq[(List[String], BitSet)] =
      targetToBits.toSeq
        .map { case (t, bitset) => (List(t), bitset) }
        .filter { case (_, bits) => bits.cardinality() > minSupport }
        .sortBy(_._1.mkString(","))

    // Record initial support only for unbounded mode with drop monitoring
    if (isUnbounded && dropFactor.isDefined && level.nonEmpty) {
      prevSupports = Some(level.map(_._2.cardinality().toDouble))
    }

    // keep all valid candidates across levels so we can pick best later
    var allCandidates = level.toBuffer
    var k = 2

    // Expand levels using prefix grouping (trie-like join)
    while (level.nonEmpty && k <= maxTargets) {

        // For level 2, we join single items directly
        // For level k>2, we group by prefix of length k-2 
        val nextBuilder = scala.collection.mutable.ArrayBuffer.empty[(List[String], BitSet)]

        if (k == 2) {
            // Special case for level 2: combine all pairs of single targets
            val arr = level.toArray
            val n = arr.length
            var i = 0
            while (i < n) {
                var j = i + 1
                while (j < n) {
                    val (targets1, bits1) = arr(i)
                    val (targets2, bits2) = arr(j)
                    val newTargets = (targets1 ++ targets2).sorted // ensure canonical order

                    val inter = bits1.clone().asInstanceOf[BitSet]
                    inter.xor(bits2)
                    
                    if (inter.cardinality() > minSupport) {
                        nextBuilder += ((newTargets, inter))
                    }
                    j += 1
                }
                i += 1
            }
        } else {
            // For level k>2: Group by prefix of length k-2
            val grouped: Map[List[String], Seq[(List[String], BitSet)]] =
                level.groupBy { case (targets, _) => targets.take(k - 2) }

            // For each group, consider pairs that share the same prefix
            grouped.values.foreach { groupSeq =>
                if (groupSeq.size >= 2) {
                    val arr = groupSeq.sortBy(_._1.last)
                    val n = arr.length
                    var i = 0
                    while (i < n) {
                        var j = i + 1
                        while (j < n) {
                            val (set1, bits1) = arr(i)
                            val (set2, bits2) = arr(j)
                            // create new candidate: prefix ++ last elements
                            val newTargets = (set1 ++ List(set2.last)).distinct.sorted

                            val inter = bits1.clone().asInstanceOf[BitSet]
                            inter.xor(bits2)
                            
                            if (inter.cardinality() > minSupport) {
                                nextBuilder += ((newTargets, inter))
                            }
                            j += 1
                        }
                        i += 1
                    }
                }
            }
        }

        // Deduplicate candidate target lists and prune by minSupport
        val pruned: Seq[(List[String], BitSet)] = nextBuilder
            .groupBy(_._1) // group by target-list
            .map { case (targetsList, seq) =>
                // combine bitsets if multiple bitsets produced for same union:
                // take intersection across all (should be equal in Apriori-style join,
                // but we intersect to be safe)
                val combined = seq.map(_._2).reduce { (a, b) =>
                    val c = a.clone().asInstanceOf[BitSet]
                    c.xor(b)
                    c
                }
                (targetsList, combined)
            }
            .toSeq
            .filter { case (_, bits) => bits.cardinality() > minSupport }
            .sortBy(_._1.mkString(",")) // deterministic order

        // add pruned to allCandidates and continue
        allCandidates ++= pruned
        level = pruned
        
        // For unbounded mode with drop monitoring, track support and check for major drops
        if (isUnbounded && level.nonEmpty && dropFactor.isDefined && prevSupports.isDefined) {
          val currentSupports = level.map(_._2.cardinality().toDouble)
          
          // Update drop statistics incrementally and check if we should stop
          val (shouldStop, updatedStats) = shouldStopExtension(
            prevSupports.get, currentSupports, dropStats, dropFactor.get
          )
          dropStats = updatedStats
          prevSupports = Some(currentSupports)
          
          if (shouldStop) {
            level = Seq.empty  // Stop expansion
          }
        }
        
        k += 1
    }

    if (allCandidates.isEmpty) {
      None
    } else {
      // First, convert all candidates to TargetBranchedPairConstraint objects
      val allConstraints = allCandidates
        .map { case (targetsList, bits) =>
            val (sup, traces) = supportAndTraces(bits)
            TargetBranchedPairConstraint(rule, source, targetsList.toArray, traces)
        }
        
      // Filter out any constraints with target sets that are induced by others
      val filteredConstraints = allConstraints.filter { current =>
        val currentTargets = current.targets.toSet
        
        !allConstraints.exists { other =>
          if (current eq other) false  // Skip self-comparison
          else {
            val otherTargets = other.targets.toSet
            
            // Current is induced by other if:
            // 1. There's overlap between target sets
            // 2. Current's target set is a proper subset of other's
            val hasOverlap = currentTargets.exists(otherTargets.contains)
            val isProperSubset = currentTargets.size < otherTargets.size && currentTargets.subsetOf(otherTargets)
            
            hasOverlap && isProperSubset
          }
        }
      }
      
      // From the remaining non-induced constraints, select the best one
      // Tie-breaking: support -> largest target set -> lexicographic
      Some(filteredConstraints.maxBy(bc => (bc.traces.size, bc.targets.length, bc.targets.mkString(","))))
    }
  }




  /**
   * XOR mining with support for both bounded and unbounded target set extension.
   * Unlike AND mining, XOR treats all constraints (including chains) the same way
   * since XOR operates on individual constraint satisfaction.
   * 
   * @param constraints Input constraints dataset
   * @param minSupport Minimum support threshold for valid constraints
   * @param maxTargets Maximum number of targets in a constraint. Use Int.MaxValue for unbounded extension.
   * @param swap Whether to swap source and target for source-branching (default: false)
   * @param dropFactor Optional factor controlling major drop detection in unbounded mode.
   *                   If None, unbounded extension continues until support threshold or no more candidates.
   *                   If Some(value), uses drop monitoring with threshold = avg_drop + (value * std_dev_of_drops)
   * @return Dataset of mined XOR-branched constraints
   * 
   * Usage examples:
   * - Bounded: xorMine(constraints, 0.1, maxTargets = 5)
   * - Unbounded (traditional): xorMine(constraints, 0.1, Int.MaxValue) 
   * - Unbounded (with drop monitoring): xorMine(constraints, 0.1, Int.MaxValue, dropFactor = Some(2.0))
   */
  def xorMine(
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
      return xorMine(swapped, minSupport, maxTargets, swap = false, dropFactor).map(c => PairConstraint(c.rule, c.target, c.source, c.traces))
    }

    if (isUnary.getOrElse(false)) {
      // For unary constraints, we treat source as none
      val unaryConstraints = constraints.map(c => PairConstraint(c.rule, "", c.source, c.traces))
      return xorMine(unaryConstraints, minSupport, maxTargets, swap = false, dropFactor).map(c => PairConstraint(c.rule, c.source, c.target, c.traces))
    }

    // Collect all distinct trace IDs and assign integer indices once (driver).
    val allTraces: Array[String] = constraints.flatMap(_.traces).distinct.collect()
    val traceToInt: Map[String, Int] = allTraces.zipWithIndex.toMap
    val intToTrace: Array[String] = allTraces

    // Broadcast dictionaries for use in executors
    val bcTraceToInt = spark.sparkContext.broadcast(traceToInt)
    val bcIntToTrace = spark.sparkContext.broadcast(intToTrace)

    // For XOR, all constraints (including chains) are processed the same way
    // since XOR operates on individual constraint satisfaction
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

        regularMining(rule, source, targetToBits, minSupport, maxTargets, bcIntToTrace, dropFactor)
      }
      .filter(_.isDefined)
      .map(_.get)
      .map(bc => PairConstraint(bc.rule, bc.source, bc.targets.mkString(","), bc.traces))

    results
  }
}
