package auth.datalab.siesta.mining


import auth.datalab.siesta.model.Structs.{PairConstraint, TargetBranchedPairConstraint, PairConstraintBits}
import org.apache.spark.sql.{Dataset, SparkSession}
import java.util.BitSet
import scala.jdk.CollectionConverters._
import org.apache.log4j.{Level, Logger}

object AndBranchingMiner {

  Logger.getLogger("org").setLevel(Level.INFO)
  private val log: Logger = Logger.getLogger(this.getClass)

  /**
   * Case class for distributed candidate generation with serialized BitSets.
   * BitSets are serialized as byte arrays for Spark Dataset compatibility.
   */
  case class CandidateWithBits(rule: String, source: String, targets: Array[String], bitset: Array[Byte])

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

  private def bottomUpMining(
      rule: String,
      source: String,
      targetToBits: Map[String, BitSet],
      minSupport: Double,
      maxTargets: Int,
      bcIntToTrace: org.apache.spark.broadcast.Broadcast[Array[String]],
      dropFactor: Option[Double]
    ): Option[TargetBranchedPairConstraint] = {

    log.info(s"Starting bottomUpMining for rule '$rule', source '$source' with ${targetToBits.size} initial targets.")

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

    log.info(s"Level 1: Found ${level.size} candidates meeting minSupport.")

    // Record initial support only for unbounded mode with drop monitoring
    if (isUnbounded && dropFactor.isDefined && level.nonEmpty) {
      prevSupports = Some(level.map(_._2.cardinality().toDouble))
    }

    // Track only the best candidate found so far (greedy approach)
    // Tie-breaking: support -> largest target set -> lexicographic
    var bestCandidate: Option[(List[String], BitSet)] = if (level.nonEmpty) {
      Some(level.maxBy { case (targets, bits) => 
        (bits.cardinality(), targets.length, targets.mkString(","))
      })
    } else {
      None
    }
    
    var k = 2

    // Expand levels using prefix grouping (trie-like join)
    while (level.nonEmpty && k <= maxTargets) {
      log.info(s"Starting level $k expansion...")
      // For level 2, we join single items directly
      // For level k>2, we group by prefix of length k-2
      val nextBuilder = scala.collection.mutable.ArrayBuffer.empty[(List[String], BitSet)]

      if (k == 2) {
        // Special case for level 2: combine all pairs of single targets
        // Ensure canonical order: only combine if first < second lexicographically
        val arr = level.toArray.sortBy(_._1.head) // Sort by the single element
        val n = arr.length
        var i = 0
        while (i < n) {
          var j = i + 1
          while (j < n) {
            val (targets1, bits1) = arr(i)
            val (targets2, bits2) = arr(j)
            // targets1 and targets2 are single-element lists
            // Since arr is sorted and j > i, this naturally maintains order
            val newTargets = targets1 ++ targets2 // Already in sorted order

            val inter = bits1.clone().asInstanceOf[BitSet]
            inter.and(bits2)

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
        // CRITICAL: Only join if last element of first < last element of second
        // This ensures each k-itemset is generated exactly once (canonical pairing)
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
                // Since arr is sorted by last element and j > i,
                // we have set1.last < set2.last (canonical order)
                // create new candidate: prefix ++ last elements
                // No need to sort - maintaining prefix order + ordered last elements
                val newTargets = set1 :+ set2.last

                val inter = bits1.clone().asInstanceOf[BitSet]
                inter.and(bits2)
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
      log.info(s"Level $k: Generated ${nextBuilder.size} candidates before pruning.")

      // With canonical pairing, we should have no duplicates, but we still need to
      // verify and handle any edge cases. Most groups will have size 1.
      val pruned: Seq[(List[String], BitSet)] = nextBuilder
        .groupBy(_._1) // group by target-list
        .map { case (targetsList, seq) =>
          val combined = if (seq.size == 1) {
            // No duplicates - common case with canonical pairing
            seq.head._2
          } else {
            // Duplicates found (shouldn't happen with canonical pairing)
            // Take first one since they should all be equal
            log.warn(s"Level $k: Found ${seq.size} duplicates for target list ${targetsList.mkString(",")}")
            seq.head._2
          }
          (targetsList, combined)
        }
        .toSeq
        .filter { case (_, bits) => bits.cardinality() > minSupport }
        .sortBy(_._1.mkString(",")) // deterministic order

      log.info(s"Level $k: ${pruned.size} candidates remaining after pruning.")

      // Update best candidate if this level has a better one
      if (pruned.nonEmpty) {
        val levelBest = pruned.maxBy { case (targets, bits) => 
          (bits.cardinality(), targets.length, targets.mkString(","))
        }
        
        bestCandidate = bestCandidate match {
          case None => Some(levelBest)
          case Some((oldTargets, oldBits)) =>
            val oldScore = (oldBits.cardinality(), oldTargets.length, oldTargets.mkString(","))
            val newScore = (levelBest._2.cardinality(), levelBest._1.length, levelBest._1.mkString(","))
            
            import scala.math.Ordering.Implicits._
            if (newScore > oldScore) Some(levelBest) else Some((oldTargets, oldBits))
        }
      }
      
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
          log.info(s"Stopping unbounded expansion at level $k due to significant support drop.")
          level = Seq.empty // Stop expansion
        }
      }

      k += 1
    }

    bestCandidate match {
      case None =>
        log.info("No valid branched constraints found.")
        None
      case Some((targetsList, bits)) =>
        val (sup, traces) = supportAndTraces(bits)
        val result = TargetBranchedPairConstraint(rule, source, targetsList.toArray, traces)
        log.info(s"Selected best constraint with ${result.targets.length} targets and support ${result.traces.size}.")
        Some(result)
    }
  }

  private def chainMining(
      rule: String,
      source: String,
      targetToBits: Map[String, BitSet],
      minSupport: Double,
      maxTargets: Int,
      bcIntToTrace: org.apache.spark.broadcast.Broadcast[Array[String]],
      dropFactor: Option[Double]
    ): Option[TargetBranchedPairConstraint] = {

    if (targetToBits.isEmpty) {
      return None
    }

    // Find the best chain target based on the largest trace set (bitset cardinality)
    val (bestChainTarget, bestChainBits) = targetToBits.maxBy { case (_, bits) => bits.cardinality() }

    // Filter the other targets: retain only those whose bitsets have a non-empty intersection with the best chain's bitset.
    // The best chain target itself is included to start the bottom-up process.
    val filteredTargetToBits = targetToBits.filter { case (target, bits) =>
      if (target == bestChainTarget) {
        true // Always include the best chain target
      } else {
        val intersection = bestChainBits.clone().asInstanceOf[BitSet]
        intersection.and(bits)
        !intersection.isEmpty
      }
    }

    // The rule for bottom-up mining should be the original "chain-" prefixed rule.
    // The source is the same. The targets are the filtered ones.
    bottomUpMining(rule, source, filteredTargetToBits, minSupport, maxTargets, bcIntToTrace, dropFactor)
  }


  /**
   * AND mining with support for both bounded and unbounded target set extension.
   *
   * @param constraints Input constraints dataset
   * @param minSupport Minimum support threshold for valid constraints
   * @param maxTargets Maximum number of targets in a constraint. Use Int.MaxValue for unbounded extension.
   * @param swap Whether to swap source and target for source-branching (default: false)
   * @param dropFactor Optional factor controlling major drop detection in unbounded mode.
   *                   If None, unbounded extension continues until support threshold or no more candidates.
   *                   If Some(value), uses drop monitoring with threshold = avg_drop + (value * std_dev_of_drops)
   * @param allConstraints Optional dataset of ALL constraints (needed for chain rule mixing). 
   *                       When mining chain-X rules, this should contain both chain-X and X constraints.
   * @return Dataset of mined AND-branched constraints
   *
   * Usage examples:
   * - Bounded: andMine(constraints, 0.1, maxTargets = 5)
   * - Unbounded (traditional): andMine(constraints, 0.1, Int.MaxValue)
   * - Unbounded (with drop monitoring): andMine(constraints, 0.1, Int.MaxValue, dropFactor = Some(2.0))
   * - Chain rule mixing: andMine(chainConstraints, 0.1, 3, allConstraints = Some(allConstraintsDataset))
   */
  def andMine(
      constraints: Dataset[PairConstraint],
      minSupport: Double,
      maxTargets: Int,
      swap: Boolean = false,
      dropFactor: Option[Double] = None,
      isUnary: Option[Boolean] = Some(false)
    ): Dataset[PairConstraint] = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Cache the original dataset before any transformations for chain rule mixing
    val allConstraints = constraints.cache()

    // Handle source-branching by swapping source and target, then recursively calling.
    // The final result is swapped back.
    if (swap) {
      val swapped = constraints.map(c => PairConstraint(c.rule, c.target, c.source, c.traces))
      return andMine(swapped, minSupport, maxTargets, swap = false, dropFactor, isUnary)
        .map(c => PairConstraint(c.rule, c.target, c.source, c.traces))
    }

    // Handle unary constraints by treating the source as an empty string.
    // The final result has the target moved back to the source field.
    if (isUnary.getOrElse(false)) {
      val unaryConstraints = constraints.map(c => PairConstraint(c.rule, "", c.source, c.traces))
      return andMine(unaryConstraints, minSupport, maxTargets, swap = false, dropFactor, isUnary = Some(false))
        .map(c => PairConstraint(c.rule, c.target, "", c.traces)) // source is empty for unary
    }

    // Create a distributed map from trace IDs to integers to avoid collecting all traces on the driver.
    log.info("Creating distributed map from trace IDs to integers")
    val distinctTraces = constraints.flatMap(_.traces).distinct().cache()
    val traceToInt: Map[String, Int] = distinctTraces
      .rdd
      .zipWithIndex()
      .map { case (trace, index) => (trace, index.toInt) }
      .collectAsMap()
      .toMap
    log.info(s"Collected ${traceToInt.size} distinct traces")

    // Create the reverse mapping from integers to trace IDs.
    val intToTrace: Array[String] = new Array[String](traceToInt.size)
    traceToInt.foreach { case (trace, index) => intToTrace(index) = trace }

    // Broadcast the mappings to all executors.
    log.info("Broadcasting trace mappings to executors")
    val bcTraceToInt = spark.sparkContext.broadcast(traceToInt)
    val bcIntToTrace = spark.sparkContext.broadcast(intToTrace)

    // Group constraints by (rule, source) and process each group.
    log.info("Grouping constraints by (rule, source) and processing groups")
    
    // Pre-process ALL constraints grouped by (rule, source) for chain mixing
    // Use RDD to avoid Spark Dataset serialization issues with BitSet
    val allConstraintsByRuleSource: Map[(String, String), Map[String, BitSet]] = allConstraints
      .rdd
      .map { c =>
        val bits = new BitSet()
        val idxs = c.traces.flatMap(t => bcTraceToInt.value.get(t))
        idxs.foreach(bits.set)
        ((c.rule, c.source), (c.target, bits))
      }
      .groupByKey()
      .mapValues(_.toMap)
      .collectAsMap()
      .toMap
    
    val bcAllConstraints = spark.sparkContext.broadcast(allConstraintsByRuleSource)
    
    // Convert to a Dataset with BitSets for distributed processing
    val candidatesWithBits = constraints
      .groupByKey(c => (c.rule, c.source))
      .flatMapGroups { case ((rule, source), iter) =>
        // Convert trace sets to BitSets for this group (these are the constraints for this specific rule)
        val chainTargetToBits: Map[String, BitSet] = iter.map { c =>
          val bits = new BitSet()
          val idxs = c.traces.flatMap(t => bcTraceToInt.value.get(t))
          idxs.foreach(bits.set)
          c.target -> bits
        }.toMap
        
        // For chain rules: mix best chain target with non-chain targets
        val filteredTargetToBits = if (rule.startsWith("chain-")) {
          if (chainTargetToBits.isEmpty) {
            Map.empty[String, BitSet]
          } else {
            // Find the best chain target (highest support)
            val (bestChainTarget, bestChainBits) = chainTargetToBits.maxBy { case (_, bits) => bits.cardinality() }
            
            // Get non-chain targets for this source from the broadcast variable
            val baseRule = rule.stripPrefix("chain-")
            val nonChainTargets = bcAllConstraints.value.getOrElse((baseRule, source), Map.empty[String, BitSet])
            
            // Filter non-chain targets: keep only those with non-empty intersection with best chain target
            val compatibleNonChainTargets = nonChainTargets.filter { case (target, bits) =>
              val intersection = bestChainBits.clone().asInstanceOf[BitSet]
              intersection.and(bits)
              !intersection.isEmpty
            }
            
            log.info(s"Chain rule '$rule', source '$source': best chain target '$bestChainTarget', " +
                    s"${compatibleNonChainTargets.size} compatible non-chain targets")
            
            // Combine: best chain target + compatible non-chain targets
            Map(bestChainTarget -> bestChainBits) ++ compatibleNonChainTargets
          }
        } else {
          chainTargetToBits
        }
        
        // Return level-1 candidates (individual targets) that meet minSupport
        filteredTargetToBits.filter { case (_, bits) => bits.cardinality() > minSupport }
          .map { case (target, bits) =>
            CandidateWithBits(rule, source, Array(target), bits.toByteArray)
          }
      }
    
    val initialCandidateCount = candidatesWithBits.count()
    log.info(s"Initial candidates generated for all (rule, source) pairs: $initialCandidateCount total")
    
    // Now perform level-by-level expansion in a distributed manner
    var currentLevel = candidatesWithBits.cache()
    var bestCandidates: Dataset[CandidateWithBits] = null  // Accumulates best per (rule, source) across all levels
    var k = 1
    
    // Track incremental drop statistics for unbounded mode with drop monitoring
    val isUnbounded = maxTargets == Int.MaxValue
    var dropStats = DropStats(0, 0.0, 0.0)
    var prevSupports: Option[Seq[Double]] = None
    
    // Record initial support values for drop monitoring
    if (isUnbounded && dropFactor.isDefined) {
      val initialSupports = currentLevel.map(c => BitSet.valueOf(c.bitset).cardinality().toDouble).collect()
      if (initialSupports.nonEmpty) {
        prevSupports = Some(initialSupports)
        log.info(s"Level 1: Initialized drop monitoring with ${initialSupports.length} support values")
      }
    }
    
    while (k < maxTargets) {
      val candidateCount = currentLevel.count()
      log.info(s"Level $k: $candidateCount candidates")
      
      if (candidateCount == 0) {
        log.info(s"No candidates at level $k, stopping expansion")
        k = maxTargets // Break the loop
      } else {
        // Select best from current level per (rule, source)
        val currentBest = currentLevel
          .groupByKey(c => (c.rule, c.source))
          .mapGroups { case ((rule, source), iter) =>
            val candidates = iter.toArray
            // Select best: highest support, then most targets, then lexicographic
            candidates.maxBy { c =>
              val bits = BitSet.valueOf(c.bitset)
              (bits.cardinality(), c.targets.length, c.targets.mkString(","))
            }
          }
        
        // Merge with accumulated best: for each (rule, source), keep the better one
        if (bestCandidates == null) {
          bestCandidates = currentBest.cache()
        } else {
          val merged = bestCandidates.union(currentBest)
            .groupByKey(c => (c.rule, c.source))
            .mapGroups { case ((rule, source), iter) =>
              val candidates = iter.toArray
              // Keep the best across old and new
              candidates.maxBy { c =>
                val bits = BitSet.valueOf(c.bitset)
                (bits.cardinality(), c.targets.length, c.targets.mkString(","))
              }
            }
            .cache()
          
          bestCandidates.unpersist()
          bestCandidates = merged
        }
        
        val bestCount = bestCandidates.count()
        log.info(s"Level $k: Accumulated $bestCount best candidates across all levels so far")
        
        // Check for support drop before expanding to next level (unbounded mode only)
        if (isUnbounded && dropFactor.isDefined && prevSupports.isDefined && k > 1) {
          val currentSupports = currentLevel.map(c => BitSet.valueOf(c.bitset).cardinality().toDouble).collect()
          
          if (currentSupports.nonEmpty) {
            val (shouldStop, updatedStats) = shouldStopExtension(
              prevSupports.get, currentSupports, dropStats, dropFactor.get
            )
            dropStats = updatedStats
            prevSupports = Some(currentSupports)
            
            if (shouldStop) {
              log.info(s"Stopping unbounded expansion at level $k due to significant support drop.")
              k = maxTargets // Break the loop
            }
          }
        }
        
        if (k < maxTargets) {
          k += 1
          
          // Generate next level candidates
          val nextLevel = if (k == 2) {
          // Special case: join all pairs
          currentLevel.as("a")
            .joinWith(currentLevel.as("b"), 
              $"a.rule" === $"b.rule" && $"a.source" === $"b.source" && $"a.targets"(0) < $"b.targets"(0))
            .map { case (c1, c2) =>
              val bits1 = BitSet.valueOf(c1.bitset)
              val bits2 = BitSet.valueOf(c2.bitset)
              val inter = bits1.clone().asInstanceOf[BitSet]
              inter.and(bits2)
              
              val newTargets = (c1.targets ++ c2.targets).sorted
              CandidateWithBits(c1.rule, c1.source, newTargets, inter.toByteArray)
            }
            .filter(c => BitSet.valueOf(c.bitset).cardinality() > minSupport)
        } else {
          // For k>2: join candidates that share k-2 prefix
          currentLevel
            .groupByKey(c => (c.rule, c.source, c.targets.take(k - 2).mkString(",")))
            .flatMapGroups { case (_, iter) =>
              val candidates = iter.toArray.sortBy(_.targets.last)
              val results = scala.collection.mutable.ArrayBuffer.empty[CandidateWithBits]
              
              var i = 0
              while (i < candidates.length) {
                var j = i + 1
                while (j < candidates.length) {
                  val c1 = candidates(i)
                  val c2 = candidates(j)
                  
                  val bits1 = BitSet.valueOf(c1.bitset)
                  val bits2 = BitSet.valueOf(c2.bitset)
                  val inter = bits1.clone().asInstanceOf[BitSet]
                  inter.and(bits2)
                  
                  if (inter.cardinality() > minSupport) {
                    val newTargets = c1.targets :+ c2.targets.last
                    results += CandidateWithBits(c1.rule, c1.source, newTargets, inter.toByteArray)
                  }
                  j += 1
                }
                i += 1
              }
              results
            }
        }
        
        currentLevel.unpersist()
        currentLevel = nextLevel.cache()
        
        // Update support tracking for next iteration (unbounded mode with drop monitoring)
        if (isUnbounded && dropFactor.isDefined) {
          val newSupports = currentLevel.map(c => BitSet.valueOf(c.bitset).cardinality().toDouble).collect()
          if (newSupports.nonEmpty) {
            prevSupports = Some(newSupports)
          }
        }
      }
    }
    }
    // Convert best candidates to PairConstraints
    log.info(s"Converting ${if (bestCandidates != null) "best candidates" else "no candidates"} to PairConstraints")
    
    val results = if (bestCandidates != null) {
      bestCandidates.map { c =>
        val bits = BitSet.valueOf(c.bitset)
        val idxs = bits.stream().toArray
        val traces = idxs.map(i => bcIntToTrace.value(i)).toSet
        PairConstraint(c.rule, c.source, c.targets.mkString(","), traces)
      }.cache() // Cache results before unpersisting source data
    } else {
      log.warn("No best candidates found - returning empty dataset")
      spark.emptyDataset[PairConstraint]
    }

    val resultCount = results.count()
    log.info(s"Returning $resultCount best candidates from highest level")
    
    // Clean up after materializing results
    if (currentLevel != null) currentLevel.unpersist()
    if (bestCandidates != null) bestCandidates.unpersist()
    
    results
  }
}
