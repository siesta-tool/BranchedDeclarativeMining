package auth.datalab.siesta


import auth.datalab.siesta.Structs.{PairConstraint, TargetBranchedPairConstraint, PairConstraintBits}
import org.apache.spark.sql.{Dataset, SparkSession}
import java.util.BitSet
import scala.jdk.CollectionConverters._
import auth.datalab.siesta.Structs.{PairConstraint, TargetBranchedPairConstraint}


object AndBranchingMiner {

  private def regularMining(
      rule: String, 
      source: String, 
      targetToBits: Map[String, BitSet], 
      minSupport: Double, 
      maxTargets: Int, 
      bcIntToTrace: org.apache.spark.broadcast.Broadcast[Array[String]]
    ): Option[TargetBranchedPairConstraint] = {
    
    // Helper: from BitSet -> (support, Set[String])
    def supportAndTraces(bits: BitSet): (Int, Set[String]) = {
      val idxs: Array[Int] = bits.stream().toArray 
      val traces: Set[String] = idxs.map(i => bcIntToTrace.value(i)).toSet
      (idxs.length, traces)
    }

    // Level-1 candidates: single-target lists 
    var level: Seq[(List[String], BitSet)] =
      targetToBits.toSeq
        .map { case (t, bitset) => (List(t), bitset) }
        .filter { case (_, bits) => bits.cardinality() > minSupport }
        .sortBy(_._1.mkString(","))

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

        // Deduplicate candidate target lists and prune by minSupport
        val pruned: Seq[(List[String], BitSet)] = nextBuilder
            .groupBy(_._1) // group by target-list
            .map { case (targetsList, seq) =>
                // combine bitsets if multiple bitsets produced for same union:
                // take intersection across all (should be equal in Apriori-style join,
                // but we intersect to be safe)
                val combined = seq.map(_._2).reduce { (a, b) =>
                    val c = a.clone().asInstanceOf[BitSet]
                    c.and(b)
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

  private def chainMining(
      rule: String,
      allConstraints: Array[PairConstraint], 
      minSupport: Double, 
      maxTargets: Int,
      traceToInt: Map[String, Int],
      intToTrace: Array[String]
    ): Seq[PairConstraint] = {
    
    // Helper: from BitSet -> (support, Set[String])
    def supportAndTraces(bits: BitSet): (Int, Set[String]) = {
      val idxs: Array[Int] = bits.stream().toArray 
      val traces: Set[String] = idxs.map(i => intToTrace(i)).toSet
      (idxs.length, traces)
    }

    // Convert constraints to BitSet representation
    def constraintToBits(constraint: PairConstraint): BitSet = {
      val bits = new BitSet()
      val idxs = constraint.traces.flatMap(t => traceToInt.get(t))
      idxs.foreach(bits.set)
      bits
    }

    // Create a lookup: (source, target) -> BitSet for efficient chain building
    val chainLookup: Map[(String, String), BitSet] = 
      allConstraints.map(c => (c.source, c.target) -> constraintToBits(c)).toMap

    // Group by source to get starting points, but use level-wise approach like original
    val startingConstraints = allConstraints.groupBy(_.source)
    
    val allValidChains = scala.collection.mutable.ArrayBuffer[PairConstraint]()

    startingConstraints.foreach { case (startSource, constraints) =>
      // Level 1: Single constraints (A -> B)
      var currentLevel: Seq[(List[String], BitSet)] = constraints
        .map { c => 
          val bits = constraintToBits(c)
          (List(c.source, c.target), bits)
        }
        .filter { case (_, bits) => bits.cardinality() > minSupport }
        .toSeq

      var allCandidates = currentLevel.toBuffer
      var k = 2

      // Level-wise expansion
      while (currentLevel.nonEmpty && k <= maxTargets) {
        val nextLevelBuilder = scala.collection.mutable.ArrayBuffer[(List[String], BitSet)]()
        
        // For each current chain, try to extend it by one step
        currentLevel.foreach { case (currentChain, currentBits) =>
          val lastTarget = currentChain.last
          
          // Look for constraints where lastTarget -> nextTarget exists
          chainLookup.foreach { case ((source, target), nextBits) =>
            if (source == lastTarget && !currentChain.contains(target)) {
              // Found a valid extension: lastTarget -> target
              val extendedChain = currentChain :+ target
              val chainBits = currentBits.clone().asInstanceOf[BitSet]
              chainBits.and(nextBits)
              
              if (chainBits.cardinality() > minSupport) {
                nextLevelBuilder += ((extendedChain, chainBits))
              }
            }
          }
        }
        
        // Deduplicate and prune (similar to your original approach)
        val prunedLevel = nextLevelBuilder
          .groupBy(_._1) // group by chain
          .map { case (chain, candidates) =>
            // If multiple candidates for same chain, take intersection
            val combinedBits = candidates.map(_._2).reduce { (a, b) =>
              val c = a.clone().asInstanceOf[BitSet]
              c.and(b)
              c
            }
            (chain, combinedBits)
          }
          .toSeq
          .filter { case (_, bits) => bits.cardinality() > minSupport }
          .sortBy(_._1.mkString(",")) // deterministic order
        
        allCandidates ++= prunedLevel
        currentLevel = prunedLevel
        k += 1
      }

      // Convert valid chains to PairConstraints
      allCandidates.foreach { case (chain, bits) =>
        if (chain.length >= 2) { // Only chains with at least 2 elements (source and 1st target)
          val (_, traces) = supportAndTraces(bits)
          val chainTargets = chain.tail // Remove source, keep only targets in chain
          allValidChains += PairConstraint(rule, startSource, chainTargets.mkString(","), traces)
        }
      }
    }

    // Filter chains to remove induced target sets
    val filteredChains = allValidChains.filter { current =>
      val currentTargets = current.target.split(",").toSet
      
      !allValidChains.exists { other =>
        if (current eq other) false  // Skip self-comparison
        else if (current.source != other.source) false // Only compare same source
        else {
          val otherTargets = other.target.split(",").toSet
          
          // Check if current is induced by other:
          // 1. There's overlap in targets
          // 2. Current's targets are a proper subset of other's
          val hasOverlap = currentTargets.exists(otherTargets.contains)
          val isProperSubset = currentTargets.size < otherTargets.size && currentTargets.subsetOf(otherTargets)
          
          hasOverlap && isProperSubset
        }
      }
    }
    
    // For each source, target pattern, choose the best chain
    filteredChains
      .groupBy(c => (c.source, c.target))
      .map { case (_, chains) =>
        // Pick the chain with highest support, then longest, then lexicographic
        chains.maxBy(c => (c.traces.size, c.target.split(",").length, c.target))
      }
      .toSeq
  }


  def andMine(
      constraints: Dataset[PairConstraint],
      minSupport: Double,
      maxTargets: Int,
      swap: Boolean = false
    ): Dataset[PairConstraint] = {
    
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._
    
    if (swap) {
      // Swap source and target in constraints for source-branching
      val swapped = constraints.map(c => PairConstraint(c.rule, c.target, c.source, c.traces))
      return andMine(swapped, minSupport, maxTargets)
    }


    // Collect all distinct trace IDs and assign integer indices once (driver).
    val allTraces: Array[String] = constraints.flatMap(_.traces).distinct.collect()
    val traceToInt: Map[String, Int] = allTraces.zipWithIndex.toMap
    val intToTrace: Array[String] = allTraces

    // Broadcast dictionaries for use in executors
    val bcTraceToInt = spark.sparkContext.broadcast(traceToInt)
    val bcIntToTrace = spark.sparkContext.broadcast(intToTrace)

    // For chain rules, we need a different approach that considers all constraints
    // First, separate chain rules from regular rules
    val chainConstraints = constraints.filter(_.rule.startsWith("chain"))
    val regularConstraints = constraints.filter(!_.rule.startsWith("chain"))

    // Process regular constraints with the existing algorithm
    val regularResults = regularConstraints
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

        regularMining(rule, source, targetToBits, minSupport, maxTargets, bcIntToTrace)
      }
      .filter(_.isDefined)
      .map(_.get)
      .map(bc => PairConstraint(bc.rule, bc.source, bc.targets.mkString(","), bc.traces))

    // Process chain constraints with a more distributed approach
    val chainResults = if (chainConstraints.count() > 0) {
      // Broadcast trace mappings for use in executors
      val bcTraceToIntForChain = spark.sparkContext.broadcast(traceToInt)
      val bcIntToTraceForChain = spark.sparkContext.broadcast(intToTrace)
      
      // Group by rule and process each rule's constraints in parallel
      chainConstraints
        .groupByKey(_.rule)
        .mapGroups { (rule, constraintsIter) =>
          // Collect constraints for this specific rule only
          val ruleConstraints = constraintsIter.toArray
          // Run chain mining for this rule's constraints
          chainMining(rule, ruleConstraints, minSupport, maxTargets, 
                      bcTraceToIntForChain.value, bcIntToTraceForChain.value)
        }
        .flatMap(identity) // Flatten the results
    } else {
      spark.emptyDataset[PairConstraint]
    }

    // Union the results - filtering is now done in the mining methods
    regularResults.union(chainResults)
  }
}
