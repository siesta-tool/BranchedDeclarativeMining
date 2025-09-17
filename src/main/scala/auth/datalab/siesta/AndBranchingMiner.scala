package auth.datalab.siesta


import auth.datalab.siesta.Structs.{PairConstraint, TargetBranchedPairConstraint, PairConstraintBits}
import org.apache.spark.sql.{Dataset, SparkSession}
import java.util.BitSet
import scala.jdk.CollectionConverters._
import auth.datalab.siesta.Structs.{PairConstraint, TargetBranchedPairConstraint}


/**
 * AND branching miner (Spark-friendly). 
 * - Uses a global trace->int dictionary (broadcast).
 * - Stores trace indices as arrays in Spark-readable fields (we keep PairConstraint unchanged).
 * - Rebuilds java.util.BitSet inside each group for fast bitwise ops.
 * - Uses prefix-group join to generate next-level candidates (avoids full pairwise O(n^2) joins).
 */

object AndBranchingMiner {

  def mineBest(
      constraints: Dataset[PairConstraint],
      minSupport: Double,
      maxTargets: Int
    ): Array[(String, String, Set[String])] = {
    
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    // -----------------------------
    // 1) Global trace dictionary
    // -----------------------------
    // Collect all distinct trace IDs and assign integer indices once (driver).
    val allTraces: Array[String] = constraints.flatMap(_.traces).distinct.collect()
    val traceToInt: Map[String, Int] = allTraces.zipWithIndex.toMap
    val intToTrace: Array[String] = allTraces

    // Broadcast dictionaries for use in executors
    val bcTraceToInt = spark.sparkContext.broadcast(traceToInt)
    val bcIntToTrace = spark.sparkContext.broadcast(intToTrace)

    // -----------------------------
    // 2) Group and mine per (rule, source)
    // -----------------------------
    constraints
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

        // Helper: from BitSet -> (support, Set[String])
        def supportAndTraces(bits: BitSet): (Int, Set[String]) = {
          val idxs: Array[Int] = bits.stream().toArray // primitive int[]
          val traces: Set[String] = idxs.map(i => bcIntToTrace.value(i)).toSet
          (idxs.length, traces)
        }

        // Level-1 candidates: single-target lists (use List for efficient concat)
        var level: Seq[(List[String], BitSet)] =
          targetToBits.toSeq
            .map { case (t, bitset) => (List(t), bitset) }
            .filter { case (_, bits) => bits.cardinality() >= minSupport }
            .sortBy(_._1.mkString(",")) // stable ordering by single target (not required, but deterministic)

        // println(s"Mining ($rule, $source), ${level.size} initial targets")
        // println(s"Initial candidates: ${level.map(c => s"${c._1.mkString(",")}(${c._2.cardinality()})").mkString(", ")}")

        // keep all valid candidates across levels so we can pick best later
        var allCandidates = level.toBuffer
        var k = 2

        // Expand levels using prefix grouping (trie-like join)
        while (level.nonEmpty && k <= maxTargets) {
            // println(s"Level $k: Starting with ${level.size} candidates")

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
                        // println(s"  Level 2 candidate: ${newTargets.mkString(",")}")

                        // intersection
                        val inter = bits1.clone().asInstanceOf[BitSet]
                        inter.and(bits2)
                        // println(s"    Support: ${inter.cardinality()} (bits1: ${bits1.cardinality()}, bits2: ${bits2.cardinality()})")
                        
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

                // grouped.foreach { case (prefix, groupSeq) =>
                //     println(s"Level $k - Prefix: ${prefix.mkString(",")}, Group size: ${groupSeq.size}")
                // }

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
                                // println(s"  Level $k candidate: ${newTargets.mkString(",")}")

                                // intersection
                                val inter = bits1.clone().asInstanceOf[BitSet]
                                inter.and(bits2)
                                // println(s"    Support: ${inter.cardinality()} (bits1: ${bits1.cardinality()}, bits2: ${bits2.cardinality()})")
                                
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

            // println(s"Level $k: Generated ${nextBuilder.size} candidates, ${pruned.size} after pruning")
            // if (pruned.nonEmpty) {
            //     println(s"Level $k results: ${pruned.map(c => s"${c._1.mkString(",")}(${c._2.cardinality()})").mkString(", ")}")
            // }
      
            // add pruned to allCandidates and continue
            allCandidates ++= pruned
            level = pruned
            k += 1
        }

        // allCandidates.foreach { case (targets, bits) =>
        //     println(s"  Candidate: ${targets.mkString(",")} with support ${bits.cardinality()}")
        // }

        val bestConstraint = allCandidates
            .map { case (targetsList, bits) =>
                val (sup, traces) = supportAndTraces(bits)
                TargetBranchedPairConstraint(rule, source, targetsList.toArray, traces)
            }
            // Tie-breaking: support -> largest target set -> lexicographic
            .maxBy(bc => (bc.traces.size, bc.targets.length, bc.targets.mkString(",")))

        // println(s"Best constraint: ${bestConstraint.rule}, ${bestConstraint.source}, ${bestConstraint.targets.mkString(",")}, support: ${bestConstraint.traces.size}")
        bestConstraint
    }
    .map(bc => (bc.rule, bc.source + "|" + bc.targets.mkString(","), bc.traces))
    .collect()
  }
}
