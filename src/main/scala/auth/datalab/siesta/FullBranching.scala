package auth.datalab.siesta

import auth.datalab.siesta.Structs.PairConstraint
import org.apache.spark.sql.Dataset

import scala.collection.parallel.ParSeq
import scala.sys.exit

object FullBranching {

  /**
   * Optimized and parallelized algorithmic framework for mining full-branching Declare constraints in Scala.
   * Uses combinations for k-way joins to avoid invalid ordering on Sets.
   */



  case class BranchConstraint(
                               rule: String,
                               sources: Set[String],
                               sourceOp: LogicalOp,
                               targets: Set[String],
                               targetOp: LogicalOp,
                               support: Double,
                               confidence: Double
                             )

  sealed trait LogicalOp
  object LogicalOp {
    case object AND extends LogicalOp
    case object OR  extends LogicalOp
    case object XOR extends LogicalOp
    val values = Seq(AND, OR, XOR)
  }

  // Parameters
  val maxSetSize  = 3
  val sigmaThresh = 0.2
  val kappaThresh = 0.7

  /**
   * Main mining function.
   * Returns ParSeq[BranchConstraint] for full parallelism.
   */
  def mineFullBranch(
                      pcs: Seq[PairConstraint],
                      traceMap: Map[String, Array[String]],
                      freqMap: Map[String, Double]
                    ): ParSeq[BranchConstraint] = {
    // Pre-index traces
    val traceSets = traceMap.mapValues(_.toSet)
    val byRule = pcs.groupBy(_.rule)

    // Parallel: each template mined concurrently
    byRule.par.flatMap { case (rule, pcList) =>
      val relatedIds = pcList.flatMap(_.traces).distinct
      val relatedSets = relatedIds.flatMap(traceSets.get)

      // Generate source candidates in parallel
      val sourceCands: ParSeq[(LogicalOp, Set[String])] =
        LogicalOp.values.par.flatMap { opS =>
          generateCandidateSets(pcList.map(_.eventA).toSet, opS, relatedSets, freqMap)
            .map(s => (opS, s))
        }.toList.par

      // Extend to full branch and filter
      sourceCands.flatMap { case (opS, sources) =>
        LogicalOp.values.par.flatMap { opT =>
          generateCandidateSets(pcList.map(_.eventB).toSet, opT, relatedSets, freqMap).flatMap { targets =>
            val (support, confidence) = computeMetrics(sources, opS, targets, opT, relatedSets)
            if (support >= sigmaThresh && confidence >= kappaThresh)
              Some(BranchConstraint(rule, sources, opS, targets, opT, support, confidence))
            else None
          }
        }.toList.par
      }
    }.toList.par
  }

  /**
   * Apriori candidate generation using combinations for joins.
   */
  def generateCandidateSets(
                             items: Set[String],
                             op: LogicalOp,
                             traceSets: Seq[Set[String]],
                             freqMap: Map[String, Double]
                           ): Seq[Set[String]] = {
    // Singletons
    var prev = items.filter(a => aprioriCheck(Set(a), op, traceSets, freqMap)).map(Set(_)).toSeq
    val results = scala.collection.mutable.ArrayBuffer(prev: _*)

    // k-way joins with combinations
    for (k <- 2 to maxSetSize if prev.nonEmpty) {
      val next = prev.combinations(2).collect {
        case Seq(a, b) if (a union b).size == k => a union b
      }.toSeq.distinct.filter(u => aprioriCheck(u, op, traceSets, freqMap))
      results ++= next
      prev = next
    }

    // Remove trivial XORs
    results.filterNot(set => op == LogicalOp.XOR && set.size < 2)
  }

  /**
   * Apriori and operator-specific pruning.
   */
  private def aprioriCheck(
                    set: Set[String],
                    op: LogicalOp,
                    traceSets: Seq[Set[String]],
                    freqMap: Map[String, Double]
                  ): Boolean = op match {
    case LogicalOp.AND => set.forall(a => freqMap.getOrElse(a, 0.0) >= sigmaThresh)
    case LogicalOp.OR  => set.exists(a => freqMap.getOrElse(a, 0.0) >= sigmaThresh)
    case LogicalOp.XOR => !traceSets.par.exists(ts => set.count(ts.contains) > 1)
  }

  /**
   * Compute support and confidence.
   */
  private def computeMetrics(
                      sources: Set[String],
                      opS: LogicalOp,
                      targets: Set[String],
                      opT: LogicalOp,
                      traceSets: Seq[Set[String]]
                    ): (Double, Double) = {
    val total = traceSets.size
    val (act, ful) = traceSets.par.aggregate((0,0))(
      { case ((aAcc, fAcc), ts) =>
        if (evalOp(opS, sources, ts))
          (aAcc + 1, fAcc + (if (evalOp(opT, targets, ts)) 1 else 0))
        else (aAcc, fAcc)
      }, { case ((a1,f1),(a2,f2)) => (a1+a2, f1+f2) }
    )
    (act.toDouble/total, if (act>0) ful.toDouble/act else 0.0)
  }

  /** Logical evaluation. */
  private def evalOp(
              op: LogicalOp,
              set: Set[String],
              ts: Set[String]
            ): Boolean = op match {
    case LogicalOp.AND => set.forall(ts.contains)
    case LogicalOp.OR  => set.exists(ts.contains)
    case LogicalOp.XOR => set.count(ts.contains) == 1
  }

  def fullmining(pairConstraints: Dataset[PairConstraint]) = {

    val s3Connector = new S3Connector()
    s3Connector.initialize("log_t5e5")
    val traceMap = s3Connector.get_events_sequence_table().rdd.map(x => (x.trace, x.eventType)).groupBy(_._1).mapValues(_.map(_._2).toArray).collect().toMap
    val freqMap = s3Connector.get_index_table().rdd.map(x => (x.eventA, x.trace_id)).groupBy(_._1).mapValues(_.size.toDouble).collect().toMap
    val pcs: Seq[PairConstraint] = pairConstraints.collect().toSeq

    // Invoke mining
    val results = mineFullBranch(pcs, traceMap, freqMap)

    // Determine output path (optional first arg or default)
    val outPath =  "results"
    val writer = new java.io.PrintWriter(new java.io.File(outPath))
    try {
      // Write header including logical operators
      writer.println("rule|sourceOp|sources|targetOp|targets|support|confidence")
      // Write each constraint
      results.foreach { bc =>
        val sourcesCsv = bc.sources.mkString(",")
        val targetsCsv = bc.targets.mkString(",")
        writer.println(
          s"${bc.rule}|${bc.sourceOp}|$sourcesCsv|${bc.targetOp}|$targetsCsv|${bc.support}|${bc.confidence}"
        )
      }
    } finally {
      writer.close()
    }
    println(s"Results written to $outPath")
    exit(0)
  }
}
