package auth.datalab.siesta

import auth.datalab.siesta.Structs.{Constraint, PairConstraint, PositionConstraint, TargetBranchedConstraint}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{collect_set, countDistinct, explode}
import org.apache.spark.sql.{Dataset, SparkSession, functions}

import scala.collection.mutable.ListBuffer


object TBDeclare {

//  def combine_constraints(logname: String, support: Double, total_traces: Long): Dataset[TargetBranchedConstraint]  = {
//    val spark = SparkSession.builder().getOrCreate()
//    val rulesToIgnore = Set("last", "first", "existence", "absence", "exactly")
//
//    import spark.implicits._
//    val order_path = s"""s3a://siesta/$logname/declare/order.parquet/"""
//
//    try {
//      spark.read.parquet(order_path)
//        .map(x => TargetBranchedConstraint(
//          x.getAs[String]("rule"),
//          x.getAs[String]("eventA"),
//          Array((x.getAs[String]("eventB"), x.getAs[Double]("occurrences")))
//        ))
//        .rdd
//        .keyBy(x => (x.rule, x.activation))
//        // Discard meaningless-for-branching rules
//        .filter(x => !rulesToIgnore.contains(x._1._1))
//        // Merge the targets arrays
//        .reduceByKey((a, b) => TargetBranchedConstraint(
//          a.rule,
//          a.activation,
//          a.targets ++ b.targets
//        ))
//        // Keep branches that hold over the support threshold
//        .map { case ((rule, activation), constraint) =>
//          if (total_traces > 0)
//            TargetBranchedConstraint(rule, activation, constraint.targets.filter(_._2 / total_traces >= support))
//          else
//            null
//        }
//        .filter(!_.targets.isEmpty)
//        .toDS()
//    } catch {
//      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[TargetBranchedConstraint]
//    }
//  }

  // `OR` policy
  def getORBranchedConstraints(constraints: Dataset[PairConstraint], totalTraces: Long)
                              (implicit spark: SparkSession): Dataset[TargetBranchedConstraint] = {
    import spark.implicits._

    // Explode traces to create individual rows for (rule, eventA, eventB, trace)
    val exploded = constraints
      .withColumn("trace", explode($"traces"))
      .select($"rule", $"eventA", $"eventB", $"trace")

    // Compute unique traces for each (rule, eventA, eventB)
    val traceCounts = exploded
      .groupBy($"rule", $"eventA", $"eventB")
      .agg(collect_set($"trace").as("uniqueTraces"))

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
        val optimalSuffixes = findOptimalSuffixes(eventBData)

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

  private def findOptimalSuffixes(eventBData: Seq[(String, Seq[String])]): Seq[(String, Seq[String])] = {
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

    // Refactor the following w.r.t. every policy
    val branchedConstraints: Dataset[TargetBranchedConstraint] = if (policy == "OR") {
      getORBranchedConstraints(constraints, totalTraces)(spark)
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
