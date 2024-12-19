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
//        .keyBy(x => (x.rule, x.prefix))
//        // Discard meaningless-for-branching rules
//        .filter(x => !rulesToIgnore.contains(x._1._1))
//        // Merge the suffix arrays
//        .reduceByKey((a, b) => TargetBranchedConstraint(
//          a.rule,
//          a.prefix,
//          a.suffix ++ b.suffix
//        ))
//        // Keep branches that hold over the support threshold
//        .map { case ((rule, prefix), constraint) =>
//          if (total_traces > 0)
//            TargetBranchedConstraint(rule, prefix, constraint.suffix.filter(_._2 / total_traces >= support))
//          else
//            null
//        }
//        .filter(!_.suffix.isEmpty)
//        .toDS()
//    } catch {
//      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[TargetBranchedConstraint]
//    }
//  }

  // `OR` policy
  def getORBranchedConstraints(constraints: Dataset[PairConstraint])
                              (implicit spark: SparkSession): Dataset[TargetBranchedConstraint] = {
    import spark.implicits._

    // Step 1: Explode traces to create individual rows for (rule, eventA, eventB, trace)
    val exploded = constraints
      .withColumn("trace", explode($"traces"))
      .select($"rule", $"eventA", $"eventB", $"trace")

    // Step 2: Compute unique traces for each (rule, eventA, eventB)
    val traceCounts = exploded
      .groupBy($"rule", $"eventA", $"eventB")
      .agg(collect_set($"trace").as("uniqueTraces")) // Collect unique traces

    // Step 3: Group by (rule, eventA), calculate optimal suffixes
    val grouped = traceCounts
      .as[(String, String, String, Seq[String])] // Convert to typed Dataset
      .groupByKey { case (rule, eventA, _, _) => (rule, eventA) } // Group by (rule, eventA)
      .mapGroups { case ((rule, eventA), rows) =>
        // Create a struct for each target event (eventB) and each list of traces, w.r.t. activation (eventA)
        val eventBData = rows.toSeq.map { case (_, _, eventB, uniqueTraces) =>
          (eventB, uniqueTraces)
        }

        // Step 4: Find the optimal subset of eventBs
        val optimalSuffixes = findOptimalSuffixes(eventBData)

        // Compute total unique traces for the optimal suffixes
        val totalUniqueTraces = optimalSuffixes.flatMap(_._2).distinct.size.toDouble

        // Create TargetBranchedConstraint
        TargetBranchedConstraint(
          rule = rule,
          prefix = eventA,
          suffix = optimalSuffixes.map(_._1).toArray, // Extract eventBs
          support = totalUniqueTraces
        )
      }

    grouped
  }

  private def findOptimalSuffixes(eventBData: Seq[(String, Seq[String])]): Seq[(String, Seq[String])] = {
    var suffixes = eventBData // Start with all eventBs
    var uniqueTraces = suffixes.flatMap(_._2).distinct.size // Total unique traces

    // Iteratively remove eventBs causing the least reduction in unique traces
    while (suffixes.size > 1) {
      // Find the eventB that causes the smallest reduction in unique traces
      val candidateToRemove = suffixes
        .map { suffix =>
          val updatedSuffixes = suffixes.filterNot(_ == suffix) // Remove this suffix
          val updatedTraces = updatedSuffixes.flatMap(_._2).distinct.size // Recalculate unique traces
          (suffix, updatedSuffixes, updatedTraces)
        }
        .minBy { case (_, _, updatedTraces) => uniqueTraces - updatedTraces }

      // Calculate the reduction in unique traces
      val reduction = uniqueTraces - candidateToRemove._3

      // Stop if the reduction exceeds the threshold
      if (reduction > uniqueTraces.toDouble / 2) {
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

  def extractAllOrderConstraints (constraints: Dataset[PairConstraint],
                                  bUnique_traces_to_event_types: Broadcast[scala.collection.Map[String, Long]],
                                  activity_matrix: RDD[(String, String)],
                                  support: Double,
                                  policy: String): Any = { }

  def extractAllUnorderedConstraints(merge_u: Dataset[(String, Long)], merge_i: Dataset[(String, String, Long)],
                                     activity_matrix: RDD[(String, String)], support: Double, total_traces: Long)
  : Any = { }
}
