package auth.datalab.siesta

import auth.datalab.siesta.Structs.{BranchedPairConstraint, BranchedSingleConstraint}
import org.apache.spark.sql.{Dataset, SparkSession}


object TBDeclare {

  def combine_constraints(logname: String, support: Double, total_traces: Long): Dataset[BranchedPairConstraint]  = {
    val spark = SparkSession.builder().getOrCreate()
    val rulesToIgnore = Set("last", "first", "existence", "absence", "exactly")

    import spark.implicits._
    val order_path = s"""s3a://siesta/$logname/declare/order.parquet/"""

    try {
      spark.read.parquet(order_path)
        .map(x => BranchedPairConstraint(
          x.getAs[String]("rule"),
          x.getAs[String]("eventA"),
          Array((x.getAs[String]("eventB"), x.getAs[Double]("occurrences")))
        ))
        .rdd
        .keyBy(x => (x.rule, x.prefix))
        // Discard meaningless-for-branching rules
        .filter(x => !rulesToIgnore.contains(x._1._1))
        // Merge the suffix arrays
        .reduceByKey((a, b) => BranchedPairConstraint(
          a.rule,
          a.prefix,
          a.suffix ++ b.suffix
        ))
        // Keep branches that hold over the support threshold
        .map { case ((rule, prefix), constraint) =>
          if (total_traces > 0)
            BranchedPairConstraint(rule, prefix, constraint.suffix.filter(_._2 / total_traces >= support))
          else
            null
        }
        .filter(!_.suffix.isEmpty)
        .toDS()
    } catch {
      case _: org.apache.spark.sql.AnalysisException => spark.emptyDataset[BranchedPairConstraint]
    }
  }

  // `OR` policy

  def getORBranchedConstraints(logname: String, support: Double, total_traces: Long): Dataset[BranchedSingleConstraint]
  = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    combine_constraints(logname, support, total_traces)
      .map { x =>
      val concatenatedSuffix = x.suffix.map(_._1).mkString("|")
      val supportSum = x.suffix.map(_._2).sum
        BranchedSingleConstraint(x.rule, x.prefix, concatenatedSuffix, supportSum)
    }}

}
