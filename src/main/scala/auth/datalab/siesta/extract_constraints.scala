package auth.datalab.siesta

import auth.datalab.siesta.Structs.{ActivityExactly, PairConstraint, PositionConstraint}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

object extract_constraints {
  def main(args: Array[String]): Unit = {

    val s3Connector = new S3Connector()
    s3Connector.initialize(args(0))
    val support = args(1).toDouble

    val metaData = s3Connector.get_metadata()
    val spark = SparkSession.builder().getOrCreate()
    spark.time({
      import spark.implicits._
      //activity matrix
      val event_types_occurrences: scala.collection.Map[String, Long] = s3Connector.get_events_sequence_table()
        .select("event_type", "trace_id")
        .groupBy("event_type")
        .agg(functions.count("trace_id").alias("unique"))
        .collect()
        .map(row => (row.getAs[String]("event_type"), row.getAs[Long]("unique")))
        .toMap
      val bEvent_types_occurrences = spark.sparkContext.broadcast(event_types_occurrences)

      //all possible activity pair matrix
      val activity_matrix: RDD[(String, String)] = Utilities.get_activity_matrix(event_types_occurrences)
      activity_matrix.persist(StorageLevel.MEMORY_AND_DISK)

      //      Position extraction
      val position_path = s"""s3a://siesta/${metaData.log_name}/declare/position.parquet/"""
      val position_state = spark.read.parquet(position_path).as[PositionConstraint]
      val position_constraints = DeclareMining.extractAllPositionConstraints(position_state.rdd, support, metaData.traces)

      //      Existence extraction
      val existence_path = s"""s3a://siesta/${metaData.log_name}/declare/existence.parquet/"""
      val existence_state = spark.read.parquet(existence_path)
        .map(x => ActivityExactly(x.getString(0), x.getInt(1), x.getLong(2)))
      val existence_constraints = DeclareMining.extractAllExistenceConstraints(existence_state, support, metaData.traces)

      //      Unordered extraction
      val i_path = s"""s3a://siesta/${metaData.log_name}/declare/unorder/i.parquet/"""
      val u_path = s"""s3a://siesta/${metaData.log_name}/declare/unorder/u.parquet/"""
      val u = spark.read.parquet(u_path).map(x => (x.getString(0), x.getLong(1)))
      val i = spark.read.parquet(i_path).map(x => (x.getString(0), x.getString(1), x.getLong(2)))
      val unordered_constraints = DeclareMining.extractAllUnorderedConstraints(u, i, activity_matrix, support, metaData.traces)

      // Ordered extraction
      val order_path = s"""s3a://siesta/${metaData.log_name}/declare/order.parquet/"""
      val ordered_state = spark.read.parquet(order_path)
        .map(x => PairConstraint(x.getString(0), x.getString(1), x.getString(2), x.getDouble(3)))
      val ordered_constraints = DeclareMining.extractAllOrderedConstraints(ordered_state, bEvent_types_occurrences, activity_matrix, support)

      // Negative pairs
      val negative_path = s"""s3a://siesta/${metaData.log_name}/declare/negatives.parquet/"""
      val negative_state = spark.read.parquet(negative_path)
        .map(x => (x.getString(0), x.getString(1))).collect()


      val l = ListBuffer[String]()
      position_constraints.foreach(x => {
        val formattedDouble = f"${x.occurrences}%.3f"
        l += s"${x.rule}|${x.event_type}|$formattedDouble\n"
      })

      existence_constraints.foreach(x => {
        val formattedDouble = f"${x.occurrences}%.3f"
        l += s"${x.rule}|${x.event_type}|${x.n}|$formattedDouble\n"
      })

      unordered_constraints.foreach(x => {
        val formattedDouble = f"${x.occurrences}%.3f"
        l += s"${x.rule}|${x.eventA}|${x.eventB}|$formattedDouble\n"
      })
      ordered_constraints.foreach(x => {
        val formattedDouble = f"${x.occurrences}%.3f"
        l += s"${x.rule}|${x.eventA}|${x.eventB}|$formattedDouble\n"
      })

      negative_state.foreach(x => {
        l += s"not-succession|${x._1}|${x._2}|1.000\n"
      })
      negative_state.filter(x => x._1 < x._2).foreach(x => {
        if (negative_state.contains((x._2, x._1))) {
          l += s"not co-existence|${x._1}|${x._2}|1.000\n"
          l += s"exclusive choice|${x._1}|${x._2}|1.000\n"
        }

      })

      l.foreach(print)


    })
  }
}
