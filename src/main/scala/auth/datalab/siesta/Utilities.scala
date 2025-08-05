package auth.datalab.siesta

import auth.datalab.siesta.Structs.{Event, MetaData, PairFull}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Try

object Utilities {

  /**
   * Read environment variable
   *p
   * @param key The key of the variable
   * @return The variable
   * @throws NullPointerException if the variable does not exist
   */
  @throws[NullPointerException]
  def readEnvVariable(key: String): String = {
    val envVariable = System.getenv(key)
    if (envVariable == null) throw new NullPointerException("Error! Environment variable " + key + " is missing")
    envVariable
  }

  def load_metadata(metaDataObj:DataFrame):MetaData = {
    metaDataObj.collect().map(x => {
      val last_declare_mined = Try(x.getAs[String]("last_declare_mined")).getOrElse("")
      val start_ts = Try(x.getAs[String]("start_ts")).getOrElse("")
      val last_ts = Try(x.getAs[String]("last_ts")).getOrElse("")

      MetaData(traces = x.getAs("traces"),
        events = x.getAs("events"),
        pairs = x.getAs("pairs"),
        lookback = x.getAs("lookback"),
        has_previous_stored = true,
        filename = x.getAs("filename"),
        streaming = x.getAs("streaming"),
        log_name = x.getAs("log_name"),
        mode = x.getAs("mode"),
        compression = x.getAs("compression"),
        start_ts = start_ts,
        last_ts = last_ts,
        last_declare_mined = last_declare_mined)}).head
  }

  def get_activity_matrix(event_types_occurrences:scala.collection.Map[String, Long]):RDD[(String,String)]={
    val keys: Iterable[String] = event_types_occurrences.keys

    val cartesianProduct: Iterable[(String, String)] = for {
      key1 <- keys
      key2 <- keys
    } yield (key1, key2)

    SparkSession.builder().getOrCreate().sparkContext.parallelize(cartesianProduct.toSeq)
  }
}