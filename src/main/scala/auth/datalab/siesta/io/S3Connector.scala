package auth.datalab.siesta.io

import auth.datalab.siesta.model.Structs.{Event, MetaData, PairFull}
import auth.datalab.siesta.utils.Utilities
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

class S3Connector {

  private var seq_table: String = _
  private var detailed_table: String = _
  private var meta_table: String = _
  private var single_table: String = _
  private var last_checked_table: String = _
  private var index_table: String = _
  private var count_table: String = _
  private var logname: String = _

  /**
   * Spark initializes the connection to S3 utilizing the hadoop properties and the aws-bundle library
   */
  def initialize(logname: String): Unit = {
    this.logname = logname
    lazy val spark = SparkSession.builder()
      .appName("Declare extraction")
      .master("local[*]")
      .getOrCreate()

    val s3accessKeyAws = Utilities.readEnvVariable("s3accessKeyAws")
    val s3secretKeyAws = Utilities.readEnvVariable("s3secretKeyAws")
    val s3ConnectionTimeout = Utilities.readEnvVariable("s3ConnectionTimeout")
    val s3endPointLoc: String = Utilities.readEnvVariable("s3endPointLoc")

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3endPointLoc)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3accessKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3secretKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.timeout", s3ConnectionTimeout)

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    // Configure SSL based on endpoint - disable for localhost/MinIO
    val sslEnabled = !s3endPointLoc.startsWith("localhost") && !s3endPointLoc.startsWith("127.0.0.1")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", sslEnabled.toString)
    
    // Additional MinIO/S3 compatible settings
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.attempts.maximum", "3")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.establish.timeout", "10000")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.retry.throttle.limit", "20")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.retry.throttle.interval", "1000ms")
    
    // Disable multipart uploads for small files (helps with MinIO)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.multipart.threshold", "67108864") // 64MB
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.multipart.size", "16777216") // 16MB


    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")
    spark.sparkContext.setLogLevel("WARN")

    seq_table = s"""s3a://siesta/${logname}/seq.parquet/"""
    detailed_table = s"""s3a://siesta/${logname}/detailed.parquet/"""
    meta_table = s"""s3a://siesta/${logname}/meta.parquet/"""
    single_table = s"""s3a://siesta/${logname}/single.parquet/"""
    last_checked_table = s"""s3a://siesta/${logname}/last_checked.parquet/"""
    index_table = s"""s3a://siesta/${logname}/index.parquet/"""
    count_table = s"""s3a://siesta/${logname}/count.parquet/"""
  }

  def get_metadata(): MetaData = {
    val spark = SparkSession.builder().getOrCreate()
    val metaDataObj = try {
      spark.read.parquet(meta_table)
    } catch {
      case _: org.apache.spark.sql.AnalysisException => null
    }

    //calculate new metadata object
    val metaData = if (metaDataObj == null) {
      return null
    } else {
      Utilities.load_metadata(metaDataObj)
    }
    metaData
  }

  def write_metadata(metaData: MetaData): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(Seq(metaData))
    val df = rdd.toDF()
    df.write.mode(SaveMode.Overwrite).parquet(meta_table)
  }

  def get_events_sequence_table(): Dataset[Event] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    spark.read.parquet(this.seq_table)
      .map(x => {
        Event(trace_id = x.getAs[String]("trace_id"), timestamp = x.getAs[String]("timestamp"), event_type = x.getAs[String]("event_type"),
          pos = x.getAs[Int]("position"))
      })
  }

  def get_index_table(): Dataset[PairFull] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    spark.read.parquet(this.index_table)
      .rdd.map(row => {
        val eventA = row.getAs[String]("eventA")
        val eventB = row.getAs[String]("eventB")
        val posA = row.getAs[Int]("positionA")
        val posB = row.getAs[Int]("positionB")
        val trace_id = row.getAs[String]("trace_id")
        PairFull(eventA, eventB, trace_id, posA, posB)
      }).toDS()
  }

  def get_single_table(): Dataset[(String, String)] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    spark.read.parquet(this.single_table)
      .map(x => (x.getAs("event_type").toString, x.getAs("trace_id").toString))
  }


}
