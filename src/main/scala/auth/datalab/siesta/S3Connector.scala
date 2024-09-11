package auth.datalab.siesta

import auth.datalab.siesta.Structs.{Event, MetaData, PairFull}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import java.sql.Timestamp

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
//      .master("local[*]")
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
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")



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
      .flatMap(x => {
        val pEvents = x.getAs[Seq[Row]]("events")
          .zipWithIndex
          .map(y => (y._1.getString(0), y._1.getString(1), y._2)) // map to eventType,timestamp,index
        val trace_id = x.getAs[String]("trace_id")
        pEvents.map(x => Event(x._1, x._2, x._3, trace_id))
      })
  }

  def get_index_table(): Dataset[PairFull] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    spark.read.parquet(this.index_table)
      .rdd.flatMap(row => {
        val eventA = row.getAs[String]("eventA")
        val eventB = row.getAs[String]("eventB")
        row.getAs[Seq[Row]]("occurrences").flatMap(oc => {
          val id = oc.getString(0)
          oc.getAs[Seq[Row]](1).map(o => {
            Structs.PairFull(eventA, eventB, id, o.getInt(0), o.getInt(1))
          })
        })
      }).toDS()
  }


}
