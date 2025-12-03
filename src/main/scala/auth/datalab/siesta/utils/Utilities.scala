package auth.datalab.siesta.utils

import auth.datalab.siesta.model.{Config, Event, MetaData, PairFull, BranchingPolicy, BranchingType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scopt.OParser

import scala.util.Try

object Utilities {

  /**
   * Read environment variable
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

  def printConfig(config: Config): Unit = {
    println(s"[Log Name]\t\t${config.logName}")
    println(s"[Support]\t\t${config.support}")

    if (config.isBranchingEnabled) {
      val effectiveBound = config.getEffectiveBranchingBound
      println(s"[Branching]\t\tPolicy=${config.getBranchingPolicyName}, " +
        s"Type=${config.getBranchingTypeName}, " +
        s"Bound=${if(effectiveBound != Int.MaxValue) effectiveBound else "None"}, " +
        s"Drop=${config.dropFactor.getOrElse("None")}, " +
        s"FilterRare=${config.filterRare}, " +
        s"FilterUnderBound=${config.filterUnderBound}")
    } else {
      println("[Branching]\t\tDisabled")
    }

    println(s"[Options]\t\tRediscovery=${config.hardRediscovery}, " +
      s"QuickMining=${config.quickMining}")
  }

  /**
   * Checks if branching is enabled based on the configuration
   * @param config The configuration object
   * @return true if branching should be applied
   */
  def isBranchingEnabled(config: Config): Boolean = config.isBranchingEnabled

  /**
   * Checks if branching is enabled based on policy string (for legacy compatibility)
   * @param branchingPolicy The branching policy string
   * @return true if branching should be applied
   */
  def isBranchingEnabled(branchingPolicy: String): Boolean = {
    BranchingPolicy.fromString(branchingPolicy).isDefined
  }

  /**
   * Creates and returns the command-line argument parser
   * 
   * @return OParser for Config
   */
  def createArgumentParser(): OParser[Unit, Config] = {
    val builder = OParser.builder[Config]
    import builder._
    OParser.sequence(
      programName("SIESTA CBDeclare Constraints Mining"),
      head("SIESTA CBDeclare Module", "1.0"),

      opt[String]('l', "logname")
        .required()
        .action((x, c) => c.copy(logName = x))
        .text("S3 logname is required"),

      opt[Double]('s', "support")
        .action((x, c) => c.copy(support = x))
        .text("Support value, default is 0"),

      opt[String]('p', "branchingPolicy")
        .action((x, c) => c.copy(branchingPolicy = BranchingPolicy.fromString(x)))
        .text("Branching policy (AND, OR, XOR), default is None"),

      opt[String]('t', "branchingType")
        .action((x, c) => c.copy(branchingType = Some(BranchingType.fromString(x))))
        .text("Branching type (SOURCE, TARGET), required when using branching policy"),

      opt[Int]('b', "branchingBound")
        .action((x, c) => c.copy(branchingBound = x))
        .text("Branching bound, default is 0"),

      opt[Double]('d', "dropFactor")
        .action((x, c) => c.copy(dropFactor = Some(x)))
        .text("Reduction Drop factor, default is None (disabled)"),

      opt[Boolean]('r', "filterRare")
        .action((x, c) => c.copy(filterRare = x))
        .text("Filter out rare events, default is false"),

      opt[Boolean]('u', "filterUnderBound")
        .action((x, c) => c.copy(filterUnderBound = x))
        .text("Filter out under-bound templates, default is false"),

      opt[Boolean]('h', "hardRediscover")
        .action((x, c) => c.copy(hardRediscovery = x))
        .text("Hard rediscovery, default is false"),

      opt[Boolean]('q', "quickMining")
        .action((x, c) => c.copy(quickMining = x))
        .text("Quick mining, default is false"),

      opt[String]('o', "outputPath")
        .action((x, c) => c.copy(outputPath = x))
        .text("Output directory path, default is './output'"),
    )
  }

  /**
   * Parses command-line arguments and returns the configuration
   * 
   * @param args Command-line arguments
   * @return Some(Config) if parsing successful, None otherwise
   */
  def parseArguments(args: Array[String]): Option[Config] = {
    OParser.parse(createArgumentParser(), args, Config())
  }
}