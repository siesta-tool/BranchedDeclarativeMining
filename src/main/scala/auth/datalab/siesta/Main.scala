package auth.datalab.siesta

import auth.datalab.siesta.Structs.{Config, Event}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession, functions}
import org.apache.spark.storage.StorageLevel

import java.io.{BufferedWriter, FileWriter}
import scopt.OParser

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer

object Main {

  def main(args: Array[String]): Unit = {

    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("SIESTA CBDeclare Constraints Mining"),
        head("SIESTA CBDecalre Module", "1.0"),

        opt[String]('l', "logname")
          .required()
          .action((x, c) => c.copy(logName = x))
          .text("S3 logname is required"),

        opt[Double]('s', "support")
          .action((x, c) => c.copy(support = x))
          .text("Support value, default is 0"),

        opt[String]('p', "branchingPolicy")
          .action((x, c) => c.copy(branchingPolicy = x))
          .text("Branching policy, default is null"),

        opt[String]('t', "branchingType")
          .action((x, c) => c.copy(branchingType = x.toUpperCase))
          .text("Branching type, default is 'TARGET' if policy is set"),

        opt[Int]('b', "branchingBound")
          .action((x, c) => c.copy(branchingBound = x))
          .text("Branching bound, default is 0"),

        opt[Double]('d', "dropFactor")
          .action((x, c) => c.copy(dropFactor = x))
          .text("Reduction Drop factor, default is 2.5"),

        opt[Boolean]('r', "filterRare")
          .action((x, c) => c.copy(filterRare = x))
          .text("Filter out rare events, default is false"),

        opt[Boolean]('u', "filterUnderBound")
          .action((x, c) => c.copy(filterUnderBound = x))
          .text("Filter out under-bound templates, default is false"),

        opt[Boolean]('h', "hardRediscover")
          .action((x, c) => c.copy(hardRediscovery = x))
          .text("Hard rediscovery, default is false")
      )
    }

    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        val s3Connector = new S3Connector()
        s3Connector.initialize(config.logName)
        println(s"Log: ${config.logName}")
        println(s"Support: ${config.support}")
        println(s"Branching Policy: ${config.branchingPolicy}")
        println(s"Branching Type: ${config.branchingType}")
        println(s"Branching Bound: ${config.branchingBound}")
        println(s"Reduction Drop factor: ${config.dropFactor}")
        println(s"Filter out Rare events: ${config.filterRare}")
        println(s"Filter out under-bound templates: ${config.filterUnderBound}")
        println(s"Find all constraints from beginning: ${config.hardRediscovery}")

        val support = config.support
        var branchingPolicy = config.branchingPolicy
        val branchingType = config.branchingType
        val branchingBound = config.branchingBound
        val filterRare = config.filterRare
        val dropFactor = config.dropFactor
        val filterUnderBound = if (branchingBound > 0) config.filterUnderBound else false
        val hardRediscover = config.hardRediscovery
        val metaData = s3Connector.get_metadata()

        val spark = SparkSession.builder().getOrCreate()
        spark.time({ import spark.implicits._

          /** Extract all preprocessed events of the log from S3 */
          val events: Dataset[Event] = s3Connector.get_events_sequence_table()
          events.persist(StorageLevel.MEMORY_AND_DISK)
//          println(s"Total events: ${events.count()}")

          /** Create a map event_type -> #total occurrences in log */
          val eventTypeOccurrencesMap: scala.collection.Map[String, Long] = events
            .select("eventType", "trace")
            .groupBy("eventType")
            .agg(functions.count("trace").alias("unique"))
            .collect()
            .map(row => (row.getAs[String]("eventType"), row.getAs[Long]("unique")))
            .toMap

          val traceIds: Set[String] = events.select("trace").distinct().rdd.map(x => x.getAs[String]("trace")).collect().toSet
          val bTraceIds = spark.sparkContext.broadcast(traceIds)
//          println(s"Total traces: ${traceIds.size}")

          /** Retain separately only the newly arrived events */
          val prevMiningTs = metaData.last_declare_mined
          val newEvents: Dataset[Event] = if (prevMiningTs.isEmpty || hardRediscover) events
                          else events.filter(e => {/*true*/ Timestamp.valueOf(prevMiningTs).before(Timestamp.valueOf(e.ts))})

          /** Distinguish traces that only evolved; the bounds include the new events */
          val evolvedTracesBounds: scala.collection.Map[String, (Int, Int)] = newEvents
            .groupBy("trace")
            .agg(functions.min("pos"), functions.max("pos"))
            .map(x => (x.getAs[String]("trace"), (x.getAs[Int]("min(pos)"), x.getAs[Int]("max(pos)"))))
            .rdd
            .keyBy(_._1)
            .mapValues(_._2)
            .collectAsMap()
          val bEvolvedTracesBounds = spark.sparkContext.broadcast(evolvedTracesBounds)
          val evolvedTracesIds = evolvedTracesBounds.keys.toSeq
          val bEvolvedTracesIds = spark.sparkContext.broadcast(evolvedTracesIds)

          /**
           * Retain the all events that belong to an evolved trace since already-mined constraints may be affected from these traces
           */
          val affectedEvents = events.filter(functions.col("trace").isin(bEvolvedTracesIds.value:_*))
          affectedEvents.count()
          affectedEvents.persist(StorageLevel.MEMORY_AND_DISK)

          // TODO: evaluate if we need this
          // All possible event_type pairs matrix
          val allEventOccurrences = s3Connector.get_single_table().rdd.groupBy(_._1).map(x =>(x._1, x._2.map(_._2).toSet))
          val activityMatrix = allEventOccurrences.cartesian(allEventOccurrences)
          activityMatrix.persist(StorageLevel.MEMORY_AND_DISK)

          /**
           * Position patterns
           */
          val position = DeclareMining.extractPositionConstraints(
            logName = metaData.log_name,
            affectedEvents = affectedEvents,
            bEvolvedTracesBounds = bEvolvedTracesBounds,
            supportThreshold = support,
            totalTraces = metaData.traces,
            branchingPolicy = branchingPolicy,
            branchingBound = branchingBound,
            filterRare = filterRare,
            dropFactor = dropFactor,
            filterUnderBound = filterUnderBound,
            hardRediscover = hardRediscover)

          /** Existence patterns */
          val existence = DeclareMining.extractExistenceConstraints(
            logName = metaData.log_name,
            affectedEvents = affectedEvents,
            supportThreshold = support,
            totalTraces = metaData.traces,
            bTraceIds = bTraceIds,
            branchingPolicy = branchingPolicy,
            branchingBound = branchingBound,
            filterRare = filterRare,
            dropFactor = dropFactor,
            filterUnderBound = filterUnderBound)

          /** Unordered patterns */
          val unorder = DeclareMining.extractUnordered(
            logName = metaData.log_name,
            bEvolvedTracesBounds = bEvolvedTracesBounds,
            affectedEvents = affectedEvents,
            bTraceIds = bTraceIds,
            activityMatrix = activityMatrix,
            allEventOccurrences = allEventOccurrences,
            supportThreshold = support,
            branchingPolicy = branchingPolicy,
            branchingBound = branchingBound,
            branchingType = branchingType,
            filterRare = filterRare,
            dropFactor = dropFactor,
            filterUnderBound = filterUnderBound)

          /** Ordered patterns */
          val ordered = DeclareMining.extractOrdered(metaData.log_name, affectedEvents, bEvolvedTracesBounds,
            bTraceIds, activityMatrix, metaData.traces, support, branchingPolicy, branchingType,
            branchingBound, filterRare = filterRare, dropFactor = dropFactor, filterBounded = filterUnderBound, hardRediscover = hardRediscover)


          val l = ListBuffer[String]()
          events.unpersist()
          activityMatrix.unpersist()
          affectedEvents.unpersist()

          ordered.union(position).union(existence).union(unorder).foreach(x => {
            val support = f"${x._3.length.toDouble / traceIds.size}%.3f"
            l += s"${x._1}|${x._2}|${x._3.mkString(",")}|$support\n"
          })
          println("Constraints mined: " + l.size)

          branchingPolicy = if (branchingPolicy == null) "none" else branchingPolicy

          val file = "constraints_" + config.logName + "_s" + support.toString + "_b" + branchingBound.toString + "_p" + branchingPolicy + ".txt"
          val writer = new BufferedWriter(new FileWriter(file))
          l.toList.sorted.foreach(writer.write)
          writer.close()

          if (!newEvents.isEmpty) {
            metaData.last_declare_mined = newEvents.rdd
              .map(x => Timestamp.valueOf(x.ts)).reduce((x, y) => { if (x.after(y)) x else y }).toString
            s3Connector.write_metadata(metaData)
          }
        })
      case _ =>
        throw new IllegalArgumentException("Wrong configuration!")
    }

  }

}
