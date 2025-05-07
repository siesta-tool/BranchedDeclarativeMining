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
        programName("SIESTA Declare Constraints Mining"),
        head("SIESTA", "1.0"),

        arg[String]("<logname>")
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
        println(s"Support: ${config.support}")
        println(s"Branching Policy: ${config.branchingPolicy}")
        println(s"Branching Type: ${config.branchingType}")
        println(s"Branching Bound: ${config.branchingBound}")
        println(s"Reduction Drop factor: ${config.dropFactor}")
        println(s"Filter out Rare events: ${config.filterRare}")
        println(s"Filter out under-bound templates: ${config.filterUnderBound}")
        println(s"Find all constraints from beginning: ${config.hardRediscovery}")

        val support = config.support
        val branchingPolicy = config.branchingPolicy
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

          /** Create a map event_type -> #total occurrences in log */
          val eventTypeOccurrencesMap: scala.collection.Map[String, Long] = events
            .select("eventType", "trace")
            .groupBy("eventType")
            .agg(functions.count("trace").alias("unique"))
            .collect()
            .map(row => (row.getAs[String]("eventType"), row.getAs[Long]("unique")))
            .toMap
          val bEventTypeOccurrencesMap = spark.sparkContext.broadcast(eventTypeOccurrencesMap)

          val traceIds: Set[String] = events.select("trace").distinct().rdd.map(x => x.getAs[String]("trace")).collect().toSet
          val bTraceIds = spark.sparkContext.broadcast(traceIds)

          /** Retain separately only the newly arrived events */
          val prevMiningTs = metaData.last_declare_mined
          val newEvents: Dataset[Event] = if (prevMiningTs.isEmpty || hardRediscover) events
                          else events.filter(e => {Timestamp.valueOf(prevMiningTs).before(Timestamp.valueOf(e.ts))})

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
          val activity_matrix: RDD[(String, String)] = Utilities.get_activity_matrix(bEventTypeOccurrencesMap.value)
          activity_matrix.persist(StorageLevel.MEMORY_AND_DISK)


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
            activity_matrix = activity_matrix,
            supportThreshold = support,
            branchingPolicy = branchingPolicy,
            branchingBound = branchingBound,
            branchingType = branchingType,
            filterRare = filterRare,
            dropFactor = dropFactor,
            filterUnderBound = filterUnderBound)

          //extract order relations
          val ordered = DeclareMining.extractOrdered(metaData.log_name, affectedEvents, bEvolvedTracesBounds,
            bTraceIds, activity_matrix, metaData.traces, support, branchingPolicy, branchingType,
            branchingBound, filterRare = filterRare, dropFactor = dropFactor, filterBounded = filterUnderBound, hardRediscover = hardRediscover)

          //handle negative pairs = pairs that does not appear not even once in the data
          //      val negative_pairs: Array[(String, String)] = DeclareMining.handle_negatives(metaData.log_name,
          //        activity_matrix)

          val l = ListBuffer[String]()
          //each negative pair wil have 100% support in the constraints not-coexist, exclusive-choice, not succession and not chain-succession
          //      negative_pairs.foreach(x => {
          //        //      l+=s"not-chain-succession|${x._1}|${x._2}|1.000\n"
          //        l += s"not-succession|${x._1}|${x._2}|1.000\n"
          //      })
          //      negative_pairs.filter(x => x._1 < x._2).foreach(x => {
          //        if (negative_pairs.contains((x._2, x._1))) {
          //          l += s"not co-existence|${x._1}|${x._2}|1.000\n"
          //          l += s"exclusive choice|${x._1}|${x._2}|1.000\n"
          //        }
          //
          //      })


          //      position_constraints.foreach(x => {
          //        val formattedDouble = f"${x.support}%.3f"
          //        l += s"${x.rule}|${x.source}|$formattedDouble\n"
          //      })
          //      existence_constraints.foreach(x => {
          //        val formattedDouble = f"${x.support}%.3f"
          //        l += s"${x.rule}|${x.source}|${x.target}|$formattedDouble\n"
          //      })
          //      unorder.foreach(x => {
          //        val formattedDouble = f"${x.occurrences}%.3f"
          //        l += s"${x.rule}|${x.eventA}|${x.eventB}|$formattedDouble\n"
          //      })

//          ordered_constraints match {
//            case Right(x) => x match {
//              case Left(x) => x.foreach(x => {
//                val formattedDouble = f"${x.support}%.7f"
//                l += s"${x.rule}|${x.source}|${x.targets.mkString("(", ", ", ")")}|$formattedDouble\n"
//              })
//              case Right(x) => x.foreach(x => {
//                val formattedDouble = f"${x.support}%.7f"
//                l += s"${x.rule}|${x.sources.mkString("(", ", ", ")")}|${x.target}|$formattedDouble\n"
//              })
//            }
//            case Left(x) => x.foreach(x => {
//              val formattedDouble = f"${x.support}%.7f"
//              l += s"${x.rule}|${x.activation}|${x.target}|$formattedDouble\n"
//            })
//          }

//          //TODO: change support/ total traces to broadcasted variables
//
          events.unpersist()
          activity_matrix.unpersist()
          affectedEvents.unpersist()

          ordered.union(position).union(existence).union(unorder).foreach(x => {
            val formattedDouble = f"${x._3}%.7f"
            l += s"${x._1}|${x._2}|$formattedDouble\n"
          })
          println("Constraints mined: " + l.size)
          val file = "constraints_" + config.logName + "_s" + config.support.toString + "_b" + config.branchingBound + "_" +
            config.branchingPolicy + ".txt"
          val writer = new BufferedWriter(new FileWriter(file))
          l.toList.foreach(writer.write)
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
