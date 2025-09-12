package auth.datalab.siesta

import auth.datalab.siesta.Structs.{Config, Event}
import auth.datalab.siesta.Utilities.{printConfig, parseArguments}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession, functions}
import org.apache.spark.storage.StorageLevel

import java.io.{BufferedWriter, FileWriter}

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer

object Main {

  def main(args: Array[String]): Unit = {

    parseArguments(args) match {
      case Some(config) =>
        val s3Connector = new S3Connector()
        s3Connector.initialize(config.logName)

        printConfig(config)

        val support = config.support
        val branchingPolicy = config.getEffectiveBranchingPolicy
        val branchingType = config.getEffectiveBranchingType
        val branchingBound = config.branchingBound
        val filterRare = config.filterRare
        val dropFactor = config.dropFactor
        val filterUnderBound = if (branchingBound > 0) config.filterUnderBound else false
        val hardRediscover = config.hardRediscovery
        val quickMining = config.quickMining
        val metaData = s3Connector.get_metadata()

        val spark = SparkSession.builder().getOrCreate()
        spark.time({ import spark.implicits._

          /** Extract all preprocessed events of the log from S3 */
          val events: Dataset[Event] = s3Connector.get_events_sequence_table()
          events.persist(StorageLevel.MEMORY_AND_DISK)

          val traceIds: Set[String] = events.select("trace").distinct().rdd.map(x => x.getAs[String]("trace")).collect().toSet
          val bTraceIds = spark.sparkContext.broadcast(traceIds)

          /** Retain separately only the newly arrived events */
          val prevMiningTs = metaData.last_declare_mined
          val newEvents: Dataset[Event] = if (prevMiningTs.isEmpty || hardRediscover) events
                          else events.filter(e => {Timestamp.valueOf(prevMiningTs).before(Timestamp.valueOf(e.ts))})
//                          else events.filter(e => {true})

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

          val allEventTypes = s3Connector.get_single_table().rdd.groupBy(_._1).keys.collect().toSet

          /** Position patterns */
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
            bEvolvedTracesBounds = bEvolvedTracesBounds,
            supportThreshold = support,
            totalTraces = metaData.traces,
            bTraceIds = bTraceIds,
            branchingPolicy = branchingPolicy,
            branchingBound = branchingBound,
            filterRare = filterRare,
            dropFactor = dropFactor,
            filterUnderBound = filterUnderBound)

          /** Unordered patterns */
          DeclareMining.incrementally_maintain_unorder_state(metaData, bEvolvedTracesBounds, newEvents, allEventTypes,affectedEvents)
          val unorder = DeclareMining.extractUnordered(metaData)

          /** Ordered patterns */
          val ordered = DeclareMining.extractOrdered(metaData.log_name, affectedEvents, bEvolvedTracesBounds,
            bTraceIds, metaData.traces, support, branchingPolicy, branchingType,
            branchingBound, filterRare = filterRare, dropFactor = dropFactor, filterBounded = filterUnderBound, hardRediscover = hardRediscover)

          val allConstraints = ordered.union(position).union(existence).union(unorder)
          events.unpersist()
          affectedEvents.unpersist()

          // Process constraints using the dedicated processor
          val constraintProcessor = new ConstraintProcessor()
          val miningResult = constraintProcessor.processConstraints(
            allConstraints, 
            traceIds.size, 
            config.logName
          )

          println("Constraints mined: " + miningResult.totalConstraints)

          // Generate output using the dedicated writer
          val outputWriter = new JsonOutputWriter()
          val jsonFile = outputWriter.generateFileName(
            config.logName, 
            support, 
            branchingBound, 
            branchingPolicy,  // This is now already normalized or null
            config
          )
          
          outputWriter.writeToFile(miningResult, jsonFile)
          println(s"Results written to: $jsonFile")

          if (!newEvents.isEmpty) {
            metaData.last_declare_mined = events.rdd  //not newEvents; maybe the batch does not follow temporal order
              .map(x => Timestamp.valueOf(x.ts)).reduce((x, y) => { if (x.after(y)) x else y }).toString
            s3Connector.write_metadata(metaData)
          }
        })
      case _ =>
        throw new IllegalArgumentException("Wrong configuration!")
    }

    // Graceful shutdown
    try {
      val spark = SparkSession.getActiveSession
      if (spark.isDefined) {
        spark.get.stop()
      }
    } catch {
      case _: Exception => // Ignore shutdown exceptions
    }

  }

}
