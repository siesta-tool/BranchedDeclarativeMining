package auth.datalab.siesta

import org.apache.spark.sql.Dataset
import org.apache.spark.broadcast.Broadcast

object Structs {
  case class MetaData(var traces: Long, var events: Long, var pairs: Long,
                      lookback: Int, var has_previous_stored: Boolean,
                      filename: String, streaming: Boolean,log_name: String, mode: String, compression: String,
                      var start_ts:String, var last_ts:String,
                      var last_declare_mined:String)extends Serializable {
  }


  case class Config(logName: String = "",
                    support: Double = 0,
                    branchingPolicy: String = null,
                    branchingType: String = "TARGET",
                    branchingBound: Int = 0,
                    dropFactor: Double = 1.5,
                    filterRare: Boolean = false,
                    filterUnderBound: Boolean = false,
                    hardRediscovery: Boolean = false,
                    quickMining:Boolean = false,
                    outputPath: String = "./output") {
    
    /**
     * Determines if branching is enabled based on policy
     * @return true if branching should be applied
     */
    def isBranchingEnabled: Boolean = branchingPolicy != null && branchingPolicy.trim.nonEmpty
    
    /**
     * Gets the effective branching type, defaulting to TARGET if policy is enabled
     * @return the branching type to use, or null if branching is disabled
     */
    def getEffectiveBranchingType: String = {
      if (isBranchingEnabled) {
        if (branchingType == null || branchingType.trim.isEmpty) "TARGET" else branchingType.toUpperCase
      } else {
        null
      }
    }
    
    /**
     * Gets the normalized branching policy
     * @return the branching policy in uppercase, or null if disabled
     */
    def getEffectiveBranchingPolicy: String = {
      if (isBranchingEnabled) branchingPolicy.toUpperCase else null
    }
  }

  case class PairFull(source:String,target:String,trace_id:String,positionA:Int,positionB:Int)

  case class Event(event_type:String, timestamp:String, pos:Int, trace_id:String)

  case class PositionConstraint(rule: String, event_type: String, traces: Set[String])
  case class PositionConstraintRow(rule: String, event_type: String, trace_id: String)

  case class ExChoiceRecord(trace_id: String, source: String, target: String, found: Int)
  case class CoExistenceRecord(trace_id: String, source: String, target: String)

  // each activity existed exactly <instances> times in these <traces>
  case class ExactlyConstraint(rule: String, event_type:String, instances:Long, traces:Set[String])
  case class ExactlyConstraintRow(rule:String, event_type:String, instances:Long, trace_id:String)

  case class PairConstraintRow(rule: String, source: String, target: String, trace_id: String)
  case class PairConstraint(rule:String, source:String, target:String, traces:Set[String])

  case class TraceStats( totalCount: Map[String, Int], adjacentCount: Map[(String, String), Int])

  // Target-branched constraint with a single source and multiple targets
  case class TargetBranchedPairConstraint(
   rule: String,
   source: String,
   targets: Array[String],
   traces: Set[String]
  ) extends Serializable

  // Source-branched constraint with multiple sources and a single target
  case class SourceBranchedPairConstraint(
   rule: String,
   sources: Array[String],
   target: String,
   traces: Set[String]
  ) extends Serializable

  case class FullBranchedPairConstraint(
    rule: String,
    sources: Array[String],
    targets: Array[String],
    traces: Set[String]
  ) extends Serializable

  // JSON output structures for constraint mining results
  case class Constraint(
    source: String,
    target: Option[String],
    instances: Option[Int],
    traces: Seq[String],
    support: Double
  )

  case class ConstraintGroup(
    rule: String,
    constraints: Seq[Constraint]
  )

  case class MiningResult(
    logName: String,
    totalConstraints: Int,
    constraintGroups: Seq[ConstraintGroup]
  )

  /**
   * Mining context that encapsulates common parameters for constraint extraction
   */
  case class MiningContext(
    metaData: MetaData,
    affectedEvents: Dataset[Event],
    bEvolvedTracesBounds: Broadcast[scala.collection.Map[String, (Int, Int)]],
    bTraceIds: Broadcast[Set[String]],
    newEvents: Dataset[Event],
    allEventTypes: Set[String],
    totalTraces: Long
  )

  case class PairConstraintBits(
  rule: String,
  source: String,
  target: String,
  tracesBits: Array[Int]
  ) extends Serializable
}
