package auth.datalab.siesta

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
                    quickMining:Boolean = false) {
    
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

  case class PairFull(eventA:String,eventB:String,trace_id:String,positionA:Int,positionB:Int)

  case class Event(eventType:String, ts:String, pos:Int, trace:String)

  case class PositionConstraint(rule: String, eventType: String, traces: Array[String])
  case class PositionConstraintRow(rule: String, eventType: String, trace: String)

  case class ExChoiceRecord(trace_id: String, ev_a: String, ev_b: String, found: Int)
  case class CoExistenceRecord(trace_id: String, ev_a: String, ev_b: String)

  // each activity existed exactly <instances> times in these <traces>
  case class ExactlyConstraint(rule: String, eventType:String, instances:Long, traces:Array[String])
  case class ExactlyConstraintRow(rule:String, eventType:String, instances:Long, trace:String)

  case class PairConstraintRow(rule: String, eventA: String, eventB: String, trace: String)
  case class PairConstraint(rule:String, eventA:String, eventB:String, traces:Array[String])

  case class TraceStats( totalCount: Map[String, Int], adjacentCount: Map[(String, String), Int])

  // Target-branched constraint with a single source and multiple targets
  case class TargetBranchedPairConstraint(
   rule: String,
   source: String,
   targets: Array[String],
   traces: Array[String]
  ) extends Serializable

  // Source-branched constraint with multiple sources and a single target
  case class SourceBranchedPairConstraint(
   rule: String,
   sources: Array[String],
   target: String,
   traces: Array[String]
  ) extends Serializable

  case class FullBranchedPairConstraint(
    rule: String,
    sources: Array[String],
    targets: Array[String],
    traces: Array[String]
  ) extends Serializable

  // JSON output structures for constraint mining results
  case class Constraint(
    source: String,
    target: Option[String],
    number: Option[Int],
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
}
