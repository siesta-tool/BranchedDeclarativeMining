package auth.datalab.siesta.model

import org.apache.spark.sql.Dataset
import org.apache.spark.broadcast.Broadcast

object Structs {
  
  /**
   * Enumeration for branching policies used in constraint mining
   */
  sealed trait BranchingPolicy {
    def name: String
  }
  
  object BranchingPolicy {
    case object AND extends BranchingPolicy { val name = "AND" }
    case object OR extends BranchingPolicy { val name = "OR" }
    case object XOR extends BranchingPolicy { val name = "XOR" }
    
    def fromString(str: String): Option[BranchingPolicy] = str match {
      case null | "" => None
      case s if s.trim.equalsIgnoreCase("AND") => Some(AND)
      case s if s.trim.equalsIgnoreCase("OR") => Some(OR)
      case s if s.trim.equalsIgnoreCase("XOR") => Some(XOR)
      case s if s.trim.equalsIgnoreCase("NONE") => None
      case _ => None
    }
    
    def values: List[BranchingPolicy] = List(AND, OR, XOR)
  }
  
  /**
   * Enumeration for branching types used in constraint mining
   */
  sealed trait BranchingType {
    def name: String
  }
  
  object BranchingType {
    case object SOURCE extends BranchingType { val name = "SOURCE" }
    case object TARGET extends BranchingType { val name = "TARGET" }
    
    def fromString(str: String): BranchingType = str match {
      case null | "" => TARGET // Default to TARGET
      case s if s.trim.equalsIgnoreCase("SOURCE") => SOURCE
      case s if s.trim.equalsIgnoreCase("TARGET") => TARGET
      case _ => TARGET // Default to TARGET for invalid values
    }
    
    def values: List[BranchingType] = List(SOURCE, TARGET)
  }

  case class MetaData(var traces: Long, var events: Long, var pairs: Long,
                      lookback: Int, var has_previous_stored: Boolean,
                      filename: String, streaming: Boolean,log_name: String, mode: String, compression: String,
                      var start_ts:String, var last_ts:String,
                      var last_declare_mined:String)extends Serializable {
  }


  case class Config(logName: String = "",
                    support: Double = 0,
                    branchingPolicy: Option[BranchingPolicy] = None,
                    branchingType: Option[BranchingType] = None,
                    branchingBound: Int = 0,
                    dropFactor: Option[Double] = None,
                    filterRare: Boolean = false,
                    filterUnderBound: Boolean = false,
                    hardRediscovery: Boolean = false,
                    quickMining:Boolean = false,
                    outputPath: String = "./output") {
    
    /**
     * Determines if branching is enabled based on policy and type being defined
     * @return true if branching should be applied
     */
    def isBranchingEnabled: Boolean = branchingPolicy.isDefined && branchingType.isDefined
    
    /**
     * Gets the effective branching type if branching is enabled
     * @return the branching type to use, or None if branching is disabled
     */
    def getEffectiveBranchingType: Option[BranchingType] = {
      if (isBranchingEnabled) branchingType else None
    }
    
    /**
     * Gets the branching type name as string (for backward compatibility)
     * @return the branching type name, or null if branching is disabled
     */
    def getBranchingTypeName: String = {
      if (isBranchingEnabled) branchingType.get.name else null
    }
    
    /**
     * Gets the normalized branching policy
     * @return the branching policy name, or null if disabled
     */
    def getEffectiveBranchingPolicy: Option[BranchingPolicy] = {
      if (isBranchingEnabled) branchingPolicy else None
    }
    
    /**
     * Gets the branching policy name as string (for backward compatibility)
     * @return the branching policy name in uppercase, or null if disabled
     */
    def getBranchingPolicyName: String = {
      branchingPolicy.map(_.name).orNull
    }
    
    /**
     * Gets the effective branching bound when branching is enabled.
     * If branchingBound is 0 (default/not specified), returns Int.MaxValue for unbounded mining.
     * Otherwise returns the specified bound.
     * @return the effective branching bound
     */
    def getEffectiveBranchingBound: Int = {
      if (branchingBound == 0) Int.MaxValue else branchingBound
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
