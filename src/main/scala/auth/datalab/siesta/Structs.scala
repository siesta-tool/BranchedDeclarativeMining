package auth.datalab.siesta

object Structs {
  case class MetaData(var traces: Long, var events: Long, var pairs: Long,
                      lookback: Int,
                      var has_previous_stored: Boolean,
                      filename: String, log_name: String, mode: String, compression: String,
                      var last_declare_mined:String) extends Serializable {
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
                    quickMining:Boolean = false)

  case class PairFull(eventA:String,eventB:String,trace_id:String,positionA:Int,positionB:Int)

  case class Event(eventType:String, ts:String, pos:Int, trace:String)

  case class PositionConstraint(rule: String, eventType: String, traces: Array[String])
  case class PositionConstraintRow(rule: String, eventType: String, trace: String)

  // each activity existed exactly <instances> times in these <traces>
  case class ExactlyConstraint(rule: String, eventType:String, instances:Long, traces:Array[String])
  case class ExactlyConstraintRow(rule:String, eventType:String, instances:Long, trace:String)

  case class PairConstraintRow(rule: String, eventA: String, eventB: String, trace: String)
  case class PairConstraint(rule:String, eventA:String, eventB:String, traces:Array[String])

  case class UnorderedHelper(eventA:String,eventB:String, ua:Long, ub:Long, pairs:Long,key:String)


  case class ExChoiceRecord(traceId: String, eventA: String, eventB: String, found: Int)
  case class CoExistenceRecord(traceId: String, eventA: String, eventB: String)

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
}