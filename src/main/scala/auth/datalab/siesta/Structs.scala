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
                    onlyNew: Boolean = true)

  case class PairFull(eventA:String,eventB:String,trace_id:String,positionA:Int,positionB:Int)

  case class Event(eventType:String, ts:String, pos:Int, trace:String)

  case class PositionConstraint(rule: String, eventType: String, traces: Array[String])
  case class PositionConstraintRow(rule: String, eventType: String, trace: String)

  // each activity existed exactly <instances> times in these <traces>
  case class ActivityExactly(event_type:String, instances:Long, traces:Array[String])

  case class ExistenceConstraint(rule:String, event_type:String, n: Int, traces:Array[String])
  case class PairConstraintRow(rule: String, eventA: String, eventB: String, trace: String)
  case class PairConstraint(rule:String, eventA:String, eventB:String, traces:Array[String])


  case class UnorderedHelper(eventA:String,eventB:String, ua:Long, ub:Long, pairs:Long,key:String)

  case class PairConstraintSupported(rule:String, activation:String, target:String, support:Double) extends Serializable

  // Target-branched constraint with a single source and multiple targets
  case class TargetBranchedPairConstraint(
                                       rule: String,
                                       source: String,
                                       targets: Array[String],
                                       support: Double
                                     ) extends Serializable

  // Source-branched constraint with multiple sources and a single target
  case class SourceBranchedPairConstraint(
                                       rule: String,
                                       sources: Array[String],
                                       target: String,
                                       support: Double
                                     ) extends Serializable
}
