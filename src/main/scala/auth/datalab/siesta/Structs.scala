package auth.datalab.siesta

object Structs {
  case class MetaData(var traces: Long, var events: Long, var pairs: Long,
                      lookback: Int,
                      var has_previous_stored: Boolean,
                      filename: String, log_name: String, mode: String, compression: String,
                      var last_declare_mined:String) extends Serializable {
  }

  case class PairFull(eventA:String,eventB:String,trace_id:String,positionA:Int,positionB:Int)

  case class Event(event_type:String, ts:String, pos:Int, trace_id:String)

  case class PositionConstraint(rule:String, event_type:String, traces:Array[String])

  // each activity existed exactly <instances> times in these <traces>
  case class ActivityExactly(event_type:String, instances:Long, traces:Array[String])
  case class ExistenceConstraint(rule:String, event_type:String, n: Int, traces:Array[String])
  case class PairConstraint(rule:String, eventA:String, eventB:String, traces:Array[String])

  case class UnorderedHelper(eventA:String,eventB:String, ua:Long, ub:Long, pairs:Long,key:String)

  case class BranchedSingleConstraint(rule:String, prefix:String, suffix:String, support:Double)
  case class BranchedPairConstraint(rule:String, prefix:String, suffix:Array[(String,Double)])
}
