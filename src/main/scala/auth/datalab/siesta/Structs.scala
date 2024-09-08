package auth.datalab.siesta

import java.sql.Timestamp

object Structs {

  case class MetaData(var traces: Long, var events: Long, var pairs: Long,
                      lookback: Int, split_every_days: Int,
                      var last_interval: String, var has_previous_stored: Boolean,
                      filename: String, log_name: String, mode: String, compression: String,
                      var last_checked_split:Int, var last_declare_mined:String) extends Serializable {
  }

  case class PairFull(eventA:String,eventB:String,trace_id:String,positionA:Int,positionB:Int)

  case class Event(event_type:String, ts:String, pos:Int, trace_id:String)

  case class PositionConstraint(rule:String, event_type:String, occurrences:Double)
  //each activity in how many traces it is contained exactly
  case class ActivityExactly(event_type:String, occurrences: Int, contained:Long)
  case class ExistenceConstraint(rule:String, event_type:String, n: Int, occurrences:Double)
  case class PairConstraint(rule:String, eventA:String, eventB:String, occurrences:Double)

  case class UnorderedHelper(eventA:String,eventB:String, ua:Long, ub:Long, pairs:Long,key:String)

}
