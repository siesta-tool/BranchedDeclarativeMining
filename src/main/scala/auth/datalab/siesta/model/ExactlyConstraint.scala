package auth.datalab.siesta.model

// each activity existed exactly <instances> times in these <traces>
case class ExactlyConstraint(rule: String, event_type:String, instances:Long, traces:Set[String])
