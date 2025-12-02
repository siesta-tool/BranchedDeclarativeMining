package auth.datalab.siesta.model

case class TraceStats( totalCount: Map[String, Int], adjacentCount: Map[(String, String), Int])
