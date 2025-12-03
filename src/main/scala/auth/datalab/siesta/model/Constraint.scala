package auth.datalab.siesta.model

// JSON output structures for constraint mining results
case class Constraint(
  source: String,
  target: Option[String],
  instances: Option[Int],
  traces: Seq[String],
  support: Double
)
