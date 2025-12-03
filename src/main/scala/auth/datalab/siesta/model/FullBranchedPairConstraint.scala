package auth.datalab.siesta.model

case class FullBranchedPairConstraint(
  rule: String,
  sources: Array[String],
  targets: Array[String],
  traces: Set[String]
) extends Serializable
