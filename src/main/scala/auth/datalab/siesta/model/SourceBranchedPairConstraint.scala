package auth.datalab.siesta.model

// Source-branched constraint with multiple sources and a single target
case class SourceBranchedPairConstraint(
 rule: String,
 sources: Array[String],
 target: String,
 traces: Set[String]
) extends Serializable
