package auth.datalab.siesta.model

// Target-branched constraint with a single source and multiple targets
case class TargetBranchedPairConstraint(
 rule: String,
 source: String,
 targets: Array[String],
 traces: Set[String]
) extends Serializable
