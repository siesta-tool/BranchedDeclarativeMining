package auth.datalab.siesta.model

case class MiningResult(
  logName: String,
  totalConstraints: Int,
  constraintGroups: Seq[ConstraintGroup]
)
