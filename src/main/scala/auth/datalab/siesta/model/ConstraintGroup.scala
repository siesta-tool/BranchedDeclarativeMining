package auth.datalab.siesta.model

case class ConstraintGroup(
  rule: String,
  constraints: Seq[Constraint]
)
