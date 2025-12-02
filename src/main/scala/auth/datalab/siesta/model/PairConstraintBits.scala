package auth.datalab.siesta.model

case class PairConstraintBits(
  rule: String,
  source: String,
  target: String,
  tracesBits: Array[Int]
) extends Serializable
