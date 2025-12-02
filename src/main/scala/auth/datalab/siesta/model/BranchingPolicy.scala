package auth.datalab.siesta.model

/**
 * Enumeration for branching policies used in constraint mining
 */
sealed trait BranchingPolicy {
  def name: String
}

object BranchingPolicy {
  case object AND extends BranchingPolicy { val name = "AND" }
  case object OR extends BranchingPolicy { val name = "OR" }
  case object XOR extends BranchingPolicy { val name = "XOR" }
  
  def fromString(str: String): Option[BranchingPolicy] = str match {
    case null | "" => None
    case s if s.trim.equalsIgnoreCase("AND") => Some(AND)
    case s if s.trim.equalsIgnoreCase("OR") => Some(OR)
    case s if s.trim.equalsIgnoreCase("XOR") => Some(XOR)
    case s if s.trim.equalsIgnoreCase("NONE") => None
    case _ => None
  }
  
  def values: List[BranchingPolicy] = List(AND, OR, XOR)
}
