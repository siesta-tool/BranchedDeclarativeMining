package auth.datalab.siesta.model

/**
 * Enumeration for branching types used in constraint mining
 */
sealed trait BranchingType {
  def name: String
}

object BranchingType {
  case object SOURCE extends BranchingType { val name = "SOURCE" }
  case object TARGET extends BranchingType { val name = "TARGET" }
  
  def fromString(str: String): BranchingType = str match {
    case null | "" => TARGET // Default to TARGET
    case s if s.trim.equalsIgnoreCase("SOURCE") => SOURCE
    case s if s.trim.equalsIgnoreCase("TARGET") => TARGET
    case _ => TARGET // Default to TARGET for invalid values
  }
  
  def values: List[BranchingType] = List(SOURCE, TARGET)
}
