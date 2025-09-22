package auth.datalab.siesta


import auth.datalab.siesta.Structs.{PairConstraint}
import org.apache.spark.sql.{Dataset}

object BranchingResolver {

  def branchMine(
      policy: String,
      constraints: Dataset[PairConstraint],
      minSupport: Double,
      maxTargets: Int,
      swap: Boolean = false,
      dropFactor: Option[Double] = None,
      isUnary: Option[Boolean] = Some(false)
    ): Dataset[PairConstraint] = {
    
    policy.toLowerCase match {
      case "and" => AndBranchingMiner.andMine(constraints, minSupport, maxTargets, swap, dropFactor, isUnary)
      case "xor" => XORBranchingMiner.xorMine(constraints, minSupport, maxTargets, swap, dropFactor, isUnary)
      case "or" => OrBranchingMiner.orMine(constraints, minSupport, maxTargets, swap, dropFactor, isUnary)
      case _ => throw new IllegalArgumentException(s"Unknown branching policy: $policy")
    }
  }
}
