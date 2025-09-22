package auth.datalab.siesta.mining


import auth.datalab.siesta.model.Structs.{PairConstraint, BranchingPolicy}
import org.apache.spark.sql.{Dataset}

object BranchingResolver {

  def branchMine(
      policy: BranchingPolicy,
      constraints: Dataset[PairConstraint],
      minSupport: Double,
      maxTargets: Int,
      swap: Boolean = false,
      dropFactor: Option[Double] = None,
      isUnary: Option[Boolean] = Some(false)
    ): Dataset[PairConstraint] = {
    
    policy match {
      case BranchingPolicy.AND => AndBranchingMiner.andMine(constraints, minSupport, maxTargets, swap, dropFactor, isUnary)
      case BranchingPolicy.XOR => XORBranchingMiner.xorMine(constraints, minSupport, maxTargets, swap, dropFactor, isUnary)
      case BranchingPolicy.OR => OrBranchingMiner.orMine(constraints, minSupport, maxTargets, swap, dropFactor, isUnary)
    }
  }
}
