package auth.datalab.siesta.model

case class Config(logName: String = "",
                  support: Double = 0,
                  branchingPolicy: Option[BranchingPolicy] = None,
                  branchingType: Option[BranchingType] = None,
                  branchingBound: Int = 0,
                  dropFactor: Option[Double] = None,
                  filterRare: Boolean = false,
                  filterUnderBound: Boolean = false,
                  hardRediscovery: Boolean = false,
                  quickMining:Boolean = false,
                  outputPath: String = "./output") {
  
  /**
   * Determines if branching is enabled based on policy and type being defined
   * @return true if branching should be applied
   */
  def isBranchingEnabled: Boolean = branchingPolicy.isDefined && branchingType.isDefined
  
  /**
   * Gets the effective branching type if branching is enabled
   * @return the branching type to use, or None if branching is disabled
   */
  def getEffectiveBranchingType: Option[BranchingType] = {
    if (isBranchingEnabled) branchingType else None
  }
  
  /**
   * Gets the branching type name as string (for backward compatibility)
   * @return the branching type name, or null if branching is disabled
   */
  def getBranchingTypeName: String = {
    if (isBranchingEnabled) branchingType.get.name else null
  }
  
  /**
   * Gets the normalized branching policy
   * @return the branching policy name, or null if disabled
   */
  def getEffectiveBranchingPolicy: Option[BranchingPolicy] = {
    if (isBranchingEnabled) branchingPolicy else None
  }
  
  /**
   * Gets the branching policy name as string (for backward compatibility)
   * @return the branching policy name in uppercase, or null if disabled
   */
  def getBranchingPolicyName: String = {
    branchingPolicy.map(_.name).orNull
  }
  
  /**
   * Gets the effective branching bound when branching is enabled.
   * If branchingBound is 0 (default/not specified), returns Int.MaxValue for unbounded mining.
   * Otherwise returns the specified bound.
   * @return the effective branching bound
   */
  def getEffectiveBranchingBound: Int = {
    if (branchingBound == 0) Int.MaxValue else branchingBound
  }
}
