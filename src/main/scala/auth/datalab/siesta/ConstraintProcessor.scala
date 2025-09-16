package auth.datalab.siesta

import auth.datalab.siesta.Structs.{Constraint, ConstraintGroup, MiningResult}
import scala.collection.mutable.ListBuffer

/**
 * Processes raw constraint data and converts it to structured format
 */
class ConstraintProcessor {

  /**
   * Processes raw constraints and groups them by rule type
   * 
   * @param rawConstraints Raw constraints in format (rule, info, traces)
   * @param totalTraces Total number of traces for support calculation
   * @param logName The log name for the mining result
   * @return Structured mining result with grouped constraints
   */
  def processConstraints(
    rawConstraints: TraversableOnce[(String, String, Set[String])],
    totalTraces: Int,
    logName: String
  ): MiningResult = {
    
    val constraintsByRule = scala.collection.mutable.Map[String, ListBuffer[Constraint]]()
    var totalConstraintCount = 0
    
    rawConstraints.foreach { case (rule, info, traces) =>
      totalConstraintCount += 1
      val supportValue = traces.size.toDouble / totalTraces
      val constraint = parseConstraintInfo(info, traces.toSeq, supportValue)
      constraintsByRule.getOrElseUpdate(rule, ListBuffer[Constraint]()) += constraint
    }

    // Create constraint groups sorted by rule name
    val constraintGroups = constraintsByRule.map { case (rule, constraints) =>
      ConstraintGroup(rule, constraints.toSeq)
    }.toSeq.sortBy(_.rule)

    MiningResult(
      logName = logName,
      totalConstraints = totalConstraintCount,
      constraintGroups = constraintGroups
    )
  }

  /**
   * Parses constraint info to determine constraint type and extract components
   * 
   * @param info The constraint info string (e.g., "A|1", "A|B", "A")
   * @param traces The supporting traces
   * @param support The calculated support value
   * @return Parsed Constraint object
   */
  private def parseConstraintInfo(info: String, traces: Seq[String], support: Double): Constraint = {
    if (info.contains("|")) {
      val parts = info.split("\\|")
      if (parts.length == 2) {
        // Check if second part is a number (unary constraint) or another activity (binary constraint)
        if (parts(1).forall(c => c.isDigit || c == '.')) {
          // Unary constraint: activity|number
          Constraint(
            source = parts(0),
            target = None,
            instances = Some(parts(1).toInt),
            traces = traces,
            support = support
          )
        } else {
          // Binary constraint: source|target
          Constraint(
            source = parts(0),
            target = Some(parts(1)),
            instances = None,
            traces = traces,
            support = support
          )
        }
      } else {
        // Fallback for unexpected format
        Constraint(
          source = info,
          target = None,
          instances = None,
          traces = traces,
          support = support
        )
      }
    } else {
      // Simple constraint: just activity
      Constraint(
        source = info,
        target = None,
        instances = None,
        traces = traces,
        support = support
      )
    }
  }
}