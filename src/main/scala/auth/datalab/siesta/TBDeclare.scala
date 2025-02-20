package auth.datalab.siesta

import auth.datalab.siesta.Structs.{Constraint, PairConstraint, PositionConstraint, TargetBranchedConstraint}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, collect_list, collect_set, countDistinct, explode, struct}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}



object TBDeclare {

  //////////////////////////////////////////////////////////////////////
  //               Policy-based constraints extraction                //
  //////////////////////////////////////////////////////////////////////

  def getBranchedConstraints(constraints: Dataset[PairConstraint],
                             totalTraces: Long,
                             threshold: Double = 0,
                             branchingBound: Int = 1,
                             branchingType: String = "OR",
                             dropFactor: Double = 2.5,
                             filterRare: Option[Boolean] = Some(false)): Dataset[TargetBranchedConstraint] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val pairConstraintCounts: DataFrame = explodePairConstraints(constraints)

    // Group by (rule, activationEvent), calculate target sets
    val groupedConstraints = pairConstraintCounts
      .as[(String, String, String, Seq[String])]
      .groupByKey { case (rule, activationEvent, _, _) => (rule, activationEvent) }
      .mapGroups { case ((rule, activationEvent), rows) =>

        // Create a list of target events and their corresponding trace sets
        val targetEventData = rows.toSeq.map { case (_, _, targetEvent, traceSet) =>
          (targetEvent, traceSet.toSet)
        }

        // Compute frequent traces for "AND" and "XOR" branching if needed
        val frequentTraces = if (filterRare.getOrElse(false) && branchingType != "OR")
          targetEventData.flatMap(_._2).groupBy(identity)
            .mapValues(_.size)
            .filter(_._2 == targetEventData.flatMap(_._2).groupBy(identity).mapValues(_.size).values.max)
            .keys.toSet
        else
          Set.empty[String]

        // Choose the appropriate find method based on branchingType
        val targetResult = (branchingType, branchingBound > 1) match {
          case ("OR", true)  => findORTargetsBounded(targetEventData, branchingBound)
          case ("OR", false) => findORTargetsUnbounded(targetEventData, threshold, dropFactor)
          case ("AND", true) => findANDTargetsBounded(targetEventData, frequentTraces, threshold * totalTraces, branchingBound)
          case ("AND", false) => findANDTargetsUnbounded(targetEventData, frequentTraces, threshold * totalTraces)
          case ("XOR", true) => findXORTargetsBounded(targetEventData, frequentTraces, threshold * totalTraces, branchingBound)
          case ("XOR", false) => findXORTargetsUnbounded(targetEventData, frequentTraces, threshold * totalTraces)
          case _ => throw new IllegalArgumentException(s"Unsupported branching type: $branchingType")
        }

        // Extract target events and their associated traces
        val targetEvents = targetResult.flatMap(_._1) // Extract target event names
        val targetTraces = targetResult.flatMap(_._2) // Extract associated trace sets

        // Calculate the total number of unique traces for the targets
        val totalUniqueTraces = targetTraces.distinct.size.toDouble

        // Create a TargetBranchedConstraint for the rule and its activation event
        TargetBranchedConstraint(
          rule = rule,
          activation = activationEvent,
          targets = targetEvents.toArray,
          support = totalUniqueTraces / totalTraces
        )
      }

    println(s"$branchingType # = " + groupedConstraints.filter(_.support >= threshold).count())
    groupedConstraints.filter(_.support >= threshold)
  }


  def getORBranchedConstraints(constraints: Dataset[PairConstraint],
                               totalTraces: Long,
                               threshold: Double = 0,
                               branchingBound: Int = 1,
                               dropFactor: Double = 2.5): Dataset[TargetBranchedConstraint] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val pairConstraintCounts: DataFrame = explodePairConstraints(constraints)

    // Group by (rule, activationEvent), calculate target sets
    val groupedConstraints = pairConstraintCounts
      .as[(String, String, String, Seq[String])]
      .groupByKey { case (rule, activationEvent, _, _) => (rule, activationEvent) }
      .mapGroups { case ((rule, activationEvent), rows) =>

        // Create a list of target events (eventB) and their corresponding trace sets
        val targetEventData = rows.toSeq.map { case (_, _, targetEvent, traceSet) =>
          (targetEvent, traceSet.toSet) // Ensure trace sets are unique
        }

        // Find the target events within the defined branching bound or threshold
        val targetResult = if (branchingBound > 1)
          findORTargetsBounded(targetEventData, branchingBound)
        else
          findORTargetsUnbounded(targetEventData, threshold, dropFactor)

        // Extract target events and their associated traces
        val targetEvents = targetResult.flatMap(_._1) // Extract target event names
        val targetTraces = targetResult.flatMap(_._2) // Extract associated trace sets

        // Calculate the total number of unique traces for the targets
        val totalUniqueTraces = targetTraces.distinct.size.toDouble

        // Create a TargetBranchedConstraint for the rule and its activation event
        TargetBranchedConstraint(
          rule = rule,
          activation = activationEvent,
          targets = targetEvents.toArray,
          support = totalUniqueTraces / totalTraces
        )
      }

    println("# = " + groupedConstraints.filter(_.support >= threshold).count())
    groupedConstraints.filter(_.support >= threshold)
  }

  def getANDBranchedConstraints(constraints: Dataset[PairConstraint],
                                totalTraces: Long,
                                threshold: Double = 0,
                                branchingBound: Int = 1,
                                filterRare: Option[Boolean] = Some(false)): Dataset[TargetBranchedConstraint] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val pairConstraintCounts: DataFrame = explodePairConstraints(constraints)

    // Group by (rule, activationEvent), calculate target sets
    val groupedConstraints = pairConstraintCounts
      .as[(String, String, String, Seq[String])]
      .groupByKey { case (rule, activationEvent, _, _) => (rule, activationEvent) }
      .mapGroups { case ((rule, activationEvent), rows) =>

        // Create a list of target events (eventB) and their corresponding trace sets
        val targetEventData = rows.toSeq.map { case (_, _, targetEvent, traceSet) =>
          (targetEvent, traceSet.toSet)
        }

        // Compute frequent traces for target events
        val frequentTraces = if (filterRare.get)
          targetEventData.flatMap(_._2).groupBy(identity)
          .mapValues(_.size)
          .filter(_._2 == targetEventData.flatMap(_._2).groupBy(identity).mapValues(_.size).values.max)
          .keys.toSet
        else
          Set.empty[String]

        // Find the target events within the defined branching bound or threshold
        val targetResult = if (branchingBound > 1)
          findANDTargetsBounded(targetEventData, frequentTraces, threshold * totalTraces, branchingBound)
        else
          findANDTargetsUnbounded(targetEventData, frequentTraces, threshold * totalTraces)

        // Extract target events and their associated traces
        val targetEvents = targetResult.flatMap(_._1) // Extract target event names
        val targetTraces = targetResult.flatMap(_._2) // Extract associated trace sets

        // Calculate the total number of unique traces for the targets
        val totalUniqueTraces = targetTraces.distinct.size.toDouble

        // Create a TargetBranchedConstraint for the rule and its activation event
        TargetBranchedConstraint(
          rule = rule,
          activation = activationEvent,
          targets = targetEvents.toArray,
          support = totalUniqueTraces / totalTraces
        )
      }

    println("# = " + groupedConstraints.filter(_.support >= threshold).count())
    groupedConstraints.filter(_.support >= threshold)
  }

  def getXORBranchedConstraints(constraints: Dataset[PairConstraint],
                                totalTraces: Long,
                                threshold: Double = 0,
                                branchingBound: Int = 1,
                                filterRare: Option[Boolean] = Some(false)): Dataset[TargetBranchedConstraint] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val pairConstraintCounts: DataFrame = explodePairConstraints(constraints)

    // Group by (rule, activationEvent), calculate target sets
    val groupedConstraints = pairConstraintCounts
      .as[(String, String, String, Seq[String])]
      .groupByKey { case (rule, activationEvent, _, _) => (rule, activationEvent) }
      .mapGroups { case ((rule, activationEvent), rows) =>

        // Create a list of target events (eventB) and their corresponding trace sets
        val targetEventData = rows.toSeq.map { case (_, _, targetEvent, traceSet) =>
          (targetEvent, traceSet.toSet)
        }

        // Compute frequent traces for target events
        val frequentTraces = if (filterRare.get)
          targetEventData.flatMap(_._2).groupBy(identity)
            .mapValues(_.size)
            .filter(_._2 == targetEventData.flatMap(_._2).groupBy(identity).mapValues(_.size).values.max)
            .keys.toSet
        else
          Set.empty[String]

        // Find the target events within the defined branching bound or threshold
        val targetResult = if (branchingBound > 1)
          findXORTargetsBounded(targetEventData, frequentTraces, threshold * totalTraces, branchingBound)
        else
          findXORTargetsUnbounded(targetEventData, frequentTraces, threshold * totalTraces)

        // Extract target events and their associated traces
        val targetEvents = targetResult.flatMap(_._1) // Extract target event names
        val targetTraces = targetResult.flatMap(_._2) // Extract associated trace sets

        // Calculate the total number of unique traces for the targets
        val totalUniqueTraces = targetTraces.distinct.size.toDouble

        // Create a TargetBranchedConstraint for the rule and its activation event
        TargetBranchedConstraint(
          rule = rule,
          activation = activationEvent,
          targets = targetEvents.toArray,
          support = totalUniqueTraces / totalTraces
        )
      }

    println("XOR # = " + groupedConstraints.filter(_.support >= threshold).count())
    groupedConstraints.filter(_.support >= threshold)
  }

  //////////////////////////////////////////////////////////////////////
  //                Optimal target extraction and helpers             //
  //////////////////////////////////////////////////////////////////////

  private def explodePairConstraints(constraints: Dataset[PairConstraint]): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Explode traces to create individual rows for (rule, eventA, eventB, trace)
    val exploded = constraints
      .withColumn("trace", explode($"traces"))
      .select($"rule", $"eventA", $"eventB", $"trace")

    // Compute unique traces for each (rule, eventA, eventB)
    val traceCounts = exploded
      .groupBy($"rule", $"eventA", $"eventB")
      .agg(collect_set($"trace").as("uniqueTraces"))
    traceCounts
  }


  private def findORTargetsBounded(targets: Seq[(String, Set[String])],
                                   branchingBound: Int = 1): Seq[(Set[String], Set[String])] = {
    if (targets.isEmpty) return Seq.empty

    var currentTargetsWithTraces = targets.toMap
    var currentTraceCoverage = currentTargetsWithTraces.values.flatten.toSet

    while (currentTargetsWithTraces.size > branchingBound && currentTargetsWithTraces.nonEmpty) {
      // Evaluate the contribution of each target
      val targetContributions = currentTargetsWithTraces.map { case (target, traces) =>
        val contribution = (currentTraceCoverage -- traces).size
        (target, contribution)
      }

      // Find the target with the smallest contribution
      val (minTarget, _) = targetContributions.minBy(_._2)

      // Remove the target and update the trace coverage
      currentTargetsWithTraces -= minTarget
      currentTraceCoverage = currentTargetsWithTraces.values.flatten.toSet
    }

    Seq((currentTargetsWithTraces.keySet, currentTraceCoverage))
  }


  private def findORTargetsUnbounded(targets: Seq[(String, Set[String])],
                                     threshold: Double = 0.0,
                                     dropFactor: Double = 2.5): Seq[(Set[String], Set[String])] = {
    if (targets.isEmpty) return Seq.empty

    var currentTargetsWithTraces = targets.toMap
    var currentTraceCoverage = currentTargetsWithTraces.values.flatten.toSet

    var sumOfReductions = 0.0
    var sumOfReductions2 = 0.0
    var countOfReductions = 0
    var reductionStd = 0.0

    var lastValidTargets = currentTargetsWithTraces.keySet
    var lastValidCoverage = currentTraceCoverage

    while (currentTargetsWithTraces.nonEmpty && currentTargetsWithTraces.size > 1) {
      // Evaluate the contribution of each target
      val targetContributions = currentTargetsWithTraces.map { case (target, traces) =>
        val contribution = (currentTraceCoverage -- traces).size
        (target, contribution)
      }

      // Find the target with the smallest contribution
      val (minTarget, minContribution) = targetContributions.minBy(_._2)

      // Update sum and squared sum for average and std deviation of the reduction rate
      sumOfReductions += minContribution
      sumOfReductions2 += Math.pow(minContribution, 2)
      countOfReductions += 1
      val reductionMean = sumOfReductions / countOfReductions
      reductionStd = Math.sqrt((sumOfReductions2 / countOfReductions) - Math.pow(reductionMean, 2))


      // Stop if the threshold condition is met
      if (threshold > 0 && currentTraceCoverage.size.toDouble <= threshold) {
        return Seq((lastValidTargets, lastValidCoverage))
      }

      // Check if the reduction is a major drop
      if (Math.abs(minContribution - reductionMean) > dropFactor * reductionStd) {
        return Seq((lastValidTargets, lastValidCoverage))
      } else {
        lastValidTargets = currentTargetsWithTraces.keySet
        lastValidCoverage = currentTraceCoverage
      }

      // Remove the target and update the trace coverage
      currentTargetsWithTraces -= minTarget
      currentTraceCoverage = currentTargetsWithTraces.values.flatten.toSet
    }

    Seq((lastValidTargets, lastValidCoverage))
  }


  private def findANDTargetsBounded(targets: Seq[(String, Set[String])],
                                    frequentTraces: Set[String],
                                    threshold: Double = 0,
                                    branchingBound: Int = 1): Seq[(Set[String], Set[String])] = {
    if (targets.isEmpty) return Seq.empty

    val filteredTargets = if (frequentTraces.nonEmpty) {
      targets.filter { case (_, traceSet) => traceSet.exists(frequentTraces.contains) }
    } else targets

    if (filteredTargets.size == 1)
      return Seq((Set(filteredTargets.head._1), filteredTargets.head._2))

    if (filteredTargets.isEmpty) return Seq.empty

    var candidateSets = filteredTargets.combinations(2).map { pair =>
      val combinedSet = pair.map(_._1).toSet
      val combinedCoverage = pair.map(_._2).reduce(_ intersect _)
      (combinedSet, combinedCoverage)
    }.toSeq

    if (candidateSets.isEmpty) return Seq.empty

    var lastValidTargets = candidateSets.head._1
    var lastValidCoverage = candidateSets.head._2

    var currentSetSize = 2

    while (candidateSets.nonEmpty && currentSetSize < branchingBound) {
      // Sort candidates by trace coverage and keep the top-1
      val topCandidate = candidateSets.maxBy(_._2.size)
      lastValidTargets = topCandidate._1
      lastValidCoverage = topCandidate._2

      // Check if the threshold is met
      val currentSupport = lastValidCoverage.size.toDouble
      if (currentSupport > 0 && threshold > 0 && currentSupport <= threshold) {
        return Seq((lastValidTargets, lastValidCoverage))
      }

      // Generate new candidates by increasing set size by 1
      candidateSets = candidateSets.flatMap { case (currentSet, currentCoverage) =>
        targets.filterNot(t => currentSet.contains(t._1)).map { t =>
          val newSet = currentSet + t._1
          val newCoverage = currentCoverage intersect t._2
          (newSet, newCoverage)
        }
      }

      candidateSets = candidateSets.filter(_._2.nonEmpty)
      currentSetSize += 1
    }

    Seq((lastValidTargets, lastValidCoverage))
  }


  private def findANDTargetsUnbounded(targets: Seq[(String, Set[String])],
                                      frequentTraces: Set[String],
                                      threshold: Double = 0,
                                      dropThreshold: Double = 0.5): Seq[(Set[String], Set[String])] = {
    if (targets.isEmpty) return Seq.empty

    val filteredTargets = if (frequentTraces.nonEmpty) {
      targets.filter { case (_, traceSet) => traceSet.exists(frequentTraces.contains) }
    } else targets

    if (filteredTargets.size == 1)
      return Seq((Set(filteredTargets.head._1), filteredTargets.head._2))

    var candidateSets = filteredTargets.combinations(2).map { pair =>
      val combinedSet = pair.map(_._1).toSet
      val combinedCoverage = pair.map(_._2).reduce(_ intersect _)
      (combinedSet, combinedCoverage)
    }.toSeq

    if (candidateSets.isEmpty) return Seq.empty


    var lastValidTargets = candidateSets.head._1
    var lastValidCoverage = candidateSets.head._2

    var sumOfReductions = 0.0
    var sumOfReductions2 = 0.0
    var countOfReductions = 0
    var reductionStd = 0.0

    while (candidateSets.nonEmpty) {
      val topCandidate = candidateSets.maxBy(_._2.size)

      val currentSupport = topCandidate._2.size.toDouble
      if (currentSupport <= threshold) {
        return Seq((lastValidTargets, lastValidCoverage))
      }

      sumOfReductions += currentSupport
      sumOfReductions2 += Math.pow(currentSupport, 2)
      countOfReductions += 1
      val reductionMean = sumOfReductions / countOfReductions
      reductionStd = Math.sqrt((sumOfReductions2 / countOfReductions) - Math.pow(reductionMean, 2))

      if (Math.abs(currentSupport - reductionMean) > dropThreshold * reductionStd) {
        return Seq((lastValidTargets, lastValidCoverage))
      }

      lastValidTargets = topCandidate._1
      lastValidCoverage = topCandidate._2

      candidateSets = candidateSets.flatMap { case (currentSet, currentCoverage) =>
        filteredTargets.filterNot(t => currentSet.contains(t._1)).map { t =>
          val newSet = currentSet + t._1
          val newCoverage = currentCoverage intersect t._2
          (newSet, newCoverage)
        }
      }

      candidateSets = candidateSets.filter(_._2.nonEmpty)

    }
    Seq((lastValidTargets, lastValidCoverage))
  }

  private def findXORTargetsBounded(targets: Seq[(String, Set[String])],
                                    frequentTraces: Set[String],
                                    threshold: Double = 0,
                                    branchingBound: Int = 1): Seq[(Set[String], Set[String])] = {
    if (targets.isEmpty) return Seq.empty

    val filteredTargets = if (frequentTraces.nonEmpty) {
      targets.filter { case (_, traceSet) => traceSet.exists(frequentTraces.contains) }
    } else targets

    if (filteredTargets.size == 1)
      return Seq((Set(filteredTargets.head._1), filteredTargets.head._2))

    if (filteredTargets.isEmpty) return Seq.empty

    var candidateSets = filteredTargets.combinations(2).map { pair =>
      val combinedSet = pair.map(_._1).toSet
      val combinedCoverage = (pair.head._2 union pair.last._2) diff (pair.head._2 intersect pair.last._2)
      (combinedSet, combinedCoverage)
    }.toSeq

    if (candidateSets.isEmpty) return Seq.empty

    var lastValidTargets = candidateSets.head._1
    var lastValidCoverage = candidateSets.head._2

    var currentSetSize = 2

    while (candidateSets.nonEmpty && currentSetSize < branchingBound) {
      // Sort candidates by trace coverage and keep the top-1
      val topCandidate = candidateSets.maxBy(_._2.size)
      lastValidTargets = topCandidate._1
      lastValidCoverage = topCandidate._2

      // Check if the threshold is met
      val currentSupport = lastValidCoverage.size.toDouble
      if (currentSupport > 0 && threshold > 0 && currentSupport <= threshold) {
        return Seq((lastValidTargets, lastValidCoverage))
      }

      // Generate new candidates by increasing set size by 1
      candidateSets = candidateSets.flatMap { case (currentSet, currentCoverage) =>
        targets.filterNot(t => currentSet.contains(t._1)).map { t =>
          val newSet = currentSet + t._1
          val newCoverage = (currentCoverage union t._2) diff (currentCoverage intersect t._2)
          (newSet, newCoverage)
        }
      }

      candidateSets = candidateSets.filter(_._2.nonEmpty)
      currentSetSize += 1
    }

    Seq((lastValidTargets, lastValidCoverage))
  }

  private def findXORTargetsUnbounded(targets: Seq[(String, Set[String])],
                                      frequentTraces: Set[String],
                                      threshold: Double = 0,
                                      dropThreshold: Double = 0.5): Seq[(Set[String], Set[String])] = {
    if (targets.isEmpty) return Seq.empty

    val filteredTargets = if (frequentTraces.nonEmpty) {
      targets.filter { case (_, traceSet) => traceSet.exists(frequentTraces.contains) }
    } else targets

    if (filteredTargets.size == 1)
      return Seq((Set(filteredTargets.head._1), filteredTargets.head._2))

    var candidateSets = filteredTargets.combinations(2).map { pair =>
      val combinedSet = pair.map(_._1).toSet
      val combinedCoverage = (pair.head._2 union pair.last._2) diff (pair.head._2 intersect pair.last._2)
      (combinedSet, combinedCoverage)
    }.toSeq

    if (candidateSets.isEmpty) return Seq.empty

    var lastValidTargets = candidateSets.head._1
    var lastValidCoverage = candidateSets.head._2

    var sumOfReductions = 0.0
    var sumOfReductions2 = 0.0
    var countOfReductions = 0
    var reductionStd = 0.0

    while (candidateSets.nonEmpty) {
      val topCandidate = candidateSets.maxBy(_._2.size)

      val currentSupport = topCandidate._2.size.toDouble
      if (currentSupport <= threshold) {
        return Seq((lastValidTargets, lastValidCoverage))
      }

      sumOfReductions += currentSupport
      sumOfReductions2 += Math.pow(currentSupport, 2)
      countOfReductions += 1
      val reductionMean = sumOfReductions / countOfReductions
      reductionStd = Math.sqrt((sumOfReductions2 / countOfReductions) - Math.pow(reductionMean, 2))

      if (Math.abs(currentSupport - reductionMean) > dropThreshold * reductionStd) {
        return Seq((lastValidTargets, lastValidCoverage))
      }

      lastValidTargets = topCandidate._1
      lastValidCoverage = topCandidate._2

      candidateSets = candidateSets.flatMap { case (currentSet, currentCoverage) =>
        filteredTargets.filterNot(t => currentSet.contains(t._1)).map { t =>
          val newSet = currentSet + t._1
          val newCoverage = (currentCoverage union t._2) diff (currentCoverage intersect t._2)
          (newSet, newCoverage)
        }
      }

      candidateSets = candidateSets.filter(_._2.nonEmpty)
    }
    Seq((lastValidTargets, lastValidCoverage))
  }

  //////////////////////////////////////////////////////////////////////
  //    Extraction of branched Constraints with calculated support    //
  //////////////////////////////////////////////////////////////////////

  def extractAllOrderedConstraints(constraints: Dataset[PairConstraint],
                                   totalTraces: Long,
                                   support: Double = 0,
                                   policy: String = "OR",
                                   branchingBound: Int = 0,
                                   filterRare: Option[Boolean] = Some(false)): Array[TargetBranchedConstraint] = {

    getBranchedConstraints(constraints, totalTraces, support, branchingBound, policy, filterRare = filterRare)
      .collect()
  }

//  def extractAllOrderedConstraints (constraints: Dataset[PairConstraint],
//                                    totalTraces: Long,
//                                    support: Double,
//                                    policy: String,
//                                    branchingBound: Int = 0): Array[TargetBranchedConstraint] = {
//
//    val spark = SparkSession.builder().getOrCreate()
//    import spark.implicits._
//
//    if (policy == "OR") {
//      getORBranchedConstraints(constraints, totalTraces, support, branchingBound).collect()
//    } else if (policy == "AND") {
//      getANDBranchedConstraints(constraints, totalTraces, support, branchingBound).collect()
//    } else if (policy == "XOR") {
//      getXORBranchedConstraints(constraints, totalTraces, support, branchingBound).collect()
//    } else {
//      spark.emptyDataset[TargetBranchedConstraint].collect()
//    }
//  }

  def extractAllUnorderedConstraints(merge_u: Dataset[(String, Long)], merge_i: Dataset[(String, String, Long)],
                                     activity_matrix: RDD[(String, String)], support: Double, total_traces: Long)
  : Array[TargetBranchedConstraint] = {
    Array.empty[auth.datalab.siesta.Structs.TargetBranchedConstraint]
  }
}
