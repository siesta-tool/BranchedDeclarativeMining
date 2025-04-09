package auth.datalab.siesta

import auth.datalab.siesta.Structs.{PairConstraint, PositionConstraint, SourceBranchedPairConstraint, TargetBranchedPairConstraint}
import org.apache.spark.sql.functions.{collect_set, count, explode, pow}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}



object BranchedDeclare {

  def extractBranchedPositionConstraints( constraints: Dataset[PositionConstraint],
                                          totalTraces: Long,
                                          support: Double = 0,
                                          policy: String = "OR",
                                          branchingBound: Int = 2,
                                          dropFactor: Double = 2.5,
                                          filterUnderBound: Boolean = false,
                                          filterRare: Boolean = false): Array[(String, Array[String], Double)] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Group the constraints by rule
    val result = constraints.groupByKey(_.rule).mapGroups { case (rule, iter) =>
      val eventMap = iter.map(c => (c.eventType, c.traces.toSet)).toSeq

      // Compute frequent traces for "AND" and "XOR" branching if needed
      val frequentTraces = if (!filterRare && policy != "OR")
        eventMap.flatMap(_._2).groupBy(identity)
          .mapValues(_.size)
          .filter(_._2 == eventMap.flatMap(_._2).groupBy(identity).mapValues(_.size).values.max)
          .keys.toSet
      else
        Set.empty[String]

      // Choose the appropriate find method based on policy
      val result = (policy, branchingBound > 1) match {
        case ("OR", true) => findORBranchesBounded(eventMap, branchingBound)
        case ("OR", false) => findORBranchesUnbounded(eventMap, support, dropFactor)
        case ("AND", true) => findANDBranchesBounded(null, rule, eventMap, frequentTraces, support * totalTraces, branchingBound)
        case ("AND", false) => findANDBranchesUnbounded(null, rule, eventMap, frequentTraces, support * totalTraces)
        case ("XOR", true) => findXORBranchesBounded(null, rule, eventMap, frequentTraces, support * totalTraces, branchingBound)
        case ("XOR", false) => findXORBranchesUnbounded(null, rule, eventMap, frequentTraces, support * totalTraces)
        case _ => throw new IllegalArgumentException(s"Unsupported branching policy: $policy")
      }
      val events = result.flatMap(_._1)   // Extract event names
      val traces = result.flatMap(_._2)   // Extract associated trace sets
      (rule, events.toArray, traces.toArray.length.toDouble / totalTraces)
    }.filter(_._3 > support)

    if (filterUnderBound)
      result.filter(_._2.length == branchingBound).collect()
    else
      result.collect()
  }


















  def extractAllOrderedConstraints(constraints: Dataset[PairConstraint],
                                   totalTraces: Long,
                                   support: Double = 0,
                                   policy: String = "OR",
                                   branchingType: String = "TARGET",
                                   branchingBound: Int = 2,
                                   dropFactor: Double = 2.5,
                                   filterBounded: Boolean = false,
                                   filterRare: Option[Boolean] = Some(false))
                                  : Array[(String, String, Double)] = {
    if (branchingType == "TARGET")
        getTargetBranchedConstraints(constraints, totalTraces, support, branchingBound, policy, filterRare =
        filterRare, dropFactor = dropFactor, filterBounded = filterBounded)
    else if (branchingType == "SOURCE")

        getSourceBranchedConstraints(constraints, totalTraces, support, branchingBound, policy,  filterRare =
        filterRare, dropFactor = dropFactor, filterBounded = filterBounded)
    else
      throw new IllegalArgumentException("Only SOURCE | TARGET branching is available!")
  }

  private def getTargetBranchedConstraints(constraints: Dataset[PairConstraint],
                                           totalTraces: Long,
                                           threshold: Double = 0,
                                           branchingBound: Int = 2,
                                           policy: String = "OR",
                                           dropFactor: Double = 2.5,
                                           filterBounded: Boolean = false,
                                           filterRare: Option[Boolean] = Some(false),
                                           printNum: Option[Boolean] = Some(false)): Array[(String, String, Double)] = {
      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._

      val pairConstraintCounts: DataFrame = explodePairConstraints(constraints)

      val pairConstraints = pairConstraintCounts.as[(String, String, String, Seq[String])]
        .groupByKey { case (rule, activationEvent, _, _) => (rule, activationEvent) }
        .mapGroups { case ((rule, activationEvent), rows) => (rule, activationEvent, rows.map{ case (_,_,tar,tr) => (tar, tr)}.toSeq) }
        .collect()

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
          val frequentTraces = if (filterRare.getOrElse(false) && policy != "OR")
            targetEventData.flatMap(_._2).groupBy(identity)
              .mapValues(_.size)
              .filter(_._2 == targetEventData.flatMap(_._2).groupBy(identity).mapValues(_.size).values.max)
              .keys.toSet
          else
            Set.empty[String]

          // Choose the appropriate find method based on branchingType
          val targetResult = (policy, branchingBound > 1) match {
            case ("OR", true) => findORBranchesBounded(targetEventData, branchingBound)
            case ("OR", false) => findORBranchesUnbounded(targetEventData, threshold, dropFactor)
            case ("AND", true) => findANDBranchesBounded(pairConstraints, rule, targetEventData, frequentTraces, threshold * totalTraces, branchingBound)
            case ("AND", false) => findANDBranchesUnbounded(pairConstraints, rule, targetEventData, frequentTraces, threshold * totalTraces)
            case ("XOR", true) => findXORBranchesBounded(pairConstraints, rule, targetEventData, frequentTraces, threshold * totalTraces, branchingBound)
            case ("XOR", false) => findXORBranchesUnbounded(pairConstraints, rule, targetEventData, frequentTraces, threshold * totalTraces)
            case _ => throw new IllegalArgumentException(s"Unsupported branching policy: $policy")
          }

          // Extract target events and their associated traces
          val targetEvents = targetResult.flatMap(_._1) // Extract target event names
          val targetTraces = targetResult.flatMap(_._2) // Extract associated trace sets

          // Calculate the total number of unique traces for the targets
          val totalUniqueTraces = targetTraces.distinct.size.toDouble

          // Create a TargetBranchedConstraint for the rule and its source event
          TargetBranchedPairConstraint(rule, activationEvent, targetEvents.toArray, totalUniqueTraces / totalTraces)
        }.filter(_.support > threshold)

    if (printNum.get)
      println(s"$policy # = " + groupedConstraints.filter(_.support > threshold).count())

    var result = groupedConstraints
    if (filterBounded)
      result = groupedConstraints.filter(_.targets.length == branchingBound)

    result.map(x => (x.rule, x.source + "|" + x.targets.mkString(","), x.support)).collect()
  }

  private def getSourceBranchedConstraints(constraints: Dataset[PairConstraint],
                                           totalTraces: Long,
                                           threshold: Double = 0,
                                           branchingBound: Int = 2,
                                           policy: String = "OR",
                                           dropFactor: Double = 2.5,
                                           filterBounded: Boolean = false,
                                           filterRare: Option[Boolean] = Some(false),
                                           printNum: Option[Boolean] = Some(false)): Array[(String, String, Double)] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val pairConstraintCounts: DataFrame = explodePairConstraints(constraints)
    val pairConstraints = pairConstraintCounts.as[(String, String, String, Seq[String])]
      .groupByKey { case (rule, targetEvent, _, _) => (rule, targetEvent) }
      .mapGroups { case ((rule, targetEvent), rows) => (rule, targetEvent, rows.map{ case (_,_,tar,tr) => (tar, tr)}.toSeq) }
      .collect()

    // Group by (rule, targetEvent), calculate source sets
    val groupedConstraints = pairConstraintCounts
      .as[(String, String, String, Seq[String])]
      .groupByKey { case (rule, _, targetEvent, _) => (rule, targetEvent) }
      .mapGroups { case ((rule, targetEvent), rows) =>

        // Create a list of source events and their corresponding trace sets
        val sourceEventData = rows.toSeq.map { case (_, sourceEvent, _, traceSet) =>
          (sourceEvent, traceSet.toSet)
        }

        // Compute frequent traces for "AND" and "XOR" branching if needed
        val frequentTraces = if (filterRare.getOrElse(false) && policy != "OR")
          sourceEventData.flatMap(_._2).groupBy(identity)
            .mapValues(_.size)
            .filter(_._2 == sourceEventData.flatMap(_._2).groupBy(identity).mapValues(_.size).values.max)
            .keys.toSet
        else
          Set.empty[String]

        // Choose the appropriate find method based on branchingType
        val sourceResult = (policy, branchingBound > 0) match {
          case ("OR", true)  => findORBranchesBounded(sourceEventData, branchingBound)
          case ("OR", false) => findORBranchesUnbounded(sourceEventData, threshold * totalTraces, dropFactor)
          case ("AND", true) => findANDBranchesBounded(pairConstraints, rule, sourceEventData, frequentTraces, threshold * totalTraces, branchingBound)
          case ("AND", false) => findANDBranchesUnbounded(pairConstraints, rule, sourceEventData, frequentTraces, threshold * totalTraces)
          case ("XOR", true) => findXORBranchesBounded(pairConstraints, rule, sourceEventData, frequentTraces, threshold * totalTraces, branchingBound)
          case ("XOR", false) => findXORBranchesUnbounded(pairConstraints, rule, sourceEventData, frequentTraces, threshold * totalTraces)
          case _ => throw new IllegalArgumentException(s"Unsupported branching policy: $policy")
        }

        // Extract source events and their associated traces
        val sourceEvents = sourceResult.flatMap(_._1) // Extract source event names
        val sourceTraces = sourceResult.flatMap(_._2) // Extract associated trace sets

        // Calculate the total number of unique traces for the sources
        val totalUniqueTraces = sourceTraces.distinct.size.toDouble

        // Create a SourceBranchedConstraint for the rule and its target event
        SourceBranchedPairConstraint(
          rule = rule,
          sources = sourceEvents.toArray,
          target = targetEvent,
          support = totalUniqueTraces / totalTraces
        )
      }.filter(_.support > threshold)

    if (printNum.get)
      println(s"$policy # = " + groupedConstraints.filter(_.support >= threshold).count())

    var result = groupedConstraints
    if (filterBounded)
      result = result.filter(_.sources.length == branchingBound)
    result.map(x=> (x.rule, x.sources.mkString(",") + "|" + x.target, x.support)).collect()
  }

  //////////////////////////////////////////////////////////////////////
  //                Greedy target extraction and helpers             //
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


  private def findORBranchesBounded(targets: Seq[(String, Set[String])],
                                    branchingBound: Int = 2): Seq[(Set[String], Set[String])] = {
    if (targets.isEmpty) return Seq.empty

    var currentTargetsWithTraces = targets.toMap

    var currentTraceCoverage = currentTargetsWithTraces.values.flatten.toSet

    while (currentTargetsWithTraces.size > branchingBound && currentTargetsWithTraces.nonEmpty) {
      // Evaluate the contribution of each target
      val targetContributions = currentTargetsWithTraces.map { case (target, traces) =>
        val contribution = currentTraceCoverage.size - currentTargetsWithTraces.filter(_._1 != target).values.flatten
          .toSet.size
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


  private def findORBranchesUnbounded(targets: Seq[(String, Set[String])],
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
        val contribution = currentTraceCoverage.size - currentTargetsWithTraces.filter(_._1 != target).values.flatten
          .toSet.size
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
      if (threshold > 0 && currentTraceCoverage.size.toDouble <= threshold || currentTraceCoverage.size.toDouble == 0) {
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


  private def findANDBranchesBounded( pairConstraints: Array[(String, String, Seq[(String, Seq[String])])],
                                      rule: String,
                                      targets: Seq[(String, Set[String])],
                                      frequentTraces: Set[String],
                                      threshold: Double = 0,
                                      branchingBound: Int = 2): Seq[(Set[String], Set[String])] = {
    if (targets.isEmpty) return Seq.empty

    val filteredTargets = if (frequentTraces.nonEmpty) {
      targets.filter { case (_, traceSet) => traceSet.exists(frequentTraces.contains) }
    } else targets

    if (filteredTargets.size == 1)
      return Seq((Set(filteredTargets.head._1), filteredTargets.head._2))

    if (filteredTargets.isEmpty) return Seq.empty

    var candidateSets = if (!rule.contains("chain")) {
      filteredTargets.combinations(2).map { pair =>
        val combinedSet = pair.map(_._1).toSet
        val combinedCoverage = pair.map(_._2).reduce(_ intersect _)
        (combinedSet, combinedCoverage)
      }.toSeq
    } else {
      filteredTargets.flatMap { case (key, traceSet) =>
        pairConstraints.flatMap { case (r, keyInPair, rows) =>
          if (rule.split("chain-").length > 1 && r == rule.split("chain-")(1) && key == keyInPair) {
            rows.map(row => (Set(key, row._1), row._2.toSet intersect traceSet)).filter(_._2.nonEmpty)
          } else {
            None
          }
        }
      }
    }

    if (candidateSets.isEmpty) return Seq.empty

    var lastValidTargets = candidateSets.maxBy(_._2.size)._1
    var lastValidCoverage = candidateSets.maxBy(_._2.size)._2

    var currentSetSize = 2

    while (candidateSets.nonEmpty && currentSetSize <= branchingBound) {
      // Sort candidates by trace coverage and keep the top-1
      val topCandidate = candidateSets.maxBy(_._2.size)

      // Check if the threshold is met
      val currentSupport = topCandidate._2.size.toDouble
      if (currentSupport == 0 || threshold > 0 && currentSupport <= threshold) {
        return Seq((lastValidTargets, lastValidCoverage))
      }

      lastValidTargets = topCandidate._1
      lastValidCoverage = topCandidate._2

      // Generate new candidates by increasing set size by 1
      candidateSets = candidateSets.filter(_._1 == lastValidTargets)
      candidateSets = if (!rule.contains("chain")) {
        candidateSets.flatMap { case (currentSet, currentCoverage) =>
          targets.filterNot(t => currentSet.contains(t._1)).map { t =>
            val newSet = currentSet + t._1
            val newCoverage = currentCoverage intersect t._2
            if (newCoverage.size > 1)
              (newSet, newCoverage)
            else
              (Set.empty[String], Set.empty[String])
          }
        }.filter(_._1.nonEmpty)
      } else {
        candidateSets.flatMap { case (unionSet, traceSet) =>
          pairConstraints.flatMap { case (r, keyInPair, rows) =>
            if (rule.split("chain-").length > 1 && r == rule.split("chain-")(1) && unionSet.last == keyInPair) {
              rows.map { case (key, row) =>
                val newUnionSet = unionSet + key
                val newTraceSet = traceSet intersect row.toSet
                (newUnionSet, newTraceSet)
              }
            } else {
              None
            }
          }
        }.filter(_._1.nonEmpty)
      }

      candidateSets = candidateSets.filter(_._2.nonEmpty)
      currentSetSize += 1
    }

    Seq((lastValidTargets, lastValidCoverage))
  }

  private def findANDBranchesUnbounded( pairConstraints: Array[(String, String, Seq[(String, Seq[String])])],
                                        rule: String,
                                        targets: Seq[(String, Set[String])],
                                        frequentTraces: Set[String],
                                        threshold: Double = 0,
                                        dropThreshold: Double = 0.5): Seq[(Set[String], Set[String])] = {
    if (targets.isEmpty) return Seq.empty

    val filteredTargets = if (frequentTraces.nonEmpty) {
      targets.filter { case (_, traceSet) => traceSet.exists(frequentTraces.contains) }
    } else targets

    if (filteredTargets.size == 1)
      return Seq((Set(filteredTargets.head._1), filteredTargets.head._2))

    var candidateSets = if (!rule.contains("chain")) {
      filteredTargets.combinations(2).map { pair =>
        val combinedSet = pair.map(_._1).toSet
        val combinedCoverage = pair.map(_._2).reduce(_ intersect _)
        (combinedSet, combinedCoverage)
      }.toSeq
    } else {
      filteredTargets.flatMap { case (key, traceSet) =>
        pairConstraints.flatMap { case (r, keyInPair, rows) =>
          if (rule.split("chain-").length > 1 && r == rule.split("chain-")(1) && key == keyInPair) {
            rows.map(row => (Set(key, row._1), row._2.toSet intersect traceSet)).filter(_._2.nonEmpty)
          } else {
            None
          }
        }
      }
    }

    if (candidateSets.isEmpty) return Seq.empty

    var lastValidTargets = candidateSets.maxBy(_._2.size)._1
    var lastValidCoverage = candidateSets.maxBy(_._2.size)._2

    if (lastValidCoverage.isEmpty)
      return Seq((Set(filteredTargets.maxBy(_._2.size)._1), filteredTargets.maxBy(_._2.size)._2))

    var sumOfReductions = 0.0
    var sumOfReductions2 = 0.0
    var countOfReductions = 0
    var reductionStd = 0.0

    while (candidateSets.nonEmpty && lastValidCoverage.size > 1) {
      val topCandidate = candidateSets.maxBy(_._2.size)
      val currentSupport = topCandidate._2.size.toDouble
      if (currentSupport <= threshold || currentSupport == 0 || topCandidate._1 == lastValidTargets && countOfReductions > 0) {
        return Seq((lastValidTargets, lastValidCoverage))
      }

      sumOfReductions += currentSupport
      sumOfReductions2 += Math.pow(currentSupport, 2)
      countOfReductions += 1
      val reductionMean = sumOfReductions / countOfReductions
      reductionStd = Math.sqrt((sumOfReductions2 / countOfReductions) - Math.pow(reductionMean, 2))

      if (Math.abs(currentSupport - reductionMean) > dropThreshold * reductionStd || currentSupport == 0) {
        return Seq((lastValidTargets, lastValidCoverage))
      }

      lastValidTargets = topCandidate._1
      lastValidCoverage = topCandidate._2

      candidateSets = candidateSets.filter(_._1 == lastValidTargets)
      candidateSets = if (!rule.contains("chain")) {
        candidateSets.flatMap { case (currentSet, currentCoverage) =>
          targets.filterNot(t => currentSet.contains(t._1)).map { t =>
            val newSet = currentSet + t._1
            val newCoverage = currentCoverage intersect t._2
            if (newCoverage.size > 1 && newCoverage.intersect(lastValidCoverage) != Set.empty[String])
              (newSet, newCoverage)
            else
              (Set.empty[String], Set.empty[String])
          }
        }.filter(_._1.nonEmpty)
      } else {
        candidateSets.flatMap { case (unionSet, traceSet) =>
          pairConstraints.flatMap { case (r, keyInPair, rows) =>
            if (rule.split("chain-").length > 1 && r == rule.split("chain-")(1) && unionSet.last == keyInPair) {
              rows.map { case (key, row) =>
                val newUnionSet = unionSet + key
                val newTraceSet = traceSet intersect row.toSet
                (newUnionSet, newTraceSet)
              }
            } else {
              None
            }
          }
        }.filter(_._1.nonEmpty)
      }

      candidateSets = candidateSets.filter(_._2.nonEmpty)
    }
    Seq((lastValidTargets, lastValidCoverage))
  }

  private def findXORBranchesBounded( pairConstraints: Array[(String, String, Seq[(String, Seq[String])])],
                                      rule: String,
                                      targets: Seq[(String, Set[String])],
                                      frequentTraces: Set[String],
                                      threshold: Double = 0,
                                      branchingBound: Int = 2): Seq[(Set[String], Set[String])] = {
    if (targets.isEmpty) return Seq.empty

    val filteredTargets = if (frequentTraces.nonEmpty) {
      targets.filter { case (_, traceSet) => traceSet.exists(frequentTraces.contains) }
    } else targets

    if (filteredTargets.size == 1)
      return Seq((Set(filteredTargets.head._1), filteredTargets.head._2))

    if (filteredTargets.isEmpty) return Seq.empty

    var candidateSets = if (!rule.contains("chain")) {
      filteredTargets.combinations(2).map { pair =>
        val combinedSet = pair.map(_._1).toSet
        val combinedCoverage = (pair.head._2 union pair.last._2) diff (pair.head._2 intersect pair.last._2)
        (combinedSet, combinedCoverage)
      }.toSeq
    } else {
      filteredTargets.flatMap { case (key, traceSet) =>
        pairConstraints.flatMap { case (r, keyInPair, rows) =>
          if (rule.split("chain-").length > 1 && r == rule.split("chain-")(1) && key == keyInPair) {
            rows.map(row => (Set(key, row._1), (row._2.toSet union traceSet) diff (row._2.toSet intersect traceSet))).filter(_._2.nonEmpty)
          } else {
            None
          }
        }
      }
    }


    if (candidateSets.isEmpty) return Seq.empty

    var lastValidTargets = candidateSets.head._1
    var lastValidCoverage = candidateSets.head._2

    var currentSetSize = 2

    while (candidateSets.nonEmpty && currentSetSize <= branchingBound) {
      // Sort candidates by trace coverage and keep the top-1
      val topCandidate = candidateSets.maxBy(_._2.size)

      // Check if the threshold is met
      val currentSupport = topCandidate._2.size.toDouble
      if (currentSupport == 0 || threshold > 0 && currentSupport <= threshold) {
        return Seq((lastValidTargets, lastValidCoverage))
      }

      lastValidTargets = topCandidate._1
      lastValidCoverage = topCandidate._2

      // Generate new candidates by increasing set size by 1
      candidateSets = candidateSets.filter(_._1 == lastValidTargets)
      candidateSets = if (!rule.contains("chain")) {
        candidateSets.flatMap { case (currentSet, currentCoverage) =>
          targets.filterNot(t => currentSet.contains(t._1)).map { t =>
            val newSet = currentSet + t._1
            val newCoverage = (currentCoverage union t._2) diff (currentCoverage intersect t._2)
            if (newCoverage.size > 1)
              (newSet, newCoverage)
            else
              (Set.empty[String], Set.empty[String])
          }
        }.filter(_._1.nonEmpty)
      } else {
        candidateSets.flatMap { case (unionSet, traceSet) =>
          pairConstraints.flatMap { case (r, keyInPair, rows) =>
            if (rule.split("chain-").length > 1 && r == rule.split("chain-")(1) && unionSet.last == keyInPair) {
              rows.map { case (key, row) =>
                val newUnionSet = unionSet + key
                val newTraceSet = (traceSet union row.toSet) diff (traceSet intersect row.toSet)
                (newUnionSet, newTraceSet)
              }
            } else {
              None
            }
          }
        }.filter(_._1.nonEmpty)
      }

      candidateSets = candidateSets.filter(_._2.nonEmpty)
      currentSetSize += 1
    }

    Seq((lastValidTargets, lastValidCoverage))
  }

  private def findXORBranchesUnbounded( pairConstraints: Array[(String, String, Seq[(String, Seq[String])])],
                                        rule: String,
                                        targets: Seq[(String, Set[String])],
                                        frequentTraces: Set[String],
                                        threshold: Double = 0,
                                        dropThreshold: Double = 0.5): Seq[(Set[String], Set[String])] = {
    if (targets.isEmpty) return Seq.empty

    val filteredTargets = if (frequentTraces.nonEmpty) {
      targets.filter { case (_, traceSet) => traceSet.exists(frequentTraces.contains) }
    } else targets

    if (filteredTargets.size == 1)
      return Seq((Set(filteredTargets.head._1), filteredTargets.head._2))

    var candidateSets = if (!rule.contains("chain")) {
      filteredTargets.combinations(2).map { pair =>
        val combinedSet = pair.map(_._1).toSet
        val combinedCoverage = (pair.head._2 union pair.last._2) diff (pair.head._2 intersect pair.last._2)
        (combinedSet, combinedCoverage)
      }.toSeq
    } else {
      filteredTargets.flatMap { case (key, traceSet) =>
        pairConstraints.flatMap { case (r, keyInPair, rows) =>
          if (rule.split("chain-").length > 1 && r == rule.split("chain-")(1) && key == keyInPair) {
            rows.map(row => (Set(key, row._1), (row._2.toSet union traceSet) diff (row._2.toSet intersect traceSet))).filter(_._2.nonEmpty)
          } else {
            None
          }
        }
      }
    }



    if (candidateSets.isEmpty) return Seq.empty

    var lastValidTargets = candidateSets.maxBy(_._2.size)._1
    var lastValidCoverage = candidateSets.maxBy(_._2.size)._2

    if (lastValidCoverage.isEmpty)
      return Seq((Set(filteredTargets.maxBy(_._2.size)._1), filteredTargets.maxBy(_._2.size)._2))

    var sumOfReductions = 0.0
    var sumOfReductions2 = 0.0
    var countOfReductions = 0
    var reductionStd = 0.0

    while (candidateSets.nonEmpty) {
      val topCandidate = candidateSets.maxBy(_._2.size)

      val currentSupport = topCandidate._2.size.toDouble
//      println(candidateSets.size, topCandidate._1.size, topCandidate._2.size)
      if (currentSupport <= threshold || currentSupport == 0 || topCandidate._1 == lastValidTargets && countOfReductions > 0) {
        return Seq((lastValidTargets, lastValidCoverage))
      }

      sumOfReductions += currentSupport
      sumOfReductions2 += Math.pow(currentSupport, 2)
      countOfReductions += 1
      val reductionMean = sumOfReductions / countOfReductions
      reductionStd = Math.sqrt((sumOfReductions2 / countOfReductions) - Math.pow(reductionMean, 2))

      if (Math.abs(currentSupport - reductionMean) > dropThreshold * reductionStd || currentSupport == 0) {
        return Seq((lastValidTargets, lastValidCoverage))
      }

      lastValidTargets = topCandidate._1
      lastValidCoverage = topCandidate._2

      // Generate new candidates by increasing set size by 1
      candidateSets = candidateSets.filter(_._1 == lastValidTargets)
      candidateSets = if (!rule.contains("chain")) {
        candidateSets.flatMap { case (currentSet, currentCoverage) =>
          targets.filterNot(t => currentSet.contains(t._1)).map { t =>
            val newSet = currentSet + t._1
            val newCoverage = (currentCoverage union t._2) diff (currentCoverage intersect t._2)
            if (newCoverage.size > 1)
              (newSet, newCoverage)
            else
              (Set.empty[String], Set.empty[String])
          }
        }.filter(_._1.nonEmpty)
      } else {
        candidateSets.flatMap { case (unionSet, traceSet) =>
          pairConstraints.flatMap { case (r, keyInPair, rows) =>
            if (rule.split("chain-").length > 1 && r == rule.split("chain-")(1) && unionSet.last == keyInPair) {
              rows.map { case (key, row) =>
                val newUnionSet = unionSet + key
                val newTraceSet = (traceSet union row.toSet) diff (traceSet intersect row.toSet)
                (newUnionSet, newTraceSet)
              }
            } else {
              None
            }
          }
        }.filter(_._1.nonEmpty)
      }

      candidateSets = candidateSets.filter(_._2.nonEmpty)
    }
    Seq((lastValidTargets, lastValidCoverage))
  }

}
