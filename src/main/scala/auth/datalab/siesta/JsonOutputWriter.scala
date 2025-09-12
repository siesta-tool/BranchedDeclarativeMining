package auth.datalab.siesta

import auth.datalab.siesta.Structs.{Constraint, ConstraintGroup, MiningResult}
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write
import java.io.{BufferedWriter, FileWriter}

/**
 * Handles JSON serialization and file output for constraint mining results
 */
class JsonOutputWriter {
  
  // Implicit JSON formats for serialization
  implicit val formats: Formats = DefaultFormats

  /**
   * Writes mining results to a JSON file with pretty formatting
   * 
   * @param miningResult The mining results to write
   * @param fileName The output file name
   */
  def writeToFile(miningResult: MiningResult, fileName: String): Unit = {
    val jsonString = write(miningResult)
    val writer = new BufferedWriter(new FileWriter(fileName))
    
    try {
      // Pretty print the JSON for better readability
      val prettyJson = org.json4s.native.JsonMethods.pretty(
        org.json4s.native.JsonMethods.render(
          org.json4s.native.JsonMethods.parse(jsonString)
        )
      )
      writer.write(prettyJson)
    } finally {
      writer.close()
    }
  }

  /**
   * Generates the output filename based on configuration parameters
   * 
   * @param logName The log name
   * @param support The support threshold
   * @param branchingBound The branching bound
   * @param branchingPolicy The normalized branching policy (null if disabled)
   * @param config Additional configuration for more semantic naming
   * @return The generated filename
   */
  def generateFileName(logName: String, support: Double, branchingBound: Int, branchingPolicy: String, 
                      config: auth.datalab.siesta.Structs.Config): String = {
    
    val parts = scala.collection.mutable.ListBuffer[String]()
    
    // Base name
    parts += logName
    
    // Support threshold (only if non-zero)
    if (support > 0) {
      parts += f"s${support}%.2f".replace(".", "")
    }
    
    // Branching configuration
    if (config.isBranchingEnabled) {
      val policyLetter = config.getEffectiveBranchingPolicy.toLowerCase.take(1)
      val typeLetter = config.getEffectiveBranchingType.toLowerCase.take(1) // "s" for source, "t" for target
      
      if (branchingBound > 0) {
        parts += s"${typeLetter}${policyLetter}${branchingBound}"
      } else {
        parts += s"${typeLetter}${policyLetter}"
      }
    }
    
    // Drop factor (only if not default)
    if (config.dropFactor != 1.5) {
      parts += f"d${config.dropFactor}%.1f".replace(".", "")
    }
    
    // Filters
    val filters = scala.collection.mutable.ListBuffer[String]()
    if (config.filterRare) filters += "r"
    if (config.filterUnderBound) filters += "u"
    if (filters.nonEmpty) {
      parts += s"f${filters.mkString("")}"
    }
    
    // Mining mode
    val modes = scala.collection.mutable.ListBuffer[String]()
    if (config.hardRediscovery) modes += "h"
    if (config.quickMining) modes += "q"
    if (modes.nonEmpty) {
      parts += s"m${modes.mkString("")}"
    }
    
    "constraints_" + parts.mkString("_") + ".json"
  }

  /**
   * Legacy filename generation for backwards compatibility
   * 
   * @deprecated Use generateFileName with config parameter instead
   */
  @deprecated("Use generateFileName with config parameter", "1.1")
  def generateFileName(logName: String, support: Double, branchingBound: Int, branchingPolicy: String): String = {
    val policy = if (branchingPolicy == null) "none" else branchingPolicy
    s"constraints_${logName}_s${support}_b${branchingBound}_p${policy}.json"
  }
}