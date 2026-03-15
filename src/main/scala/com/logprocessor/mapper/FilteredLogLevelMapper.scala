package com.logprocessor.mapper

import com.logprocessor.utils.{ComputeIntervals, CreateLogger}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.Logger

import scala.util.matching.Regex

/**
 * This Mapper is used by Task 2.
 * It will first load configuration file to determine the pattern of the log message (the entire message), injected string's regex & interval.
 * Then using regex it will first check, if the line matches with the log pattern(the entire log message e.g: "09:01:11.455 [scala-execution-context-global-12] DEBUG com.logprocessor.utils.Parameters$ - A~9Md_CUb,0")
 * If matched, then it extracts injectedString(for above example that will be "A~9Md_CUb,0"), timeStamp("09:01:11.455"), logErrorLevel("DEBUG")
 * Next it will check if the logErrorLevel matches with the desiredLogErrorLevel(loaded from configuration file)
 * If logErrorLevel == desiredLogErrorLevel, next it checks if the injectedString matches injectedStringRegex pattern. (Note: the regEx pattern is un-anchored type, so if a match occurs anywhere, we consider that)
 * If above match found, it construct finalKey by calling determineInterval utility function
 * Finally, it writes to the context.write(finalKey, 1)
 */
class FilteredLogLevelMapper extends Mapper[Object, Text, Text, IntWritable] {
  private final val one = new IntWritable(1)
  val logger: Logger = CreateLogger(classOf[FilteredLogLevelMapper])
  private val config: Config = ConfigFactory.load.getConfig("LogConfiguration")
  private val word = new Text()

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    val line = value.toString
    // Get the log pattern of the log from config
    val logPattern = new Regex(config.getString("LogPattern"))
    // Get the desired logErrorLevel from config (For Task 2)
    val desiredLogErrorLevel = config.getString("LogErrorLevel")
    // Get the injected string pattern of the logs from config
    val injectedStringRegex = new Regex(config.getString("InjectedStringPattern")).unanchored

    // Check if the line matches our log pattern
    line match
      case logPattern(timeStamp, _, logErrorLevel, _, injectedString) =>
        // Found a match
        // For task 2 we need to check the logErrorLevel with desireLogErrorLevel
        if (logErrorLevel == desiredLogErrorLevel) {
          // Check for injectedString match
          injectedString match
            case injectedStringRegex(_) =>
              // Found a match
              val finalKey = ComputeIntervals.determineIntervals(timeStamp)
              word.set(finalKey)
              logger.debug(s"${this.getClass.getName}, writing to context:(${word},${one})")
              context.write(word, one)
            case _ =>
        }
      case _ =>
    // No Match Found
  }
}
