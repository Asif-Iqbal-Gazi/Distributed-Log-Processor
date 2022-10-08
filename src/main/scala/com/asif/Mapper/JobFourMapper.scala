package com.asif.Mapper

import com.asif.HelperUtils.CreateLogger
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.Logger

import scala.util.matching.Regex

/**
 * This Mapper is Used by Task 4.
 * It first loads the configuration file, and from the configuration file it loads LogPatternRegex(this pattern for entire log message) & InjectedStringRegex.
 * Then it checks if the line matches the LogPattern regex.
 * If matched, it extracts "logErrorLevel" & injectedString from the log message.
 * Then it extracts all non-overlapping matches of InjectedStringRegex on "injectedString" and stores in a list.
 * Then it iterates over the list and calculates the length of the matched extract.
 * Finally, writes to the context.write(logErrorLevel, extractLength)
 */
class JobFourMapper extends Mapper[Object, Text, Text, IntWritable] {
  val logger: Logger = CreateLogger(classOf[JobFourMapper])
  private val config: Config = ConfigFactory.load.getConfig("LogConfiguration")
  private val word = new Text()

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    val line = value.toString
    // Get the log pattern of the log from config
    val logPattern = new Regex(config.getString("LogPattern"))
    // Get the injected string pattern of the logs from config
    val injectedStringRegex = new Regex(config.getString("InjectedStringPattern"))

    // Check if the line matches our log pattern
    line match
      case logPattern(_, _, logErrorLevel, _, injectedString) =>
        // Found a match
        val allMatch = injectedStringRegex.findAllIn(injectedString).toList
        // Check for injectedString match
        if (allMatch.nonEmpty) {
          // Found a match
          allMatch.foreach(matchedInjectedString => {
            //println("String: " + matchedInjectedString + " length: " + matchedInjectedString.length)
            val finalKey = logErrorLevel
            val finalValue: Int = matchedInjectedString.length
            word.set(finalKey)
            //println("Key: " + finalKey + " Value: " + finalValue)
            logger.debug(s"${this.getClass.getName}, writing to context:($word,$finalValue)")
            context.write(word, new IntWritable(finalValue))
          })
        }
      case _ =>
  }
}

