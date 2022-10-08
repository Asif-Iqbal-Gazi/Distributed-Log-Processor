package com.asif.Mapper

import com.asif.HelperUtils.CreateLogger
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.Logger

import scala.util.matching.Regex

/**
 * This Mapper is used by Task 3.
 * It first loads the configuration file, and from the configuration file it loads LogPattern(this pattern for entire log message)
 * Then it checks if the line matches the LogPattern regex.
 * If matched, it extracts logErrorLevel from the log message.
 * Finally, writes to the context.write(logErrorLevel, 1)
 */
class JobThreeMapper extends Mapper[Object, Text, Text, IntWritable] {
  private final val one = new IntWritable(1)
  val logger: Logger = CreateLogger(classOf[JobThreeMapper])
  private val config: Config = ConfigFactory.load.getConfig("LogConfiguration")
  private val word = new Text()

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    val line = value.toString
    // Get the log pattern of the log from config
    val logPattern = new Regex(config.getString("LogPattern"))
    // Check if the line matches our log pattern
    line match
      case logPattern(_, _, logErrorLevel, _*) =>
        // Found a match
        val finalKey = logErrorLevel
        word.set(finalKey)
        logger.debug(s"${this.getClass.getName}, writing to context:($word,$one)")
        context.write(word, one)
      case _ =>
  }
}
