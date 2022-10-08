package com.asif.Mapper

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import scala.util.matching.Regex

class JobThreeMapper extends Mapper[Object, Text, Text, IntWritable] {
  private final val one = new IntWritable(1)
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
        // Check for injectedString match
        injectedString match
          case injectedStringRegex(_) =>
            // Found a match
            val finalKey = logErrorLevel
            word.set(finalKey)
            context.write(word, one)
          case _ =>
      case _ =>
  }
}
