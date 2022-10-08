package com.asif.Mapper

import com.asif.HelperUtils.ComputeIntervals
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import scala.util.matching.Regex

class JobTwoMapper extends Mapper[Object, Text, Text, IntWritable] {
  private final val one = new IntWritable(1)
  private val config: Config = ConfigFactory.load.getConfig("LogConfiguration")
  private val word = new Text()

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    val line = value.toString
    // Get the log pattern of the log from config
    val logPattern = new Regex(config.getString("LogPattern"))
    // Get the desired logErrorLevel from config (For Task 2)
    val desiredLogErrorLevel = config.getString("LogErrorLevel")
    // Get the injected string pattern of the logs from config
    val injectedStringRegex = new Regex(config.getString("InjectedStringPattern"))
    // Get the date formatting information of the logs from config
    val dateFormat = config.getString("DateFormat")
    // Get the time interval information from config
    val interval: Int = config.getInt("Interval")

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
              val finalKey = ComputeIntervals.determineIntervals(timeStamp, dateFormat, interval)
              word.set(finalKey)
              context.write(word, one)
            case _ =>
        }
      case _ =>
    // No Match Found
    //super.map(key, value, context)
  }
}
