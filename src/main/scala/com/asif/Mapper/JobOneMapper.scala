package com.asif.Mapper

import com.asif.HelperUtils.ComputeIntervals
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import scala.util.matching.Regex


class JobOneMapper extends Mapper[Object, Text, Text, IntWritable] {
  private final val one = new IntWritable(1)
  private val config: Config = ConfigFactory.load.getConfig("LogConfiguration")
  private val word = new Text()

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    val line = value.toString
    // Get the log pattern of the log from config
    val logPattern = new Regex(config.getString("LogPattern"))
    // Get the injected string pattern of the logs from config
    val injectedStringRegex = new Regex(config.getString("InjectedStringPattern")).unanchored
    // Get the date formatting information of the logs from config
    val dateFormat = config.getString("DateFormat")
    // Get the time interval information from config
    val interval: Int = config.getInt("Interval")

    // Check if the line matches our log pattern
    line match
      case logPattern(timeStamp, _, logErrorLevel, _, injectedString) =>
        // Found a match
        //println(line)
        //println("Log Error Type " + logErrorLevel)
        //println("Injected String " + injectedString)
        // Check for injectedString match
        injectedString match
          case injectedStringRegex(_) =>
            // Found a match
            println(injectedString)
            //println(timeStamp)
            //val keyPart1 = timeStamp.split('.')(0)
            // Constructing the key (e.g : "intervalStartTime - intervalEndTime,logEErrorLevel")
            val keyPart1 = ComputeIntervals.determineIntervals(timeStamp, dateFormat, interval)
            //println(keyPart1)
            val keyPart2 = logErrorLevel
            word.set(keyPart1 + "," + keyPart2)
            //println(word.toString + " " + one.toString)
            context.write(word, one)
          case _ =>
      case _ =>
    // No Match Found
    //super.map(key, value, context)
  }
}



