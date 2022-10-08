package com.asif.Mapper

import com.asif.HelperUtils.{ComputeIntervals, CreateLogger}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.Logger

import scala.util.matching.Regex

/** This Mapper is used by Task 1.
 * It will first load configuration file to determine the pattern of the log message (the entire message) & injected string's regex
 * Then using regex it will first check, if the line matches with the log pattern(the entire log message e.g: "09:01:11.455 [scala-execution-context-global-12] DEBUG com.asif.HelperUtils.Parameters$ - A~9Md_CUb,0")
 * If matched, then it extracts injectedString(for above example that will be "A~9Md_CUb,0"), timeStamp("09:01:11.455"), logErrorLevel("DEBUG")
 * Next it checks if the injectedString matches injectedStringRegex pattern. (Note: the regEx pattern is un-anchored type, so if a match occurs anywhere, we consider that)
 * If above match found, it construct keyPart1 by calling determineInterval utility function, and keyPart2 = logErrorLevel
 * Finally, it writes to the context.write(key, 1)
 */
class JobOneMapper extends Mapper[Object, Text, Text, IntWritable] {
  private final val one = new IntWritable(1)
  val logger: Logger = CreateLogger(classOf[JobOneMapper])
  private val config: Config = ConfigFactory.load.getConfig("LogConfiguration")
  private val word = new Text()

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    val line = value.toString
    // Get the log pattern of the log from config
    val logPattern = new Regex(config.getString("LogPattern"))
    // Get the injected string pattern of the logs from config
    val injectedStringRegex = new Regex(config.getString("InjectedStringPattern")).unanchored

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
            //println(injectedString)
            //println(timeStamp)
            //val keyPart1 = timeStamp.split('.')(0)
            // Constructing the key (e.g : "intervalStartTime - intervalEndTime,logEErrorLevel")
            val keyPart1 = ComputeIntervals.determineIntervals(timeStamp)
            //println(keyPart1)
            val keyPart2 = logErrorLevel
            word.set(keyPart1 + "," + keyPart2)
            logger.debug(s"${this.getClass.getName}, writing to context:($word,$one)")
            //println(word.toString + " " + one.toString)
            context.write(word, one)
          case _ =>
      case _ =>
    // No Match Found
    //super.map(key, value, context)
  }
}



