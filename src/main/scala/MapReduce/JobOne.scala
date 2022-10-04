package MapReduce

import HelperUtils.DetermineIntervals
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang
import java.text.SimpleDateFormat
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.matching.Regex

object JobOne {
  private val config: Config = ConfigFactory.load.getConfig("LogConfiguration")

  @main def runMapReduce(inputPath: String, outputPath: String): Unit =
    val conf = new Configuration
    conf.set("mapred.textoutputformat.separator", ",")
    val job = Job.getInstance(conf, "MapReduce")
    job.setJarByClass(classOf[MapperOne])
    job.setMapperClass(classOf[MapperOne])
    job.setCombinerClass(classOf[ReducerOne])
    job.setReducerClass(classOf[ReducerOne])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))
    job.submit()

  class MapperOne extends Mapper[Object, Text, Text, IntWritable] {
    private final val one = new IntWritable(1)
    private val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val line = value.toString
      // Get the log pattern of the log from config
      val logPattern = new Regex(config.getString("LogPattern"))
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
          //println(line)
          //println("Log Error Type " + logErrorLevel)
          //println("Injected String " + injectedString)
          // Check for injectedString match
          injectedString match
            case injectedStringRegex(_) =>
              // Found a match
              //println(timeStamp)
              //val keyPart1 = timeStamp.split('.')(0)
              // Constructing the key (e.g : "intervalStartTime - intervalEndTime,logEErrorLevel")
              val keyPart1 = DetermineIntervals.determineIntervals(timeStamp, dateFormat, interval)
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

  class ReducerOne extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      context.write(key, new IntWritable(sum.get()))
      //super.reduce(key, values, context)
    }
  }
  /*
    def main(args: Array[String]): Int = {
      val conf = new Configuration
      val job = Job.getInstance(conf, "Word Count")
      job.setJarByClass(classOf[MyMapper])
      job.setMapperClass(classOf[MyMapper])
      job.setCombinerClass(classOf[MyReducer])
      job.setReducerClass(classOf[MyReducer])
      job.setOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(job, new Path(args(0)))
      FileOutputFormat.setOutputPath(job, new Path(args(1)))
      if (job.waitForCompletion(true)) 0 else 1
    }
    */
}
