package MapReduce

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.matching.Regex

object JobFour {
  private val config: Config = ConfigFactory.load.getConfig("LogConfiguration")

  @main def runMapReduce4(inputPath: String, outputPath: String): Unit =
    val conf = new Configuration
    conf.set("mapred.textoutputformat.separator", ",")
    val job = Job.getInstance(conf, "MapReduce 4")
    job.setJarByClass(classOf[MapperFour])
    job.setMapperClass(classOf[MapperFour])
    job.setCombinerClass(classOf[ReducerFour])
    job.setReducerClass(classOf[ReducerFour])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))
    job.submit()

  class MapperFour extends Mapper[Object, Text, Text, IntWritable] {
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
              val finalValue: Int = injectedString.size
              word.set(finalKey)
              println("Key: " + finalKey + " Value: " + finalValue)
              context.write(word, new IntWritable(finalValue))
            case _ =>
        case _ =>
    }
  }

  class ReducerFour extends Reducer[Text, IntWritable, Text, IntWritable] {

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.reduce((valuesOne, valuesTwo) => new IntWritable(valuesOne.get() max valuesTwo.get()))
      context.write(key, new IntWritable(sum.get()))

    }
  }

}
