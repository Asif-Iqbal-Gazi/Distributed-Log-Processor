package MapReduce

import HelperUtils.DetermineIntervals
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.matching.Regex

object JobTwo {
  private val config: Config = ConfigFactory.load.getConfig("LogConfiguration")

  @main def runMapReduce2(inputPath: String, outputPath: String): Unit =
    val conf = new Configuration
    conf.set("mapred.textoutputformat.separator", ",")
    val job = Job.getInstance(conf, "MapReduce 2")
    job.setJarByClass(classOf[MapperTwo])
    job.setMapperClass(classOf[MapperTwo])
    job.setCombinerClass(classOf[ReducerTwo])
    job.setReducerClass(classOf[ReducerTwo])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath + "-temp"))
    job.submit()
    if (!job.waitForCompletion(true)) {
      System.exit(1)
    }

    val job2 = Job.getInstance(conf, "MapReduce 2-- Running Part 2")
    job2.setJarByClass(classOf[MapperTwo_2ndJob])
    job2.setMapperClass(classOf[MapperTwo_2ndJob])
    job2.setReducerClass(classOf[ReducerTwo_2ndJob])
    job2.setNumReduceTasks(1)
    job2.setMapOutputKeyClass(classOf[IntWritable])
    job2.setMapOutputValueClass(classOf[Text])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job2, new Path(outputPath + "-temp"))
    FileOutputFormat.setOutputPath(job2, new Path(outputPath))
    job2.submit()

  class MapperTwo extends Mapper[Object, Text, Text, IntWritable] {
    private final val one = new IntWritable(1)
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
                val finalKey = DetermineIntervals.determineIntervals(timeStamp, dateFormat, interval)
                word.set(finalKey)
                context.write(word, one)
              case _ =>
          }
        case _ =>
      // No Match Found
      //super.map(key, value, context)

    }
  }

  class ReducerTwo extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      context.write(key, new IntWritable(sum.get()))
    }
  }

  /* This Mapper will take previous MapReduce Job's Output and swap key value
     During swapping it will set the key as negative of previous value.
     Here I am exploiting the fact that, at the end of MapReduce Job keys are sorted in ascending order
  */
  class MapperTwo_2ndJob extends Mapper[Object, Text, IntWritable, Text] {
    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {
      val line = value.toString
      // We know output of map one will be comma separated
      // Get the time stamp
      val timeStamp = line.split(',')(0)
      // Get the count
      val count = line.split(',')(1).toInt
      //println("Time stamp: " + timeStamp + " Count: " + count)
      // Negate the count
      val finalKey = -1 * count
      //println("Final Key" + finalKey)
      // Writing to the context swapped key-value from previous Job
      context.write(new IntWritable(finalKey), new Text(timeStamp))
    }
  }

  class ReducerTwo_2ndJob extends Reducer[IntWritable, Text, Text, IntWritable] {
    override def reduce(key: IntWritable, values: lang.Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
      // Here we first make the key(value from previous Job) positive again
      val finalVal = new IntWritable(key.get() * -1)
      // Now, in previous job there could have been multiple Keys with same value, eg. "10:22:20 - 10:22:25, 15" & "18:30:15 - 18:30:20, 15"
      // Our 2nd Mapper set them to "-15, 10:22:20 - 10:22:25" & "-15, 18:30:15 - 18:30:20"
      // After shuffling- grouping phased(which happens based on keys) reducer received ki: List(v1, v2, ...., vj) e.g "-15 : (10:22:20 - 10:22:25, 18:30:15 - 18:30:20)"
      // So using for each we are swapping them again making (vj, -1 * ki) pair and writing to context
      values.asScala.foreach(value => {
        // Swapping key value
        context.write(value, finalVal)
      })
    }
  }
}
