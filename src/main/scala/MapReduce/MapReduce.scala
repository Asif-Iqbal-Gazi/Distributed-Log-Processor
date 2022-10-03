package MapReduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang
import java.text.SimpleDateFormat
import scala.jdk.CollectionConverters.IterableHasAsScala

object MapReduce {

  class MyMapper extends Mapper[Object, Text, Text, IntWritable] {
    private final val one = new IntWritable(1)
    private val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val lines = value.toString.split(System.getProperty("line.separator")) // Not a good idea to split by ('\n') as log may contain '\n'
      val logRegex = "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}".r
      val startInterval = "20:54:28.015"
      val endInterval = "21:00:07.417"
      val dateFormat = new SimpleDateFormat("HH:mm:ss.SSS")
      lines.foreach(line => {
        line match {
          case logRegex => {
            val timeStamp = line.split(" ")(0)
            val errorLevel = line.split(" ")(2)
            val newTimeStamp = timeStamp.split('.')(0)
            System.out.println(newTimeStamp + ".00 - " + newTimeStamp + ".59" + "," + errorLevel)
            word.set(newTimeStamp+ ".00 - " + newTimeStamp + ".59" + "," + errorLevel)
            context.write(word, one)

            /* The description says "intervals"
            val formattedTimeStamp = dateFormat.parse(timeStamp)
            if (formattedTimeStamp.compareTo(dateFormat.parse(startInterval)) >= 0 && formattedTimeStamp.compareTo(dateFormat.parse(endInterval)) <= 0) {
              System.out.println(timeStamp + " " + errorLevel)
              word.set(timeStamp + " " + errorLevel)
              context.write(word, one)
            }
            */
          }
        }
      })
      //super.map(key, value, context)
    }
  }

  class MyReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Object, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      context.write(new Text(key.toString() + ","), new IntWritable(sum.get()))
      //super.reduce(key, values, context)
    }
  }


  @main def runMapReduce(inputPath: String, outputPath: String) =
    val conf = new Configuration
    val job = Job.getInstance(conf, "Word Count")
    job.setJarByClass(classOf[MyMapper])
    job.setMapperClass(classOf[MyMapper])
    job.setCombinerClass(classOf[MyReducer])
    job.setReducerClass(classOf[MyReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))
    job.submit()
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
