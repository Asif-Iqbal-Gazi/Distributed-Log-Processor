package com.asif

import com.asif.HelperUtils.CreateLogger
import com.asif.Mapper.{JobFourMapper, JobOneMapper, JobThreeMapper, JobTwoMapper, SwapMapper}
import com.asif.Reducer.{CommonReducer, MaxReducer, SwapReducer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.Logger

import java.math.BigInteger
import java.util.Properties
import java.{lang, util}

class MapReduceDriver {

}

object MapReduceDriver {
  val logger: Logger = CreateLogger(classOf[MapReduceDriver])

  def main(args: Array[String]): Unit = {
    logger.info("Staring LogFileProcessor...")

    // Input validation
    logger.info("Validating input...")
    if (args.length != 3 || args(2).toInt > 4 && args(2).toInt < 1) {
      logger.error("usage: <application_jar> <input_path> <output_path> <jobNo>")
      System.exit(1)
    }

    // Extracting Arguments
    val jobOption = args(2)
    val inputPath = args(0)
    val outputPath = args(1)
    logger.info("Extracting Arguments...")
    logger.info(s"Selected Job number: $jobOption")
    logger.info(s"Selected Input Path: $inputPath")
    logger.info(s"Selected Input Path: $outputPath")

    // Setting the common variable
    val conf = new Configuration
    conf.set("mapred.textoutputformat.separator", ",") // We need the MapReduce output Comma-Seperated

    args(2) match
      case "1" =>
        val job = Job.getInstance(conf, "MapReduce Task 1")
        job.setJarByClass(this.getClass)
        job.setMapperClass(classOf[JobOneMapper])
        job.setCombinerClass(classOf[CommonReducer])
        job.setReducerClass(classOf[CommonReducer])
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[IntWritable])
        FileInputFormat.addInputPath(job, new Path(inputPath))
        FileOutputFormat.setOutputPath(job, new Path(outputPath))
        job.submit()
      case "2" =>
        val job = Job.getInstance(conf, "MapReduce Task 2")
        job.setJarByClass(classOf[JobTwoMapper])
        job.setMapperClass(classOf[JobTwoMapper])
        job.setCombinerClass(classOf[CommonReducer])
        job.setReducerClass(classOf[CommonReducer])
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[IntWritable])
        FileInputFormat.addInputPath(job, new Path(inputPath))
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "-temp"))
        job.submit()
        if (!job.waitForCompletion(true)) {
          System.exit(1)
        }

        val job2 = Job.getInstance(conf, "MapReduce 2-- Running Part 2")
        job2.setJarByClass(classOf[SwapMapper])
        job2.setMapperClass(classOf[SwapMapper])
        job2.setReducerClass(classOf[SwapReducer])
        job2.setNumReduceTasks(1)
        job2.setMapOutputKeyClass(classOf[IntWritable])
        job2.setMapOutputValueClass(classOf[Text])
        job2.setOutputKeyClass(classOf[Text])
        job2.setOutputValueClass(classOf[IntWritable])
        FileInputFormat.addInputPath(job2, new Path(outputPath + "-temp"))
        FileOutputFormat.setOutputPath(job2, new Path(outputPath))
        job2.submit()
      case "3" =>
        val job = Job.getInstance(conf, "MapReduce  Task 3")
        job.setJarByClass(classOf[JobThreeMapper])
        job.setMapperClass(classOf[JobThreeMapper])
        job.setCombinerClass(classOf[CommonReducer])
        job.setReducerClass(classOf[CommonReducer])
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[IntWritable])
        FileInputFormat.addInputPath(job, new Path(inputPath))
        FileOutputFormat.setOutputPath(job, new Path(outputPath))
        job.submit()
      case "4" =>
        val job = Job.getInstance(conf, "MapReduce Task 4")
        job.setJarByClass(classOf[JobFourMapper])
        job.setMapperClass(classOf[JobFourMapper])
        job.setCombinerClass(classOf[MaxReducer])
        job.setReducerClass(classOf[MaxReducer])
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[IntWritable])
        FileInputFormat.addInputPath(job, new Path(inputPath))
        FileOutputFormat.setOutputPath(job, new Path(outputPath))
        job.submit()
  }
}
