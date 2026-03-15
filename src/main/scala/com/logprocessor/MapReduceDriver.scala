package com.logprocessor

import com.logprocessor.utils.CreateLogger
import com.logprocessor.mapper.*
import com.logprocessor.reducer.{SumReducer, MaxReducer, SortingReducer}
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
    if (args.length != 3 || args(0).toInt > 4 && args(0).toInt < 1) {
      logger.error("usage: <application_jar> <jobNo> <input_path> <output_path>")
      System.exit(1)
    }

    // Extracting Arguments
    val jobOption = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    logger.info("Extracting Arguments...")
    logger.info(s"Selected Job number: $jobOption")
    logger.info(s"Selected Input Path: $inputPath")
    logger.info(s"Selected Output Path: $outputPath")

    // Setting the common variable (Idea is: If any task needs anything different, we will overwrite it there)
    val conf = new Configuration
    //conf.set("mapred.textoutputformat.separator", ",") <-- Deprecated
    conf.set("mapreduce.output.textoutputformat.separator", ",") // We need the MapReduce output Comma-Seperated
    val job = Job.getInstance(conf, "MapReduce Tasks")
    job.setJarByClass(this.getClass)
    job.setCombinerClass(classOf[SumReducer])
    job.setReducerClass(classOf[SumReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    args(0) match
      case "1" =>
        logger.info("Setting Configuration for Task 1...")
        job.setJobName("MapReduce Task 1")
        // Overwrite the Mapper Class for Task 1
        job.setMapperClass(classOf[TimeIntervalDistributionMapper])
        FileInputFormat.addInputPath(job, new Path(inputPath))
        FileOutputFormat.setOutputPath(job, new Path(outputPath))
        System.exit(if (job.waitForCompletion(true)) 0 else 1)
      case "2" =>
        logger.info("Setting Configuration for Task 2_Part 1...")
        job.setJobName("MapReduce Task 2 -- Part 1")
        // Overwrite Mapper Class for Task 2 Part 1
        job.setMapperClass(classOf[FilteredLogLevelMapper])
        FileInputFormat.addInputPath(job, new Path(inputPath))
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "-temp"))
        if (!job.waitForCompletion(true)) {
          System.exit(1)
        }
        logger.info("Setting Configuration for Task 2_Part 2...")
        val job2 = Job.getInstance(conf, "MapReduce 2 -- Part 2")
        job2.setJarByClass(classOf[SortingMapper])
        job2.setMapperClass(classOf[SortingMapper])
        job2.setReducerClass(classOf[SortingReducer])
        // We only need one Reducer for part <--- No effect on AWS though
        job2.setNumReduceTasks(1)
        job2.setMapOutputKeyClass(classOf[IntWritable])
        job2.setMapOutputValueClass(classOf[Text])
        job2.setOutputKeyClass(classOf[Text])
        job2.setOutputValueClass(classOf[IntWritable])
        FileInputFormat.addInputPath(job2, new Path(outputPath + "-temp"))
        FileOutputFormat.setOutputPath(job2, new Path(outputPath))
        System.exit(if (job2.waitForCompletion(true)) 0 else 1)
      case "3" =>
        logger.info("Setting Configuration for Task 3...")
        job.setJobName("MapReduce Task 3")
        job.setMapperClass(classOf[LogTypeCountMapper])
        FileInputFormat.addInputPath(job, new Path(inputPath))
        FileOutputFormat.setOutputPath(job, new Path(outputPath))
        System.exit(if (job.waitForCompletion(true)) 0 else 1)
      case "4" =>
        logger.info("Setting Configuration for Task 4...")
        job.setJobName("MapReduce Task 4")
        // Setting the MapperClass for Task 4
        job.setMapperClass(classOf[MaxCharLengthMapper])
        // Overwrite the CombinerClass to "MaxReducer"
        job.setCombinerClass(classOf[MaxReducer])
        // Overwrite the ReducerClass to "MaxReducer"
        job.setReducerClass(classOf[MaxReducer])
        FileInputFormat.addInputPath(job, new Path(inputPath))
        FileOutputFormat.setOutputPath(job, new Path(outputPath))
        System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}
