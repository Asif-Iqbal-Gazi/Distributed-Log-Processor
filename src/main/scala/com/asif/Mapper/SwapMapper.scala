package com.asif.Mapper

import com.asif.HelperUtils.CreateLogger
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.Logger

/**
 * This Mapper will take previous MapReduce Job's Output and swap key value
 * During swapping it will set the key as negative of previous value(they are integer of course).
 * I have observed that after MapReduce Job the output file contains everything sorted based on keys in ascending order
 * I am exploiting the above mentioned observation (look at SwapReducer for better understanding).
 */
class SwapMapper extends Mapper[Object, Text, IntWritable, Text] {
  val logger: Logger = CreateLogger(classOf[SwapMapper])

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
    logger.debug(s"${this.getClass.getName}, writing to context:($finalKey,$timeStamp)")
    context.write(new IntWritable(finalKey), new Text(timeStamp))
  }
}
