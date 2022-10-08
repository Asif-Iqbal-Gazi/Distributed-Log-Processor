package com.asif.Mapper

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

/* This Mapper will take previous MapReduce Job's Output and swap key value
   During swapping it will set the key as negative of previous value.
   I have observed that after MapReduce Job the file contains everything sorted based on keys in ascending order
   I am exploiting the above mentioned observation.
*/
class SwapMapper extends Mapper[Object, Text, IntWritable, Text] {
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
