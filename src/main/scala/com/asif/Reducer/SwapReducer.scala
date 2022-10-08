package com.asif.Reducer

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import java.lang
import scala.jdk.CollectionConverters.IterableHasAsScala

class SwapReducer extends Reducer[IntWritable, Text, Text, IntWritable] {
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
