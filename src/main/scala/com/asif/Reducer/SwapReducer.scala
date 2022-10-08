package com.asif.Reducer

import com.asif.HelperUtils.CreateLogger
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.Logger

import java.lang
import scala.jdk.CollectionConverters.IterableHasAsScala

/**
 * This Reducer is Used by Task2 for Sorting the output in descending order by values.
 * It works in tandem with the SwapMapper
 * The SwapMapper swaps the key value pair of a previous MapReduce Job which had integer values. (By setting the new key as negative of previous Job's value)
 * This SwapReduces basically, reverses that process.
 * To do that, it is not straightforward as just swapping value and key, remember reducer gets list of values and
 * consider the situation when for same negative key we have multiple values.
 * To address this we first negate the key again to make it positive.
 * Now for all value(previous job's key) in values list, we write to the context(value, keyFromReducer)
 */
class SwapReducer extends Reducer[IntWritable, Text, Text, IntWritable] {
  val logger: Logger = CreateLogger(classOf[CommonReducer])

  override def reduce(key: IntWritable, values: lang.Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
    // Here we first make the key(value from previous Job) positive again
    val finalVal = new IntWritable(key.get() * -1)
    // Now, in previous job there could have been multiple Keys with same value, eg. "10:22:20 - 10:22:25, 15" & "18:30:15 - 18:30:20, 15"
    // Our 2nd Mapper set them to "-15, 10:22:20 - 10:22:25" & "-15, 18:30:15 - 18:30:20"
    // After shuffling- grouping phased(which happens based on keys) reducer received ki: List(v1, v2, ...., vj) e.g "-15 : (10:22:20 - 10:22:25, 18:30:15 - 18:30:20)"
    // So using for each we are swapping them again making (vj, -1 * ki) pair and writing to context
    values.asScala.foreach(value => {
      logger.debug(s"${this.getClass.getName}, writing to context:($value,$finalVal")
      // Here key is previous job's key
      context.write(value, finalVal)
    })
  }
}
