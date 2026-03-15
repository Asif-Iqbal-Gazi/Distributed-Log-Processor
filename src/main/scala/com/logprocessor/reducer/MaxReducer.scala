package com.logprocessor.reducer

import com.logprocessor.utils.CreateLogger
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.Logger

import java.lang
import scala.jdk.CollectionConverters.IterableHasAsScala

/**
 * This Reducer is Used by Task4
 * For a key it reduces the value map/list by returning the max value from all the values.
 * Say, one input is ki : List(v1, v2, v3, ..., vj)
 * It basically, reduces for the key ki : max_of(v1,v2,v3,...vj)
 * Finally, writes that to the context
 */
class MaxReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  val logger: Logger = CreateLogger(classOf[MaxReducer])

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() max valueTwo.get()))
    logger.debug(s"${this.getClass.getName}, writing to context:($key,${sum.get()}")
    context.write(key, new IntWritable(sum.get()))
  }
}
