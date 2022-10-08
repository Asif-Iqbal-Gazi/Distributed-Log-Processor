package com.asif.Reducer

import com.asif.HelperUtils.CreateLogger
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.Logger

import java.lang
import scala.jdk.CollectionConverters.IterableHasAsScala

/**
 * This Reducer is Used by Task1, Task2, Task 3
 * For a key it reduces the value map/list by summing all the values.
 * Say one input is ki : List(v1, v2, v3, ..., vj)
 * It basically, reduces for the key ki : sum(v1,v2,v3,...vj)
 * Finally, writes that to the context
 */
class CommonReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  val logger: Logger = CreateLogger(classOf[CommonReducer])

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
    logger.debug(s"${this.getClass.getName}, writing to context:($key,${sum.get()}")
    context.write(key, new IntWritable(sum.get()))
    //super.reduce(key, values, context)
  }
}

