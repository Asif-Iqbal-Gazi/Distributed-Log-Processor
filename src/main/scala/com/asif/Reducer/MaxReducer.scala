package com.asif.Reducer

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import java.lang
import scala.jdk.CollectionConverters.IterableHasAsScala

class MaxReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() max valueTwo.get()))
    context.write(key, new IntWritable(sum.get()))
    //super.reduce(key, values, context)
  }
}