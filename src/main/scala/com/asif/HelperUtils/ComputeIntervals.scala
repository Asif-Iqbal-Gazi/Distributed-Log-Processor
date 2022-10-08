package com.asif.HelperUtils

import java.text.SimpleDateFormat
import java.util.Date

object ComputeIntervals {
  def determineIntervals(timeStamp: String, dateFormat: String, interval: Int): String = {
    // Setting the date format for intervalStartTime
    val inputDateFormat = new SimpleDateFormat(dateFormat)
    // Setting the date format for intervalEndTime
    val outputDateFormat = new SimpleDateFormat("HH:mm:ss")
    // Getting the epochTime from our time stamp
    val epochTime: Long = inputDateFormat.parse(timeStamp).toInstant.toEpochMilli
    //println(epochTime)
    // Computing the startEpochTime
    val startEpochTime: Long = epochTime - (epochTime % (interval * 1000))
    // Computing the endEpochTime
    val endEpochTime: Long = startEpochTime + interval * 1000
    // convert both the start & end Time to date, so that we can format them
    val startTime: Date = new Date(startEpochTime)
    val endTime: Date = new Date(endEpochTime)
    outputDateFormat.format(startTime) + " - " + outputDateFormat.format(endTime)
  }
}
