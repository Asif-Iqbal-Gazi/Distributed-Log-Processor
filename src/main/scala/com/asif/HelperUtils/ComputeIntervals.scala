package com.asif.HelperUtils

import com.typesafe.config.{Config, ConfigFactory}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * This function determineIntervals is used by Tasks that requires to compute in which timeInterval a particular timeStamp will lie in.
 * This function takes in the timeStamp string.
 * Applies predefined DateFormat to the timeStamp to convert it to a SimpleDate Object
 * Then it converts that Date into epochTime (in millisecond)
 * Then it starting of the interval with the formula epochTime - (epochTime % (interval * 1000)), remember our interval is in seconds, multiplying it will 1000 converts to millisecond
 * Ending of the interval is startIntervalTime + interval * 100
 * It finally formats everything back returns to user interval string
 * 
 * e.g: For timeStamp : 09:01:14.477 and interval: 5 seconds
 * This function will return to user "09:01:10 - 09:01:15"
 */
object ComputeIntervals {
  private val config: Config = ConfigFactory.load.getConfig("LogConfiguration")
  // Get the date formatting information of the logs from config
  private final val dateFormat = config.getString("DateFormat")
  // Get the time interval information from config
  private final val interval: Int = config.getInt("Interval")
  def determineIntervals(timeStamp: String): String = {
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
