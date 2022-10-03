package HelperUtils

import java.text.SimpleDateFormat
import java.util.Date

object DetermineIntervals {
  def determineIntervals(timeStamp: String, dateFormat: String, interval: Int): String = {
    val inputDateFormat = new SimpleDateFormat(dateFormat)
    val outputDateFormat = new SimpleDateFormat("HH:mm:ss")

    val epochTime: Long = inputDateFormat.parse(timeStamp).toInstant.toEpochMilli

    val startEpochTime: Long = epochTime - (epochTime % (interval * 1000))
    val endEpochTime: Long = startEpochTime + interval * 1000
    val startTime: Date = new Date(startEpochTime)
    val endTime: Date = new Date(endEpochTime)
    return outputDateFormat.format(startTime) + " - " + outputDateFormat.format(endTime)
  }
}
