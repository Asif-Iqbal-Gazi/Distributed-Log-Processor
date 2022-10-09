package TestModule

import com.asif.HelperUtils.ComputeIntervals
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.matching.Regex

class HelperUtilsTest extends AnyFlatSpec with Matchers {

  behavior of "HelperUtils and Configuration Parameters"
  val config: Config = ConfigFactory.load("application.conf").getConfig("LogConfiguration")

  it should "LogPattern" in {
    config.getString("LogPattern") should be
    "(\\d{2}\\:\\d{2}\\:\\d{2}\\.\\d{3})\\s(\\[.+\\])\\s(ERROR|WARN|DEBUG|INFO)\\s+(.*)\\s\\-\\s(.+)"
  }

  it should "dateFormat" in {
    config.getString("DateFormat") should be
    "HH:mm:ss.SSS"
  }

  it should "Compute TimeIntervals" in {
    val computedInterval = ComputeIntervals.determineIntervals("04:28:31.154")
    val correctInterval = "04:25:00 - 04:30:00"
    computedInterval should be(correctInterval)
  }

  it should "Match timeStamp from LogPattern" in {
    val sampleLog = "09:01:14.477 [scala-execution-context-global-12] INFO  com.asif.HelperUtils.Parameters$ - Pf|lrfh@j-oz~W"
    val LogPattern = new Regex(config.getString("LogPattern"))
    sampleLog match
      case LogPattern(timeStamp, _*) =>
        assert(timeStamp.equals("09:01:14.477"))
  }


  it should "Match logErrorLevel from LogPattern" in {
    val sampleLog = "09:01:14.477 [scala-execution-context-global-12] INFO  com.asif.HelperUtils.Parameters$ - Pf|lrfh@j-oz~W"
    val LogPattern = new Regex(config.getString("LogPattern"))
    sampleLog match {
      case LogPattern(_, _, extractedLogErrorLevel, _, _) =>
        assert(extractedLogErrorLevel.equals("INFO"))
      case _ =>
        assert(false)
    }
  }

  it should "Match injectedString from InjectedStringPattern" in {
    val sampleInjectedString = "z|tEyAbg0ae0be3cg0H9vcf1o'Uxmv"
    val injectedStringPattern = new Regex(config.getString("InjectedStringPattern"))
    val allMatch = injectedStringPattern.findAllIn(sampleInjectedString).toList
    val count = allMatch.size
    assert(count.equals(1))
  }

}
