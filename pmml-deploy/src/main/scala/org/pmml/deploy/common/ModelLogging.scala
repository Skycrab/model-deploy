package org.pmml.deploy.common

import java.io.PrintStream
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang.exception.ExceptionUtils

/**
  * Created by yihaibo on 19/2/25.
  */
trait ModelLogging {
  val out: PrintStream = Console.out

  protected def logName = {
    this.getClass.getName.stripSuffix("$")
  }

  def now(): String = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
  }

  protected def logInfo(msg: => String) {
    out.println(s"$now INFO $logName $msg")
  }

  protected def logInfo(msg: => String, throwable: Throwable) {
    val e = ExceptionUtils.getFullStackTrace(throwable);
    out.println(s"$now INFO $logName $msg $e")
  }

  protected def logError(msg: => String) {
    out.println(s"$now ERROR $logName $msg")
  }

  protected def logError(msg: => String, throwable: Throwable) {
    val e = ExceptionUtils.getFullStackTrace(throwable);
    out.println(s"$now ERROR $logName $msg $e")
  }

}
