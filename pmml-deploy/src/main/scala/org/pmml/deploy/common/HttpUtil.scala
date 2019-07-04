package org.pmml.deploy.common

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization._

import scalaj.http._

/**
  * Created by yihaibo on 19/2/25.
  */
object HttpUtil extends ModelLogging {
  /**
    * get返回json请求
    * @param url 请求url
    * @param retry  重试次数
    * @param intervalMs 重试间隔时间
    * @param connTimeoutMs  连接超时毫秒
    * @param readTimeoutMs  读超时毫秒
    * @param formats 隐式参数，json format
    * @param mf 隐式参数，A的manifest
    * @tparam A 返回结果
    * @return
    */
  def getJson[A](url: String,
                 retry: Int=3,
                 intervalMs: Long=60000L,
                 connTimeoutMs: Int=2000,
                 readTimeoutMs: Int=60000)
                (implicit formats: Formats, mf: scala.reflect.Manifest[A]): Option[A] = {
    for(i <- 0 until retry) {
      try {
        val res = Http(url).timeout(connTimeoutMs, readTimeoutMs).asString
        res.code match {
          case 200 => {
            try {
              return Option(parse(res.body).extract[A])
            }catch {
              case e: Exception => logError(s"metric=HttpGetJsonParseFailed||url=$url||retry=$retry||body=${res.body}", e)
            }
          }
          case code => logError(s"metric=HttpGetJsonCodeFailed||url=$url||code=$code||retry=$retry||body=${res.body}")
        }
      }catch {
        case e: Exception => logError(s"metric=HttpGetJsonFailed||url=$url||retry=$retry", e)
      }
      Thread.sleep(intervalMs)
    }
    return None
  }

  /**
    * post json数据
    * @param url 请求url
    * @param obj 请求对象
    * @param retry  重试次数
    * @param intervalMs 重试间隔时间
    * @param connTimeoutMs  连接超时毫秒
    * @param readTimeoutMs  读超时毫秒
    * @param formats 隐式参数，json format
    * @tparam A 返回结果
    * @return
    */
  def postJson[A <: AnyRef](url: String,
                 obj: A,
                 retry: Int=3,
                 intervalMs: Long=60000L,
                 connTimeoutMs: Int=2000,
                 readTimeoutMs: Int=60000)
                (implicit formats: Formats): Option[String] = {
    val body = write(obj)
    for(i <- 0 until retry) {
      try {
        val res = Http(url).timeout(connTimeoutMs, readTimeoutMs)
          .postData(body)
          .header("content-type", "application/json")
          .asString
        res.code match {
          case 200 => return Option(res.body)
          case code => logError(s"metric=HttpPostJsonCodeFailed||url=$url||code=$code||retry=$retry||body=${res.body}")
        }
      }catch {
        case e: Exception => logError(s"metric=HttpPostJsonFailed||url=$url||retry=$retry||body=$body", e)
      }
      Thread.sleep(intervalMs)
    }
    return None
  }
}