package org.pmml.deploy.handler

import java.io.{File, FileInputStream}
import java.util.{Objects, HashMap => JMap, List => JList}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame
import org.pmml.deploy.DeployException
import org.pmml.deploy.common.{Magic, ModelLogging, Util}
import org.pmml.deploy.config.Context
import org.reflections.Reflections

import scala.annotation.StaticAnnotation
import scala.collection.JavaConverters._


/**
  * Created by yihaibo on 19/3/5.
  */
trait Handler extends ModelLogging{
  val parameters: Map[String, Any]

  val uid = Util.uuid

  val spark = Context.get().sparkSession

  def handler(df: DataFrame): DataFrame = {
    setUp()
    val result = transform(df)
    tearDown(result)
    result
  }

  def transform(df: DataFrame): DataFrame

  def setUp(): Unit = {

  }

  def tearDown(df: DataFrame): Unit = {
    val deployConf = Context.get().deployConf
    val appConf = deployConf.appConf
    if(appConf.debug && Objects.nonNull(df)) {
      logInfo(s"tearDown $uid show")
      df.show(2, false)
      val writeDf = if(appConf.limit > 0) df.limit(appConf.limit) else df
      val path = appConf.savePath + deployConf.scheduleDay + "/" + uid + ".parquet"
      logInfo(s"save path:$path")
      if(!deployConf.isDevMode) {
        writeDf.write.mode("overwrite").parquet(path)
      }
    }
  }
}

class Name(name:String) extends StaticAnnotation


object Handler extends ModelLogging{
  private[handler] val packageName = "org.pmml.deploy.handler"

  private[handler] val handlers = {
    val reflections = new Reflections(packageName)
    val handlers = new JMap[String, Class[_<:Handler]]()
    reflections.getSubTypesOf(classOf[Handler]).asScala.foreach{ sub =>
      val name = Magic.getHandlerName(sub)
      if(name.isDefined) {
        handlers.put(name.get, sub)
      }else{
        println(s"warning: 组件${sub}未设置name")
      }
    }
    if(Context.get.deployConf.isDevMode) {
      println("\n检查到如下组件:")
      handlers.asScala.foreach(println)
    }
    handlers
  }

  def apply(name: String, parameters:Map[String, Any]): Handler = {
    val handlerName = StringUtils.trim(name)
    logInfo(s"组件:$handlerName,参数:$parameters")
    if(!handlers.containsKey(handlerName)) {
      logError(s"$handlerName 组件缺失!!!!!!")
      throw new DeployException(s"未知组件:$handlerName")
    }
    handlers.get(handlerName).getConstructor(classOf[Map[String, Any]]).newInstance(parameters)
  }
}

trait ParameterHelper {
  val parameters: Map[String, Any]

  def hasParameter(key: String): Boolean = {
    parameters.contains(key)
  }

  def getString(key: String): String = {
    getValAs[String](key)
  }

  def getBoolean(key: String): Boolean = {
    getValAs[Boolean](key)
  }

  def getDouble(key: String): Double = {
    getValAs[Double](key)
  }

  def getFloat(key: String): Float = {
    getValAs[Float](key)
  }

  def getInt(key: String): Int = {
    getValAs[Int](key)
  }

  def getLong(key: String): Long = {
    getValAs[Long](key)
  }

  def getJList[T](key: String): JList[T] = {
    getValAs[JList[T]](key)
  }

  def getList[T](key: String): List[T] = {
    getValAs[JList[T]](key).asScala.toList
  }

  def getListOrDefault[T](key: String, default:List[T]): List[T] = {
    if(hasParameter(key)) {
      parameters.get(key).get.asInstanceOf[JList[T]].asScala.toList
    }else {
      default
    }
  }

  def getValAs[T](key: String): T = {
    if(!parameters.contains(key)) {
      throw new DeployException(s"$key not exist in $parameters")
    }
    parameters.get(key).get.asInstanceOf[T]
  }

  def getValAsOrDefault[T](key: String, default: T): T = {
    if(hasParameter(key)) {
      parameters.get(key).get.asInstanceOf[T]
    }else{
      default
    }
  }
}


trait ContextUtil {
  def localReadRender(path: String): String = {
    val content = Util.inputStreamToString(new FileInputStream(new File(path)))
    Util.stringRender(content, Context.get().deployConf.argsAttr)
  }

  def getArgs(key: String): String = {
    Context.get().deployConf.argsAttr.getOrElse(key, "")
  }

  val sourceSavePath = {
    val path = "{sourcePath}{year}/{month}/{day}/"
    val attr = Context.get().deployConf.argsAttr + ("sourcePath" -> Context.get().deployConf.appConf.sourcePath)
    Util.stringRender(path, attr)
  }

}