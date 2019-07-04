package org.pmml.deploy.common

import scala.reflect.runtime.{universe => ru}

/**
  * Created by yihaibo on 19/3/5.
  */
object Magic extends ModelLogging{
  private val NamePattern = "\"(.*?)\"".r
  /**
    * class转为Type
    */
  def getType[T](clazz: Class[T]): ru.Type = {
    val runtimeMirror =  ru.runtimeMirror(clazz.getClassLoader)
    runtimeMirror.classSymbol(clazz).toType
  }

  /**
    * 获取Handler Name注解的名字
    */
  def getHandlerName[T](clazz: Class[T]): Option[String] = {
    try{
      val annotation = getType(clazz).typeSymbol.asClass.annotations
      if(annotation.isEmpty) {
        return None
      }
      val m = NamePattern.findFirstMatchIn(annotation(0).tree.toString())
      if(m.isDefined) {
        return Option(m.get.group(1))
      }
    }catch {
      case e: Exception => {
        logError(s"获取注解失败:$clazz", e)
      }
    }
    return None
  }
}
