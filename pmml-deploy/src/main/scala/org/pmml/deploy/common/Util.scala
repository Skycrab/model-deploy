package org.pmml.deploy.common

import java.io.InputStream
import java.util.UUID

import org.apache.commons.lang3.StringUtils
import org.clapper.scalasti.ST

/**
  * Created by yihaibo on 19/2/25.
  */
object Util {
  /**
    * 字符串渲染
    * @param template  where concat(year,month,day)={year}{month}{day}
    * @param attr
    * @return
    */
  def stringRender(template: String, attr: Map[String, String]): String = {
    ST
    val t = ST(template, '{', '}').addAttributes(attr)
    return t.render().getOrElse("")
  }

  def inputStreamToString(inputStream: InputStream): String = {
    scala.io.Source.fromInputStream(inputStream).getLines().mkString("\n")
  }

  def uuid(): String = {
    UUID.randomUUID().toString.replace("-", "");
  }

  def addLine(value: String) = value + "\n"

  def indentation(value: String, indent: Int) = StringUtils.repeat("  ", indent) + value

  def indentation(value: String):String = indentation(value, 1)

  def quote(value: String) = "'" + value + "'"

  def escape(field: String) = "`" + field + "`"

}
