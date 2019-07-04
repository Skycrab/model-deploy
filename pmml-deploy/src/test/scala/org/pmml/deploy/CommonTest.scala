package org.pmml.deploy

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{DecimalType, DoubleType}
import org.pmml.deploy.common.{Magic, Util}
import org.pmml.deploy.handler.feature.{Sql, Woe}
import org.scalatest._

/**
  * Created by yihaibo on 19/2/25.
  */
class CommonTest extends FlatSpec with Matchers{
  it should "equal spark" in {
    "Spark".toLowerCase should be ("spark")
  }

  it should "extract ok" in {
    val m = Util.stringRender("where concat(year,month,day)={year}{month}{day}", Map("yaer" -> "2018"))
    print(m+"1111")

    println(Magic.getHandlerName(classOf[Sql]))

    println(Magic.getHandlerName(classOf[Woe]))

    println(DoubleType.json)

    println(CatalystSqlParser.parseDataType("double"))

    val f = CatalystSqlParser.parseDataType("decimal(19,6)")
    println(f.isInstanceOf[DecimalType])




  }

}
