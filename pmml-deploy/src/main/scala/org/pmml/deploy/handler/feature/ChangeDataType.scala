package org.pmml.deploy.handler.feature

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.pmml.deploy.handler.{Handler, Name, ParameterHelper}

/**
  * Created by yihaibo on 19/3/5.
  * 数据类型转换
  */
@Name("feature_data_type")
class ChangeDataType(val parameters: Map[String, Any]) extends Handler with ParameterHelper{

  override def transform(df: DataFrame): DataFrame = {
    logInfo("previous schema")
    df.printSchema()
    val originalType = getList[String]("originalType")
    val exceptColumn = getListOrDefault("exceptColumn", List())

    //track decimal(19,6)
    val changeColumns = df.schema.fields.filter { s =>
      val key = s.dataType.simpleString.split("\\(", 2).head
      originalType.contains(key) && !exceptColumn.contains(s.name)
    }.map(_.name)
    val targetType = getString("targetType")
    logInfo(s"change columns ${changeColumns.toList}, target:$targetType")

    val result = df.select(df.columns.map {
      name => if (changeColumns.contains(name)) col(name).cast(targetType) else col(name)
    }: _*)
    logInfo("change schema")
    result.printSchema()
    result
  }
}
