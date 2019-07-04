package org.pmml.deploy.handler.data

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame
import org.pmml.deploy.DeployException
import org.pmml.deploy.handler.{ContextUtil, Handler, Name, ParameterHelper}

/**
  * Created by yihaibo on 19/3/5.
  */
@Name("data_source")
class Source(val parameters: Map[String, Any]) extends Handler with ParameterHelper with ContextUtil{

  override def transform(dataFrame: DataFrame): DataFrame = {
    val sourceType = getString("type")
    val df = sourceType match {
      case "hql" =>
        val query = getString("query")
        logInfo(query)
        spark.sql(query)
      case "hql_file" =>
        val query = localReadRender(getString("path"))
        logInfo(query)
        spark.sql(query)
      case "json" =>
        spark.read.format("json")
          .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
          .load(getString("path"))
      case "parquet" =>
        spark.read.parquet(getString("path"))

      case _ =>
        throw new DeployException(s"data_source not support type:$sourceType")
    }

    val saveTable = getValAsOrDefault[String]("saveTable", "")
    if(StringUtils.isNoneBlank(saveTable)) {
      val path = sourceSavePath + saveTable + ".parquet"
      logInfo(s"source save:$path")
      df.write.mode("overwrite").parquet(path)
      spark.read.parquet(path)
    }else {
      df
    }
  }
}
