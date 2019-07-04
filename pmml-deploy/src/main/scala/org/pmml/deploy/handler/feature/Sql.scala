package org.pmml.deploy.handler.feature

import org.apache.spark.sql.DataFrame
import org.pmml.deploy.handler.{Handler, Name, ParameterHelper}

/**
  * Created by yihaibo on 19/3/5.
  * 自定义sql组件
  */
@Name("feature_sql")
class Sql(val parameters: Map[String, Any]) extends Handler with ParameterHelper{

  override def transform(df: DataFrame): DataFrame = {
    val sql = getString("sql")
    val tmpTable = s"table_$uid"
    df.createOrReplaceTempView(tmpTable)
    val query = s"$sql from $tmpTable"
    logInfo(s"$uid sql:$query")
    spark.sql(query)
  }
}
