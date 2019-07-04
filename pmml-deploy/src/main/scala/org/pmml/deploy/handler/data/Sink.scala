package org.pmml.deploy.handler.data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField
import org.pmml.deploy.common.Util._
import org.pmml.deploy.handler.{ContextUtil, Handler, Name, ParameterHelper}

/**
  * Created by yihaibo on 19/3/5.
  */
@Name("data_sink")
class Sink(val parameters: Map[String, Any]) extends Handler with ParameterHelper with ContextUtil{
  val auto = getBoolean("auto")
  val path = getString("path")
  val db = getString("db")
  val table = getString("table")
  val tableName = getString("tableName")
  val ttl = getValAsOrDefault[Int]("ttl", 0)

  override def setUp(): Unit = {
    sql(s"use $db")
  }

  def sql(query: String): DataFrame = {
    logInfo(s"sink sql:\n$query")
    spark.sql(query)
  }

  def createTable(fields: Array[StructField]): Unit = {
    if(!auto) return
    if(spark.catalog.tableExists(db, table)) return

    val builder= new StringBuilder(addLine("CREATE EXTERNAL TABLE IF NOT EXISTS " + escape(table) + "("))
    val fieldInfo = fields.map{field: StructField => {
      val fieldType = field.dataType.simpleString
//      val fieldType = if(field.dataType.isInstanceOf[StringType]) "string" else "double"
      indentation(s"${escape(field.name)} $fieldType COMMENT ''")
    }}.mkString(",\n")
    builder.append(addLine(fieldInfo + ")"))
    builder.append(addLine("COMMENT " + quote(tableName)))
    builder.append(addLine("PARTITIONED BY ("))
    builder.append(addLine(indentation("`year` string,")))
    builder.append(addLine(indentation("`month` string,")))
    builder.append(addLine(indentation("`day` string)")))
    builder.append(addLine("ROW FORMAT SERDE"))
    builder.append(addLine(indentation(quote("org.apache.hadoop.hive.ql.io.orc.OrcSerde"))))
    builder.append(addLine("STORED AS INPUTFORMAT"))
    builder.append(addLine(indentation(quote("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"))))
    builder.append(addLine("OUTPUTFORMAT"))
    builder.append(addLine(indentation(quote("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"))))
    builder.append(addLine("LOCATION"))
    builder.append(addLine(indentation(quote("hdfs://DClusterNmg4/user/fbi/" + db + "/" + table))))
    builder.append(addLine("TBLPROPERTIES ("))
    //永久
    if(ttl == 0) {
      builder.append(addLine(indentation("'LEVEL'='0')")))
    }else {
      builder.append(addLine(indentation("'LEVEL'='1',")))
      builder.append(addLine(indentation(s"'TTL'='$ttl')")))
    }

    sql(builder.toString())
  }

  override def transform(df: DataFrame): DataFrame = {
    createTable(df.schema.fields)
    val tmpTable = s"table_$uid"
    df.createOrReplaceTempView(tmpTable)
    val (year, month, day) = (getArgs("year"), getArgs("month"), getArgs("day"))

    sql(s"alter table $table add if not exists partition(year='${year}',month='${month}',day='${day}') location '${year}/${month}/${day}'")
    sql(s"insert overwrite table $table partition(year='${year}',month='${month}',day='${day}') select * from $tmpTable")
    null
  }
}
