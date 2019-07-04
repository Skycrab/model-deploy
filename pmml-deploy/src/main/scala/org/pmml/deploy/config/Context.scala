package org.pmml.deploy.config

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SparkSession
import org.pmml.deploy.common.ModelLogging

import collection.JavaConversions._

/**
  * Created by yihaibo on 19/2/28.
  */
class Context(val deployConf: DeployConf) {
  lazy val sparkSession = {
    val sparkConf = deployConf.getSparkConf
    val builder = SparkSession.builder.appName(sparkConf.appName)
    if(StringUtils.isNotBlank(sparkConf.master)) {
      builder.master(sparkConf.master)
    }
    deployConf.getSparkConf.getConf.map{case (k, v) => builder.config(k, v)}
    if(sparkConf.enableHiveSupport) {
      builder.enableHiveSupport
    }

    val spark = builder.getOrCreate()
    spark
  }

}

object Context extends ModelLogging{
  @volatile
  private var context: Context = _

  def set(ctx: Context): Unit = {
    context = ctx
  }

  def get(): Context = {
    context
  }

  def create(deployConf: DeployConf): Context = {
    val ctx = new Context(deployConf)
    set(ctx)
    ctx
  }
}


