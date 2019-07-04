package org.pmml.deploy

import java.util.{HashMap => JMap}

import org.apache.spark.sql.DataFrame
import org.pmml.deploy.common.ModelLogging
import org.pmml.deploy.config.{Context, DeployConf, Tree}
import org.pmml.deploy.handler.Handler

import scala.collection.JavaConverters._

/**
  * Created by yihaibo on 19/2/28.
  */
class Schedule(val deployConf: DeployConf, val context: Context) extends ModelLogging{
  def schedule(): Unit = {
    val tree = deployConf.getTree
    schedule(tree, null)
  }

  def schedule(tree: Tree, dataFrame: DataFrame): DataFrame = {
    //union模式
    var df: DataFrame = tree.children.size() match {
      case 0 => null
      case 1 => schedule(tree.children.get(0), null)
      case _ => {
        val merge = tree.children.asScala.map(schedule(_, null)).reduce { (left, right) =>
          left.join(right, Seq(tree.joinKey), tree.joinType)
        }
        log("合并子节点", merge)
        merge
      }
    }
    df = processSingleNode(tree, df)

    //pipeline模式
    for(transformer <- tree.transformer.asScala) {
      df = processSingleNode(transformer, df)
    }
    df
  }

  def processSingleNode(tree: Tree, dataFrame: DataFrame): DataFrame = {
    logInfo(s"process ${tree.desc}")
    val handler = Handler(tree.name, tree.parameters.asScala.toMap)
    logInfo(s"process ${tree.desc} start, uid:${handler.uid}")
    handler.handler(dataFrame)
  }

  private def log(stage: String, dataFrame: DataFrame): Unit = {
    logInfo(stage)
    if(deployConf.appConf.debug) {
      dataFrame.show(2, false)
    }
  }
}

object Schedule {
  /**
    * @param argsAttr 外部传递的参数
    */
  def run(argsAttr: Map[String, String]): Unit = {
    val deployConf = DeployConf(argsAttr)
    val context = Context.create(deployConf)
    println(s"参数列表:${context.deployConf.argsAttr}")
    val schedule = new Schedule(deployConf, context)
    schedule.schedule()
  }
}