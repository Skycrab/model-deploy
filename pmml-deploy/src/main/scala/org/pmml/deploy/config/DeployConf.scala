package org.pmml.deploy.config

import java.io.{File, FileInputStream}
import java.util.{Properties, ArrayList => JList, HashMap => JMap}

import org.apache.log4j.{Level, Logger}
import org.pmml.deploy.common.{ModelLogging, Util}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.beans.BeanProperty

/**
  * Created by yihaibo on 19/2/28.
  */
class DeployConf {
  /**
    * 是否测试环境
    */
  var isDevMode = false

  /**
    * 命令行参数
    */
  var argsAttr = Map[String, String]()

  /**
    * 调度时间
    */
  var scheduleDay = ""

  /**
    * spark配置信息
    */
  @BeanProperty
  var sparkConf = new SparkConf()

  @BeanProperty
  var appConf = new AppConf()

  /**
    * 执行图
    */
  @BeanProperty
  var tree = new Tree()

}

class SparkConf {
  @BeanProperty
  var appName = "pmml-deploy"

  @BeanProperty
  var master = ""

  @BeanProperty
  var enableHiveSupport = true

  @BeanProperty
  var conf = new JMap[String, String]()
}

class AppConf {
  /**
    * debug开启时，每个节点会做持久化
    */
  @BeanProperty
  var debug = false

  /**
    * 持久化数量，0代表全量
    */
  @BeanProperty
  var limit = 0

  /**
    * 中间结果路径
    */
  @BeanProperty
  var savePath = ""

  /**
    * 数据源结果保存路径
    */
  @BeanProperty
  var sourcePath = ""
}

class Tree {
  @BeanProperty
  var desc = ""

  @BeanProperty
  var name = ""

  @BeanProperty
  var parameters = new JMap[String, Any]()

  /**
    * 子节点join类型
    */
  @BeanProperty
  var joinType = "full"

  @BeanProperty
  var joinKey = "uid"

  @BeanProperty
  var children = new JList[Tree]()

  @BeanProperty
  var transformer = new JList[Tree]()
}

object DeployConf extends ModelLogging {
  private[config] val yamlName = "application.yaml"
  private[config] val propertiesName = "application.properties"
  private[config] val modeId = "mode"
  private[config] val devModeName = "dev"

  def apply(argsAttr: Map[String, String]): DeployConf = {
    //读取profile配置属性
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream(propertiesName))
    val mode = properties.getProperty(modeId)
    logInfo(s"run mode:$mode")
    val isDevMode = mode == devModeName

    if(isDevMode) {
      Logger.getLogger("org").setLevel(Level.ERROR)
    }

    val yamlContent = Util.stringRender(readYamlConfig(isDevMode), argsAttr)
    val deployConf = buildDeployConf(yamlContent)
    deployConf.isDevMode = isDevMode
    deployConf.argsAttr = argsAttr
    deployConf.scheduleDay = argsAttr.get("scheduleDay").get
    deployConf
  }

  /**
    * 读取application配置内容
    */
  private def readYamlConfig(isDevMode: Boolean): String = {
    val yamlInput = if(isDevMode) {
      getClass.getClassLoader.getResourceAsStream(yamlName)
    }else {
      new FileInputStream(new File(yamlName))
    }
    Util.inputStreamToString(yamlInput)
  }

  private def buildDeployConf(yamlContent: String): DeployConf = {
    val yaml = new Yaml(new Constructor(classOf[DeployConf]))
    val deployConf = yaml.loadAs(yamlContent, classOf[DeployConf])
    deployConf
  }
}
