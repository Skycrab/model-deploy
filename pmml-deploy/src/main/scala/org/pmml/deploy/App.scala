package org.pmml.deploy

/**
  * 命令行格式
  * year:$year month:$month day:$day
  * Created by yihaibo on 19/2/28.
  */
object App {
  def main(args: Array[String]): Unit = {
    var argsAttr = args.map{case s => {
      require(s.contains(":"), s"arg $s must key:value pattern")
        val t = s.split(":", 2)
        (t(0), t(1))
      }}.toMap
    //年月日必须有，区分调度时间
    val mustKeys = Array("year", "month", "day")
    if(argsAttr.keys.filter(mustKeys.contains(_)).size != mustKeys.length) {
      throw new DeployException(s"keys ${mustKeys.toList} must provide")
    }
    argsAttr += ("scheduleDay" -> mustKeys.collect(argsAttr).mkString)
    argsAttr += ("V_YESTERDAY_0" -> mustKeys.collect(argsAttr).mkString("-"))
    Schedule.run(argsAttr)
  }
}
