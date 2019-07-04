package org.pmml.deploy.handler.feature

import org.apache.spark.sql.DataFrame
import org.pmml.deploy.handler.{Handler, Name}

/**
  * Created by yihaibo on 19/3/5.
  */
@Name("feature_woe")
class Woe(val parameters: Map[String, Any]) extends Handler {

  override def transform(dataFrame: DataFrame): DataFrame = {
    null
  }
}
