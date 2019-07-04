package org.pmml.deploy.handler.model

import java.io.File

import org.apache.spark.sql.DataFrame
import org.jpmml.evaluator.spark.{EvaluatorUtil, TransformerBuilder}
import org.pmml.deploy.handler.{Handler, Name, ParameterHelper}

/**
  * Created by yihaibo on 19/3/5.
  */
@Name("model_pmml")
class Pmml(val parameters: Map[String, Any]) extends Handler with ParameterHelper{

  override def transform(df: DataFrame): DataFrame = {

    val evaluator = EvaluatorUtil.createEvaluator(new File(getString("pmmlPath")))
    val pmmlTransformer = new TransformerBuilder(evaluator)
      .withTargetCols
      .withOutputCols
      .exploded(true)
      .build()
    val resultDf = pmmlTransformer.transform(df)

    val excludeOriginColumn = getValAsOrDefault[Boolean]("excludeOriginColumn", false)
    if(excludeOriginColumn) {
      val excludeExcept = getListOrDefault("excludeExcept", List())
      val columns = resultDf.columns.filter{c => !df.columns.contains(c) || excludeExcept.contains(c)}
      logInfo(s"select column:${columns.toList}")
      resultDf.selectExpr(columns: _*)
    }else {
      resultDf
    }
  }
}
