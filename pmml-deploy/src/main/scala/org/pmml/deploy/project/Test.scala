package org.pmml.deploy.project

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.jpmml.evaluator.spark.{EvaluatorUtil, TransformerBuilder}

/**
  * Created by yihaibo on 19/2/25.
  */
object Test {

  case class InputRecord(Sepal_Length: Double, Sepal_Width: Double, Petal_Length: Double, Petal_Width: Double)
  case class ResultRecord(Species: String, Probability_setosa: Double, Probability_versicolor: Double, Probability_virginica: Double, Node_id: String)

  def main(args: Array[String]): Unit = {
    implicit val sparkSession = SparkSession
      .builder()
      .master("local")
      .config(
        new SparkConf().setAppName("DecisionTreeIris")
      ).getOrCreate()

    val inputRdd = sparkSession.sparkContext.makeRDD(Seq(
      InputRecord(5.1, 3.5, 1.4, 0.2),
      InputRecord(7, 3.2, 4.7, 1.4),
      InputRecord(6.3, 3.3, 6, 2.5)
    ))
    val inputDs = sparkSession.createDataFrame(inputRdd)

    val expectedResultRdd = sparkSession.sparkContext.makeRDD(Seq(
      ResultRecord("setosa", 1.0, 0.0, 0.0, "2"),
      ResultRecord("versicolor", 0.0, 0.9074074074074074, 0.09259259259259259, "6"),
      ResultRecord("virginica", 0.0, 0.021739130434782608, 0.9782608695652174, "7")
    ))
    val expectedResultDs = sparkSession.createDataFrame(expectedResultRdd)

    // Load the PMML
//    val pmmlFile = new File("DecisionTreeIris.pmml")

    val pmmlFile = new File(getClass.getClassLoader.getResource("DecisionTreeIris.pmml").getFile)

    // Create the evaluator
    val evaluator = EvaluatorUtil.createEvaluator(pmmlFile)

    // Create the transformer
    val pmmlTransformer = new TransformerBuilder(evaluator)
      .withTargetCols
      .withOutputCols
      .exploded(true)
      .build()

    // Verify the transformed results
    var resultDs = pmmlTransformer.transform(inputDs)
    resultDs.show

    resultDs = resultDs.select("Species", "Probability_setosa", "Probability_versicolor", "Probability_virginica", "Node_Id")

    assert(resultDs.rdd.collect.toList == expectedResultDs.rdd.collect.toList)
  }

}
