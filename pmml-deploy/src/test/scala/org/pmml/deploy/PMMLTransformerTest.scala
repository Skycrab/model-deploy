package org.pmml.deploy

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.jpmml.evaluator.spark.{EvaluatorUtil, TransformerBuilder}
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._


class PMMLTransformerTest extends FunSuite {

	test("Transformer works as expected") {
		implicit val sparkSession = SparkSession
			.builder()
			.config(
				new SparkConf()
					.setAppName("DecisionTreeIris")
					.setMaster("local")
			).getOrCreate()
    val srcDF = sparkSession.read
      .format("csv")
      .option("header","true")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
			.load("file:///tmp/train_x_x.csv")
    srcDF.show(false)

		val df = srcDF.select(srcDF.columns.map(name => col(name).cast("double")): _*)

		df.printSchema()


		// Load the PMML
		val pmmlFile = new File(getClass.getClassLoader.getResource("a_card_1.pmml").getFile)

		// Create the evaluator
		val evaluator = EvaluatorUtil.createEvaluator(pmmlFile)

		// Create the transformer
		val pmmlTransformer = new TransformerBuilder(evaluator)
			.withTargetCols
			.withOutputCols
			.exploded(true)
			.build()

		// Verify the transformed results
		var resultDs = pmmlTransformer.transform(df)
		resultDs.select("pas_age", "probability(0)", "probability(1)").show(false)
	}
}
