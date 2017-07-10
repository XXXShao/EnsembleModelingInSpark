package SparkExamples

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.Params

/**
  * Created by xueting.shao on 6/30/17.
  */
object logisticRegression {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("Cross Validation Example")
      .setMaster("local[4]")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    val training = spark.read.format("libsvm").load("data/sample_libsvm_data.txt")

    training.printSchema()

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for logistic regression
//    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // We can also use the multinomial family for binary classification
    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")
    import org.apache.spark.ml.param.ParamMap

    val newParamMap = ParamMap(mlr.predictionCol -> "newPreidction")

    val mlrModel = mlr.fit(training, newParamMap)

//    println("the max iter is",mlrModel.maxIter, mlrModel.regParam.toString())
    // Print the coefficients and intercepts for logistic regression with multinomial family
//    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
//    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")

    val prediction = mlrModel.transform(training)


    prediction.printSchema()
    prediction.show()
  }

}
