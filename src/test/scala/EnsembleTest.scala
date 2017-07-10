package Ensemble

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by xueting.shao on 7/3/17.
  */
object EnsembleTest {

  def main(args: Array[String]): Unit ={
    val conf = new SparkConf()
      .setAppName("Cross Validation Example")
      .setMaster("local[4]")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    //turn of the log
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

//    val training = spark.createDataFrame(Seq(
//      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
//      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
//      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
//      (1.0, Vectors.dense(0.0, 1.2, -0.5))
//    )).toDF("label", "features")

    val training = spark.read.format("libsvm").load("data/sample_libsvm_data.txt")

    // Create a LogisticRegression instance. This instance is an Estimator.
    val lr = new LogisticRegression()

    // We may set parameters using setter methods.
    lr.setMaxIter(10)
      .setRegParam(0.01)

    val lr1 = new LogisticRegression()

    val lr2 = new LogisticRegression().setMaxIter(10).setRegParam(0.05)

    //third component is a pipline
    val normalizer = new Normalizer().setInputCol("features")
      .setOutputCol("normFeatures").setP(1.0)

    val lr30 = new LogisticRegression().setFeaturesCol("normFeatures")
    val lr3 = new Pipeline().setStages(Array(normalizer, lr30))


    //ensemble models
    val ensembling = new Ensembler().setComponents(Array(lr, lr1, lr2, lr3))

    val model = ensembling.fit(training)

    val transformers = model.components.map(t => t.transform(training))
    transformers.map(x => x.show()) //show individual predictions

    //combine models
    val prediction = model.transform(training)
    prediction.show()


  }

}
