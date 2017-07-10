package SparkExamples

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.tree.configuration.Strategy

/**
  * Created by xueting.shao on 6/30/17.
  */
object randomForest {

  def main(args: Array[String]): Unit ={

    val conf = new SparkConf()
      .setAppName("Cross Validation Example")
      .setMaster("local[4]")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .load("data/bank_additional_full_new.csv")

    df.show()

    df.printSchema()

    val labelIndexer = new StringIndexer()
      .setInputCol("y")
      .setOutputCol("indexedy")
      .fit(df)

    spark.stop()

  }





}
