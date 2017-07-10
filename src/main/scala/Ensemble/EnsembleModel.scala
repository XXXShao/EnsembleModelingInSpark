package Ensemble

import org.apache.spark.annotation.Since
import org.apache.spark.ml.Pipeline.SharedReadWrite
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.{Model, Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.ml.util.{MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xueting.shao on 7/3/17.
  */
class EnsembleModel(override val uid: String,
                    val components: Array[Transformer],
                    val outputFields: Array[StructField]) extends Model[EnsembleModel] with MLWritable{

  var combineSchema: String = "average"
  def setCombineSchema(value: String): this.type = {
    require(value == "average" | value == "majorityVote",
      "Only support average prediction and majority vote!")
    combineSchema = value
    this
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    /*
    * not implemented yet
     */
    dataset.persist() //don't know how to persist yet
    transformSchema(dataset.schema, logging = true)

//    val schema = new StructType()
//    val structTypes =
//      components.map(component => component.transformSchema(schema))
//    structTypes.foreach(s => s.printTreeString())
    val votes = components.map(transformer => transformer.transform(dataset.toDF))
    val voteFields = votes.map(df => df.schema.fields).head
    voteFields.map(x => x.name)

//    val test = components(0)
//    test.extractParamMap()



    val inputCol = outputFields.map(x => x.name)
    import dataset.sparkSession.implicits._
    val outputDF = votes.tail.foldLeft(votes(0))(
      (acc, add) =>
        acc.join(add.withColumnRenamed("ensemblePrediction", "predAdd"), inputCol, "outer")
        .withColumn( "ensemblePrediction",
          $"ensemblePrediction" + $"predAdd")
        .drop("predAdd")
    )

    val numComponents = components.size
    combineSchema match{
      case "average" => {
        outputDF.withColumn("finalPrediction", $"ensemblePrediction" / numComponents )
      }
      case "majority vote" => {
        outputDF.withColumn("finalPrediction", $"ensemblePrediction" / numComponents )
      }
      case _ => outputDF
    }

  }

  /**
    * This output schema should be the universal output schema for all component models
   */
  override def transformSchema(schema: StructType): StructType = {
//    components.foldLeft(schema)((cur, transformer) => transformer.transformSchema(cur))
//val outputFields = schema.fields :+
//  StructField($(outputCol), outputDataType, nullable = false)
//    StructType(outputFields)
    schema
  }

  override def copy(extra: ParamMap): EnsembleModel = {
    new EnsembleModel(uid, components.map(_.copy(extra)), outputFields).setParent(parent)
  }

//  override def write: MLWriter = new EnsembleModel.EnsembleModelWriter(this)

  override def write: MLWriter = {
    throw new Exception("not supported yet")
  }

}

//object EnsembleModel extends MLReadable[EnsembleModel] {
//
//  import Pipeline.SharedReadWrite
//
//  override def read: MLReader[EnsembleModel] = new EnsembleModelReader
//
//  override def load(path: String): EnsembleModel = super.load(path)
//
//  private[EnsembelModel] class EnsembleModelWriter(instance: PipelineModel) extends MLWriter {
//
//    SharedReadWrite.validateStages(instance.stages.asInstanceOf[Array[PipelineStage]])
//
//    override protected def saveImpl(path: String): Unit = SharedReadWrite.saveImpl(instance,
//      instance.stages.asInstanceOf[Array[PipelineStage]], sc, path)
//  }
//
//  private class EnsembleModelReader extends MLReader[EnsembleModel] {
//
//    /** Checked against metadata when loading model */
//    private val className = classOf[PipelineModel].getName
//
//    override def load(path: String): PipelineModel = {
//      val (uid: String, stages: Array[PipelineStage]) = SharedReadWrite.load(className, sc, path)
//      val transformers = stages map {
//        case stage: Transformer => stage
//        case other => throw new RuntimeException(s"PipelineModel.read loaded a stage but found it" +
//          s" was not a Transformer.  Bad stage ${other.uid} of type ${other.getClass}")
//      }
//      new PipelineModel(uid, transformers)
//    }
//  }
//}
