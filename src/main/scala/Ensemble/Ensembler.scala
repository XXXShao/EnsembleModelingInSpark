package Ensemble

import org.apache.spark.annotation.Since
import org.apache.spark.ml.{UnaryTransformer, _}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by xueting.shao on 7/3/17.
  */
class Ensembler(override val uid: String) extends Estimator[EnsembleModel] {

  def this() = this(Identifiable.randomUID("ensembler"))

  //components can be estimator, or pipline which will have estimator as the last stage
  val components: Param[Array[PipelineStage]] = new Param(this, "components", "components of the =ensemble")


  def setComponents(value: Array[_ <: PipelineStage]): this.type = {
    set(components, value.asInstanceOf[Array[PipelineStage]])
    this
  }

  /**
    * not right yet
    * @param value
    * @return
    */
  def setInputFeatures(value: String): this.type = {
    $(components).map(stage => stage.set(stage.getParam("featuresCol"), value))
    this
  }

  override def fit(dataset: Dataset[_]): EnsembleModel = {
    transformSchema(dataset.schema, logging = true)
    val theStages = $(components)
//    val first = theStages(0)
//    val inputCol: String = first match{
//      case predictor: Predictor[_, _, _] => {
//        predictor.getFeaturesCol
//      }
//      case pipline: Pipeline => {
//        val firstStage = pipline.getStages.head
//        firstStage.
//      }
//      case _ => throw new IllegalArgumentException(
//        s"Does not support component(first one) $first of type ${first.getClass}")
//    }

    //To DO: bootstrap data

    /**
      * add aggregate param for model vote in ensembler
      * (1) equal weight
      * (2) weighted majority
      * (3)rank average?
      */

    var curDataset = dataset
    val transformers = ListBuffer.empty[Transformer]
//    val inputCol = ArrayBuffer.empty[String]
    var piplineFields = ArrayBuffer.empty[StructField]
    theStages.view.foreach { component =>{
      val transformer = component match {
        case predictor: Predictor[_,_,_] => {
//          inputCol += predictor.getFeaturesCol
          predictor.setPredictionCol("ensemblePrediction")

          predictor.fit(curDataset)
        }
        case pipline: Pipeline =>{

//          val firstStage = pipline.getStages.head
//          firstStage match {
//            case predictor: Predictor[_,_,_] => inputCol += predictor.getFeaturesCol
//            case unarytransformer: UnaryTransformer[_,_,_] => inputCol += unarytransformer.getInputCol
//            case _ => throw new IllegalArgumentException(
//              s"Does not support component(first) $firstStage of type ${firstStage.getClass}"
//            )
//          }

          val lastStage = pipline.getStages.last

          lastStage match {
            case predictor: Predictor[_,_,_] => predictor.setPredictionCol("ensemblePrediction")
            case _ => throw new IllegalArgumentException(
              s"Does not support component(first) $lastStage of type ${lastStage.getClass}"
            )
          }

//          val theStages = pipline.getStages.init
//
//          piplineFields ++= theStages
//            .foldLeft(dataset.toDF().schema)((cur, stage) => stage.transformSchema(cur)).fields

          pipline.fit(curDataset)
        }
        case _ =>
          throw new IllegalArgumentException(
            s"Does not support component $component of type ${component.getClass}")
      }
      transformers += transformer
    }

    }

    val originalFields = dataset.toDF().schema.fields
    val outputFields = if(!piplineFields.isEmpty) {
      (originalFields ++ piplineFields).distinct
    } else {
      originalFields
    }

//    val inputColName = inputCol.toSet
//    require(inputColName.size == 1,
//      s"featuresCols or inputCols should be the same, while there are ${inputColName.size} different col names")
    new EnsembleModel(uid, transformers.toArray, outputFields).setParent(this)
  }

  override def copy(extra: ParamMap): Ensembler = {
    val map = extractParamMap(extra)
    val newComponents = map(components).map(_.copy(extra))
    new Ensembler(uid).setComponents(newComponents)
  }

  override def transformSchema(schema: StructType): StructType = {
    val theStages = $(components)
//    require(theStages.toSet.size == theStages.length,
//      "Cannot have duplicate components in a pipeline.")
//    theStages.foldLeft(schema)((cur, stage) => stage.transformSchema(cur))

    schema
  }


}
