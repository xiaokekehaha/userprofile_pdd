package creditscoring

import creditscoring.StreamingCreditScoring.SparkSessionSingleton
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
/**
 * Created by thinkpad on 2018/3/29.
 */
class CreditsModel {

}
object  CreditsModel{
  def  main (args: Array[String]){
    val sparkConf: SparkConf = new SparkConf().setAppName("creditsScoringModelBuilderTest")
    val spark=SparkSessionSingleton.getInstance(sparkConf)
    val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._
    //rdd.toDF()
    val trainData=DataProgress.prepareData(spark.sparkContext,"/home/yarn/project/german_credit.csv").toDF()

    val binarizer = new Binarizer().setInputCol("features").setOutputCol("binaryFeatures").setThreshold(0.1)
    //直接调用api
    val rf = new RandomForestClassifier().setNumTrees(20).setFeaturesCol("binaryFeatures")
    //平价模型好坏
    val evaluator = new MulticlassClassificationEvaluator()
    val pipeline = new Pipeline().setStages(Array(binarizer, rf))

    // Define the grid of hyperparameters to test for model tuning.
    val paramGrid = new ParamGridBuilder().addGrid(rf.maxDepth, Array(3, 5, 8)).build()

    // 交叉检验
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid)
    val cvModel = cv.fit(trainData)
    cvModel.avgMetrics
    //pmml 线上解析 --业内统一的机器学习模型文件
    cvModel.save("file:///home/yarn/project/module2")
  }

}
