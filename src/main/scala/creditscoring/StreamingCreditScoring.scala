package creditscoring
import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.util.IntParam

/**
 * Created by thinkpad on 2017/12/5.
 */
class StreamingCreditScoring {

}
case class Record(features: org.apache.spark.ml.linalg.Vector)
object StreamingCreditScoring{
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: FlumeEventCount <host> <port>")
      System.exit(1)
    }
  //  StreamingCreditScoring.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("FlumeEventCount").setMaster("local[2]")
    val batchInterval = Milliseconds(4000)
    val ssc = new StreamingContext(sparkConf, batchInterval)
    // Create a flume stream
    val stream = FlumeUtils.createStream(ssc, args(0), args(1).toInt, StorageLevel.MEMORY_ONLY_SER_2)
    val streamEvent=stream.map(e=>new String(e.event.getBody.array()))
    val creditRDD= streamEvent.flatMap(_.split(" "))
    import org.apache.spark.ml.tuning.CrossValidatorModel
    //pmml
    val creditModel = CrossValidatorModel.load("/home/yarn/project/module/")
    stream.count().map(cnt => "Received " + cnt + " flume events." ).print()
    creditRDD.foreachRDD{
      (rdds:RDD[String],time:Time)=>
     //   import org.apache.spark.sql.SparkSession
        val spark  = SparkSessionSingleton.getInstance(rdds.sparkContext.getConf)
        import  spark.implicits._
        val requestArray=rdds.map(r=>r.asInstanceOf[String]).collect()
        println("=======start======")
        requestArray.foreach(println)
        println("=======end========")
        if(requestArray.size>0){
         val  requestRDD=  spark.sparkContext.parallelize(requestArray)
          import spark.implicits._
          // Convert RDD[String] to RDD[case class] to DataFrame
           val  credutDataFrame = rdds.map{w =>
            val ws=w.split(",").map(_.toDouble)
            Record(org.apache.spark.ml.linalg.Vectors.dense(ws))
          }.toDF()
          //pipline
          val  trainData=creditModel.transform(credutDataFrame)
          ///credutDataFrame.createOrReplaceTempView("words")//写入es hbase hive
          println("=======predict result datas ========")
          trainData.show
        }

    }

    ssc.start()
    ssc.awaitTermination()
  }
  object SparkSessionSingleton {

    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
}
