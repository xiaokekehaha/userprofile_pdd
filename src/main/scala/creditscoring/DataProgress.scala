package creditscoring

import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.LabeledPoint

/**
 * Created by thinkpad on 2018/3/29.
 */
class DataProgress {

}

object DataProgress {
  def prepareData(sc: SparkContext, fileName: String): RDD[LabeledPoint] = {
    //"/home/yarn/sparkdatas/german_credit.csv"
    val creditText = scala.io.Source.fromFile(fileName).mkString
    val allLines = creditText.split("\n")
    val recordsOnly = allLines.slice(1, allLines.size - 1)//去掉csv格式的头文件
    val allDataset = sc.parallelize(recordsOnly).map {
      case line => val arr = line.split(",").map(t => t.trim()).map(p => p.toDouble)
        new LabeledPoint(arr(0).asInstanceOf[Int], org.apache.spark.ml.linalg.Vectors.dense(arr.slice(1, arr.length)))
    }
    allDataset
  }
}
