package userprofile

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by thinkpad on 2018/11/21.
 */
object KmeansUserCluster {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("usercluster").setMaster("local[2]")
    val sc=new SparkContext(conf)

    val data=sc.textFile("/tmp/kmeans_data.txt")

    val parseData=data.map{
    lines=>
      val datas=lines.split(" ").map(_.toDouble)
      org.apache.spark.mllib.linalg.Vectors.dense(datas)
    }
    val k=4
    val maxIterations=10
    //训练kmeans 模型
    val kmeansModel=KMeans.train(parseData,k,maxIterations)
    //打印中心点
    kmeansModel.clusterCenters.map(_.toArray.mkString(",")).foreach(println(_))

    kmeansModel.predict( org.apache.spark.mllib.linalg.Vectors.dense(Array(1.0,5.0,7.0)))
    //评判kmeans聚类好坏的损失函数值
    //val computeCostValue=kmeansModel.computeCost(parseData)

    // val maxIterations=10
    //列举不同的k值 找到最近k值
     val a=for(cluster <- Array(2,3,4,5,6))yield {
       val clusterModel=KMeans.train(parseData,cluster,maxIterations)
       val sse= clusterModel.computeCost(parseData)
       (cluster,sse)
     }
     a.sortBy(_._2).reverse.foreach(println(_))


  }
}
