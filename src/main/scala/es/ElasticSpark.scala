package es

/**
 * Created by thinkpad on 2018/11/12.
 */
class ElasticSpark {

}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._

case class ExampleClass(first_name: String, age: Int)
object ElasticSpark {
  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName(ElasticSpark.getClass.getName)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc=new SparkContext(conf)
    import spark.sqlContext.implicits._
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val userProfile=sqlContext.sql("select * from adl_user_profile_total_all_d")
    // Elastic connection parameters
    val elasticConf: Map[String, String] = Map("es.nodes" -> "hadoop002",
      "es.clustername" -> "userprofile")
    val indexName = "userprofile"
    val mappingName = "adl_user_profile_total_all_d"
    // Dummy DataFrame
   // val df = spark.sparkContext.parallelize((1 to 2).map(x => ExampleClass(s"${x}_name",x ))).toDF()
    // Write elasticsearch
    userProfile.saveToEs(s"${indexName}/${mappingName}", elasticConf)
    spark.stop()
  }
}