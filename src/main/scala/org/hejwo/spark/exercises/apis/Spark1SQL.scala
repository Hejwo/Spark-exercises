package org.hejwo.spark.exercises.apis

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable

object Spark1SQLMain extends Spark1SQL {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)
  @transient lazy val session: SparkSession = SparkSession.builder().appName("StackOverflow").getOrCreate()

  def main(args: Array[String]) {
    sc.setLogLevel("WARN")

    val lines = sc.textFile("src/main/resources/stackoverflow.csv")
    val postings = rawPostings(lines)


  }

}

class Spark1SQL {


  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType = arr(0).toInt,
        id = arr(1).toInt,
        acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
        parentId = if (arr(3) == "") None else Some(arr(3).toInt),
        score = arr(4).toInt,
        tags = if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


}


