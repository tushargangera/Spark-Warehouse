package com.snb.publish

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{ col, max, count }

object hiveViews {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    conf.set("spark.hadoop.fs.default.name", "hdfs://bigdatalite.localdomain:8020")
      .set("spark.hadoop.fs.defaultFS", "hdfs://bigdatalite.localdomain:8020")

    conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")

    val master = "local[*]"
    val spark = SparkSession.builder().config(conf).master(master).appName("test").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    printModule("databases avialable")
    spark.sql("show databases").show()

    printModule("tables under snb_rawzone schema")
    spark.sql("use snb_rawzone")
    spark.sql("show tables").show(false)

    printModule("raw zone twitter_user_source table")
    spark.sql("select * from snb_rawzone.twitter_user_source").show(30)
    spark.sql("select * from snb_rawzone.twitter_user_source").printSchema()

    printModule("snb_curated tables")
    spark.sql("use snb_curated")
    spark.sql("show tables").show()

    printModule("twitter_user_processed table")
    spark.sql("select * from snb_curated.twitter_user_processed").show(false)
    spark.sql("select * from snb_curated.twitter_user_processed").printSchema()

    printModule("snb_publish views")
    spark.sql("use snb_publish")
    spark.sql("show tables").show(false)

    printModule("reaction_count view")
    spark.sql("select * from snb_publish.reaction_count").show()

    printModule("most_liked_tweets view")
    spark.sql("select * from snb_publish.most_liked_tweets").show(false)

  }

  def printModule(text: String) {
    println("================================================================================")
    println("**************     " + text + "         ********************************")
    println("================================================================================")

  }
}
 