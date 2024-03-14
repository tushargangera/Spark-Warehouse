package com.snb.app

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object ProcessData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    //
    //    conf.set("spark.hadoop.fs.default.name", "hdfs://bigdatalite.localdomain:8020")
    //      .set("spark.hadoop.fs.defaultFS", "hdfs://bigdatalite.localdomain:8020")
    //
    val master = "local[*]"
    val spark = SparkSession.builder().config(conf).master(master).appName("test").enableHiveSupport().getOrCreate()
    //
    val df1 = spark.read.option("header", true).parquet("/home/oracle/Downloads/snb/part1.parquet")
    //    //    spark.sql("use moviework")
    //
    //    //spark.sql("create database snb")
    //
    //    val databases = spark.sql("show databases")
    ////
    //    databases.show()

    //    df1.show(100,false)
    df1.printSchema()
  }

}

object HiveSchemas {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    conf.set("spark.hadoop.fs.default.name", "hdfs://bigdatalite.localdomain:8020")
      .set("spark.hadoop.fs.defaultFS", "hdfs://bigdatalite.localdomain:8020")

    conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")

    val master = "local[*]"
    val spark = SparkSession.builder().config(conf).master(master).appName("test").enableHiveSupport().getOrCreate()
    //
    val df1 = spark.read.option("header", true).parquet("/venky/part1.parquet")

    df1.printSchema()

    //    val raw_data_df = df1.drop("preprocessing","en_tweets","user_responce","user_reaction")

    //    raw_data_df.write.mode("append").insertInto("snb_rawzone.twitter_user_source")

    //    spark.sql("create database snb_rawzone")
    //    spark.sql("create database snb_curated")
    //    spark.sql("create database snb_publish")

    //    spark.sql("use snb_rawzone")
    //    spark.sql("create table twitter_user_source( tweets string,id long,len int,date timestamp,source string,likes int,retweets int) STORED AS PARQUET")
    spark.sql("use snb_curated")
    spark.sql("create table twitter_user_processed( tweets string,id long,len int,date timestamp,source string,likes int,retweets int,en_tweets string,user_responce int,user_reaction string) STORED AS PARQUET")
    //
    //    val df = spark.sql("show tables")
    //    spark.sql("select * from  snb_rawzone.twitter_user_source").show()

    val curated_data_df = df1.drop("preprocessing")

    curated_data_df.write.mode("append").insertInto("snb_curated.twitter_user_processed")

    spark.sql("select * from  snb_curated.twitter_user_processed").show()

  }
}