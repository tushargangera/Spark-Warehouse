package com.snb.app

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object twitterAnalysis {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf

    conf.set("spark.hadoop.fs.default.name", "hdfs://bigdatalite.localdomain:8020")
      .set("spark.hadoop.fs.defaultFS", "hdfs://bigdatalite.localdomain:8020")

    val master = "local[*]"
    val spark = SparkSession.builder().config(conf).master(master).appName("test").enableHiveSupport().getOrCreate()

    val df1 = spark.read.option("header", true).csv("/venky/test.csv")
    //    spark.sql("use moviework")

    spark.sql("create database snb")

    val databases = spark.sql("show databases")

    databases.show()

  }
}



import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.Status
import twitter4j.api
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext


object twitterstreaming {
  def main(args: Array[String]) {

    val appName = "TwitterData"
    val conf = new SparkConf()
    conf.setAppName(appName).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    
    val consumerKey = "gDN3D6O7fH3r2iE9oM07MrWXr"
    
    val consumerSecret = "kJQVweUsoUCWrdP5NEgbB1KF4GXWVc2zKM0XnGXyxnwO5WlVk4"
    
    val accessToken = "1449426026807787520-LT5LI0fTQOcQQKi63nfkfOqKEX4A0B"
    
    val accessTokenSecret = "qsKwIp4YuXMYXhhnZV7HVoGQz77BzjS2pqi3WiX2tAuSM"
    

    val filters = Array("SNB","South National Bank")
    val cb = new ConfigurationBuilder

    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
    
    val auth = new OAuthAuthorization(cb.build)
    
    
    
//    val tweetsCheck = api.
    val tweets = TwitterUtils.createStream(ssc, Some(auth), filters)


    tweets.saveAsTextFiles("tweets", "json")
    ssc.start()
    ssc.awaitTermination()
  }
}