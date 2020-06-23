/*
package org.altimetrik.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import com.typesafe.config.ConfigFactory

//import twitter4j.conf.ConfigurationBuilder
//import twitter4j.auth.OAuthAuthorization

/**
 * Created by Siddhu Ch 17/06/2020
 *
 */

object StreamTwitterApi {
  def main(args: Array[String]): Unit = {

    //verify to we are passing more then 4 parameters or not
    if (args.length < 4) {
      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret>" +
        "[<filters>]")
      System.exit(1)
    }

    //Hadoop binaries for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    //Set system properties so that Twitter4j library used by twitter stream
    //use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken",accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // spark configuration object
    val sparkConf = new SparkConf()
                    .setAppName("TwitterPopularTags")
                    .setMaster("local[2]")
    // spark streaming Context object
    val streamingContext = new StreamingContext(sparkConf,Seconds(5))

    //Stream twitter data
    val stream = TwitterUtils.createStream(streamingContext,None,filters)
    //filter out only english tweets
    val englishTweets = stream.filter(_.getLang == "en")
    //Filtering the text starts with #
    val hashTags = englishTweets.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    //Every 5 seconds stream last 60 seconds of data
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_+_, Seconds(60))
      .map{case (topic, count) => (count,topic)}
      .transform((_.sortByKey(false)))

    //Print Popular hashtags from streamed data
    val res = topCounts60.foreachRDD( rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total) :".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag,count))}
    })

    //save it as csv files
    topCounts60.saveAsTextFiles("TopTweetsFromLast60Sec","csv")

    streamingContext.start()               // Start the computation
    streamingContext.awaitTermination()   // Wait for the computation to terminate
  }

}
*/
