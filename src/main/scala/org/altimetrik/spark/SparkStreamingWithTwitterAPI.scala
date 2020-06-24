package org.siddhu.spark.streaming

import com.typesafe.config.ConfigFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.streaming._

/**
 * Created by Siddhu Ch 17/06/2020
 *
 */

object SparkStreamingWithTwitterAPI {
  def main(args: Array[String]): Unit = {

   //Input parameter check
   if (args.length < 1) {
    System.err.println("Please Specify The Environment Parameter Either dev/prod")
    System.exit(1)
   }

   //Load parameters from properties file
   val envConf = ConfigFactory.load().getConfig(args(0))
   val sparkMaster = envConf.getString("execution.mode") //local/yarn
   // Get the twitter data filer from properties file
   val filterData = envConf.getString("filterData")

   //Read aws credentials from properties file
   val accessKeyId = envConf.getString("accessKey")
   val secretAccessKey  = envConf.getString("secretKey")

   //Read twitter credentials from properties file
   val consumerKey       = envConf.getString("consumerKey")
   val consumerSecret    = envConf.getString("consumerSecret")
   val accessToken       = envConf.getString("accessToken")
   val accessTokenSecret = envConf.getString("accessTokenSecret")

   //Create spark session object
   val spark = SparkSession.builder().appName("TwitterPopularTags").master(sparkMaster).getOrCreate()

   //Create spark streaming context object
   val ssc = new org.apache.spark.streaming.StreamingContext(spark.sparkContext ,Seconds(20))

   val cb = new ConfigurationBuilder
   cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
     .setOAuthConsumerSecret(consumerSecret)
     .setOAuthAccessToken(accessToken)
     .setOAuthAccessTokenSecret(accessTokenSecret)

   val auth = new OAuthAuthorization(cb.build)

   //Stream twitter data
   //val tweets: ReceiverInputDStream[Status] = TwitterUtils.createStream(streamingContext, Some(auth), filters = Seq(filterData))
   val tweets: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, Some(auth))

//USE CASE - 1
//============
   //filter out only english tweets
   val englishTweets = tweets.filter(_.getLang == "en")

   //Filtering the text starts with # words
   val hashTags: DStream[String] = englishTweets.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

   //Every 20 seconds stream last 60 seconds of data
   val topCounts60: DStream[(Int, String)] = hashTags.map((_, 1)).reduceByKeyAndWindow(_+_, Seconds(60))
     .map{case (topic, count) => (count,topic)}
     .transform(_.sortByKey(ascending = false))

   //Print Popular hashtags from streamed data
   val res: Unit = topCounts60.foreachRDD(rdd => {
    val topList = rdd.take(10)
    println("\nPopular topics in last 10 seconds (%s total) :".format(rdd.count()))
    topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag,count))}
   })

   spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKeyId)
   spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretAccessKey)
   spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    //Write Data into local
    topCounts60.saveAsTextFiles(envConf.getString("output.dir1"),envConf.getString("fileType"))

    //Write Data into AWS S3
    //topCounts60.saveAsTextFiles("s3a://s3_bucket_name/tophashtags")


   //USE CASE - 2  Compute the sentiment of a tweet
//============
   import Utils._

   // To compute the sentiment of a tweet we'll use different set of words used to
   // filter and score each word of a sentence. Since these lists are pretty small
   // it can be worthwhile to broadcast those across the cluster so that every
   // executor can access them locally
   val uselessWords:  Broadcast[Set[String]] = spark.sparkContext.broadcast(load("/stop-words.dat"))
   val positiveWords: Broadcast[Set[String]] = spark.sparkContext.broadcast(load("/pos-words.dat"))
   val negativeWords: Broadcast[Set[String]] = spark.sparkContext.broadcast(load("/neg-words.dat"))

   // Let's extract the words of each tweet
   // We'll carry the tweet along in order to print it in the end
   val textAndSentences: DStream[(TweetText, Sentence)] =
   englishTweets.
     map(_.getText).
     map(tweetText => (tweetText, wordsOf(tweetText))) //split with space

   // Apply several transformations that allow us to keep just meaningful sentences
   val textAndMeaningfulSentences: DStream[(TweetText, Sentence)] =
    textAndSentences.
      mapValues(extractWords).  //filter words contains only [a-z]
      mapValues(words => keepMeaningfulWords(words, uselessWords.value)).
      filter { case (_, sentence) => sentence.nonEmpty }

   // Compute the score of each sentence and keep only the non-neutral ones
   val textAndNonNeutralScore: DStream[(TweetText, Int)] =
    textAndMeaningfulSentences.
      mapValues(sentence => computeScore(sentence, positiveWords.value, negativeWords.value)).
      filter { case (_, score) => score != 0 }

   // Transform the (tweet, score) pair into a readable string and print it
   textAndNonNeutralScore.map(makeReadable).print

    //Write Data into local
    textAndNonNeutralScore.map(makeReadable)
     .saveAsTextFiles(envConf.getString("output.dir2"),envConf.getString("fileType"))
   //Write Data into AWS S3
   textAndNonNeutralScore.saveAsTextFiles("s3a://s3_bucket_name/tweet_sentiment")


   ssc.start()             // Start the computation
   ssc.awaitTermination() // Wait for the computation to terminate

  }

}
