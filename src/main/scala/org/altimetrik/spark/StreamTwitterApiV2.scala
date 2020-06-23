package org.altimetrik.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import com.typesafe.config.ConfigFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import twitter4j.Status


/**
 * Created by Siddhu Ch 17/06/2020
 *
 */

object StreamTwitterApiV2 {
  def main(args: Array[String]): Unit = {

    //Input parameter check
    if (args.length < 1) {
      System.err.println("Please Specify The Environment Parameter Either dev/prod")
      System.exit(1)
    }

    //Load parameters from properties file
    val envConf = ConfigFactory.load().getConfig(args(0))

    //Hadoop binaries for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    // values of Twitter API.
    val consumerKey       = envConf.getString("consumerKey")
    val consumerSecret    = envConf.getString("consumerSecret")
    val accessToken       = envConf.getString("accessToken")
    val accessTokenSecret = envConf.getString("accessTokenSecret")
   // Get the twitter data filer from properties file
    val filterData = envConf.getString("filterData")
    val sparkMaster = envConf.getString("execution.mode") //local/yarn

    // spark configuration object
    val sparkConf = new SparkConf()
      .setAppName("TwitterPopularTags")
      .setMaster(sparkMaster)
      .set("spark.driver.allowMultipleContexts", "true")

    val sparkContext = new SparkContext(sparkConf)

    // spark streaming Context object
    val streamingContext = new StreamingContext(sparkContext,Seconds(10))

    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)

    val auth = new OAuthAuthorization(cb.build)
    //Stream twitter data
    //val tweets: ReceiverInputDStream[Status] = TwitterUtils.createStream(streamingContext, Some(auth), filters = Seq(filterData))
    val tweets: ReceiverInputDStream[Status] = TwitterUtils.createStream(streamingContext, Some(auth))


 //USE CASE - 1
 //============
    //filter out only english tweets
    val englishTweets = tweets.filter(_.getLang == "en")

    //Filtering the text starts with #
    val hashTags: DStream[String] = englishTweets.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    hashTags.print

    //Every 5 seconds stream last 60 seconds of data
    val topCounts60: DStream[(Int, String)] = hashTags.map((_, 1)).reduceByKeyAndWindow(_+_, Seconds(60))
      .map{case (topic, count) => (count,topic)}
      .transform(_.sortByKey(ascending = false))


    //Print Popular hashtags from streamed data
    val res: Unit = topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total) :".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag,count))}
    })


    //save it as csv files
    //englishTweets.saveAsTextFiles(envConf.getString("output.dir1"),envConf.getString("fileType"))


    //******************************
    // S3 Integration is on going
    //*****************************



    //Write Data into AWS S3
   // val accessKeyId = envConf.getString("accessKey")
    //val secretAccessKey  = envConf.getString("secretKey")
    //val S3BucketName = envConf.getString("bucket.name")
    //val fileName = envConf.getString("file.name")

    val accessKeyId = "AKIA5AQ2FF5RUBVEY5TN"
    val secretAccessKey = "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"

    sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKeyId)
    sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)
    sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    //sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")


    topCounts60.saveAsTextFiles("s3a://twitter-streaming-siddhu/TwitterData/stream")
   // topCounts60.foreachRDD(rdd => {
    //  rdd.saveAsTextFile("s3a://siddhulandingzone/input/xyz")
    //})




//USE CASE - 2  Compute the sentiment of a tweet
//============
/*

    import Utils._

    // To compute the sentiment of a tweet we'll use different set of words used to
    // filter and score each word of a sentence. Since these lists are pretty small
    // it can be worthwhile to broadcast those across the cluster so that every
    // executor can access them locally
    val uselessWords:  Broadcast[Set[String]] = sparkContext.broadcast(load("/stop-words.dat"))
    val positiveWords: Broadcast[Set[String]] = sparkContext.broadcast(load("/pos-words.dat"))
    val negativeWords: Broadcast[Set[String]] = sparkContext.broadcast(load("/neg-words.dat"))

    // Let's extract the words of each tweet
    // We'll carry the tweet along in order to print it in the end
    val textAndSentences: DStream[(TweetText, Sentence)] =
    tweets.
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
//    textAndNonNeutralScore.map(makeReadable)
  //                        .saveAsTextFiles(envConf.getString("output.dir2"),envConf.getString("fileType"))

*/

    streamingContext.start()               // Start the computation
    streamingContext.awaitTermination()   // Wait for the computation to terminate
  }

}
