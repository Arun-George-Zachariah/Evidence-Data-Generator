package edu.missouri

import java.io.{BufferedWriter, File, FileWriter}

import com.google.gson.Gson
import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.TwitterFactory

object CollectTweets {

  private val defaultInterval = 40
  private val gson = new Gson()
  private val config = ConfigFactory.parseFile(new File("conf/app.config"))

  def main(args: Array[String]) = {
    // Defining the Spark Context.
    val conf = new SparkConf().setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Validating input arguments.
    if(args.length != 2) {
      println("Usage: " + "CollectTweets  <NUM_TO_COLLECT> <OUT_FILE>")
      System.exit(-1)
    }

    // Streaming Twitter Data.
    getTwitterData(sc, args(0).toInt, args(1))

    // Stop and exit.
    sc.stop()
    System.exit(0)
  }

  def getTwitterData(sc: SparkContext, numTweetsToCollect: Int, outFile: String): Any = {
    // Initializing the number of tweets collected.
    var numTweetsCollected = 0L

    // Defining the spark streaming context.
    val streamingContext = new StreamingContext(sc, Seconds(defaultInterval))

    // Defining the configurations.
    val twitterConfig = new twitter4j.conf.ConfigurationBuilder()
      .setOAuthConsumerKey(config.getString("CONSUMER_KEY"))
      .setOAuthConsumerSecret(config.getString("CONSUMER_SECRET"))
      .setOAuthAccessToken(config.getString("ACCESS_TOKEN"))
      .setOAuthAccessTokenSecret(config.getString("ACCESS_TOKEN_SECRET"))
      .build

    // Twitter Authorization.
    val twitter_auth = new TwitterFactory(twitterConfig)
    val a = new twitter4j.auth.OAuthAuthorization(twitterConfig)
    val auth : Option[twitter4j.auth.Authorization] = Some(twitter_auth.getInstance(a).getAuthorization())

    // Defining the keywords for filtering the tweets.
    val keywordLst = config.getStringList("KEYWORDS")
    val filter = keywordLst.toArray(Array.ofDim[String](keywordLst.size))

    var writer:BufferedWriter = null
    try {
      writer = new BufferedWriter(new FileWriter(new File(outFile)))
      val tweets = TwitterUtils.createStream(streamingContext, auth, filter).map(gson.toJson(_))

      tweets.foreachRDD(rdd => {
        // Iterating over each tweet.
        rdd.collect().foreach(tweet => {

          // Writing the output to a file.
          writer.write(tweet + "\n")

          // Incrementing the tweets collected.
          numTweetsCollected += 1

          // Stopping the process once the required tweets are collected.
          if (numTweetsCollected >= numTweetsToCollect) {
            println("CollectTweets :: getTwitterData :: Collected required number of tweets.")
            writer.flush()
            System.exit(0)
          }
        })
      })

      // Starting the Streaming process.
      print("CollectTweets :: getTwitterData :: Collecting Twitter tweets via streaming APIs...")
      streamingContext.start()
      streamingContext.awaitTermination()

    } catch {
      case e: Exception =>
        System.out.println("CollectTweets :: getTwitterData :: Exception encountered while writing to the file")
        e.printStackTrace()
        System.exit(-1)
    } finally {
      try {
        writer.close()
      } catch {
        case e:Exception => {
          System.out.println("CollectTweets :: getTwitterData :: Exception encountered while closing the BufferedWriter")
          System.exit(-1)
        }
      }
    }
  }
}
