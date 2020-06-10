package edu.missouri

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}
import org.apache.spark.sql.SparkSession
import edu.missouri.Constants.Constants
import org.apache.spark.sql.Row

object GenerateEvidence {
  def constructEvidence(inFile: String): Unit = {
    // Defining the Spark SQL Context.
    val sqlContext = SparkSession.builder.master("local[*]").appName(Constants.APP_NAME).getOrCreate()

    var reader:BufferedReader = null
    var writer:BufferedWriter = null
    try {
      reader = new BufferedReader(new FileReader(new File(inFile)))
      writer = new BufferedWriter(new FileWriter(new File("out/evidence.db")))

      // Loading the tweets to a table.
      val myTweets = sqlContext.read.json(inFile)
      myTweets.createOrReplaceTempView(Constants.TWEETS_VIEW)

      // Querying the tweets.
      var results = sqlContext.sql(Constants.SELECT_QUERY)

      results.foreach(x => {
        // tweeted predicate
        val user = x.getAs[Long](Constants.USER)
        val tweetId = x.getAs[Long](Constants.TWEET_ID)
        writer.write(Constants.TWEETED + "(" + user + "," + tweetId + ")")

        // verified predicate
        val isVerified = x.getAs[Boolean](Constants.VERIFIED)
        if (isVerified) {
          writer.write(Constants.VERIFIED + "(" + user + ")")
        }

        val retweetedStatusId = x.getAs[Long](Constants.RETWEETED_STATUS_ID)
        if (retweetedStatusId != null) {
          // retweeted predicate
          writer.write(Constants.RETWEETED + "(" + user + "," + retweetedStatusId + ")")

          // tweeted predicate
          val retweetedStatusUserId = x.getAs[Long](Constants.RETWEETED_STATUS_USER_ID)
          writer.write(Constants.TWEETED + "(" + retweetedStatusUserId + "," + retweetedStatusId + ")")

          // retweetCount predicate
          val retweetedStatusRetweetCount = x.getAs[Long](Constants.RETWEET_COUNT)
          writer.write(Constants.RETWEET_COUNT + "(" + retweetedStatusId + "," + retweetedStatusRetweetCount + ")")
        }

        // isPossiblySensitive predicate
        val isPossiblySensitive = x.getAs[Boolean](Constants.IS_POSSIBLY_SENSITIVE)
        if (isPossiblySensitive) {
          writer.write(Constants.IS_POSSIBLY_SENSITIVE + "(" + tweetId + ")")
        }

        // statusesCount predicate
        val statusCount = x.getAs[Long](Constants.STATUS_COUNT)
        writer.write(Constants.STATUS_COUNT + "(" + user + "," + statusCount + ")")

        // friendsCount predicate
        val friendsCount = x.getAs[Int](Constants.FRIENDS_COUNT)
        writer.write(Constants.FRIENDS_COUNT + "(" + user + "," + friendsCount + ")")

        // followersCount Predicate
        val followersCount = x.getAs[Int](Constants.FOLLOWERS_COUNT)
        writer.write(Constants.FOLLOWERS_COUNT + "(" + user + "," + followersCount + ")")

        // containsLink Predicate
        var links = x.getAs[Seq[String]](Constants.URL)
        if(links != null && links.length > 0) {
          for(link <- links) {
            writer.write(Constants.CONTAINS_LINK + "(" + tweetId + "," + link + ")")
          }
        }

        // mentions Predicate
        var metionedLst = x.getAs[Seq[Row]](Constants.MENTIONED_LST)
        if(metionedLst != null && metionedLst.length > 0) {
          for(mentions <- metionedLst) {
            writer.write(Constants.MENTIONS + "(" + tweetId + "," + mentions.getAs[Long]("id") + ")")
          }
        }

        // containsHashtag Predicate
        var hashtags = x.getAs[Seq[Row]](Constants.HASHTAGS)
        if(hashtags != null && hashtags.length > 0) {
          for(hashtag <- hashtags) {
            writer.write(Constants.CONTAINS_HASHTAG + "(" + tweetId + "," + hashtag.getAs[String]("text") + ")")
          }
        }

      })

      // Starting the Streaming process.
      print("CollectTweets :: getTwitterData :: Collecting Twitter tweets via streaming APIs...")
    } catch {
      case e: Exception =>
        System.out.println("GenerateEvidence :: constructEvidence :: Exception encountered while writing to the file")
        e.printStackTrace()
        System.exit(-1)
    } finally {
      try {
        reader.close()
      } catch {
        case e:Exception => {
          System.out.println("GenerateEvidence :: constructEvidence :: Exception encountered while closing the BufferedReader")
          System.exit(-1)
        }
      }
      try {
        writer.close()
      } catch {
        case e:Exception => {
          System.out.println("GenerateEvidence :: constructEvidence :: Exception encountered while closing the BufferedWriter")
          System.exit(-1)
        }
      }
    }
  }
}
