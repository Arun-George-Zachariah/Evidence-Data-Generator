package edu.missouri

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object GenerateEvidence {
  def constructEvidence(inFile: String): Unit = {
    // Defining the Spark SQL Context.
    val sqlContext = SparkSession.builder.master("local[*]").appName(Constants.Constants.APP_NAME).getOrCreate()

    var reader:BufferedReader = null
    var writer:BufferedWriter = null
    try {
      reader = new BufferedReader(new FileReader(new File(inFile)))
      writer = new BufferedWriter(new FileWriter(new File("out/evidence.db")))

      // Loading the tweets to a table.
      val myTweets = sqlContext.read.json(inFile)
      myTweets.createOrReplaceTempView(Constants.Constants.TWEETS_VIEW)

      // Querying the tweets.
      var results = sqlContext.sql(Constants.Constants.SELECT_QUERY)

      results.foreach(x => {
        // tweeted predicate
        val user = x.getAs[Long](Constants.Constants.USER)
        val tweetId = x.getAs[Long](Constants.Constants.TWEET_ID)
        writer.write(Constants.Constants.TWEETED + "(" + user + "," + tweetId + ")")

        // verified predicate
        val isVerified = x.getAs[Boolean](Constants.Constants.VERIFIED)
        if (isVerified) {
          writer.write(Constants.Constants.VERIFIED + "(" + user + ")")
        }

        val retweetedStatusId = x.getAs[Long](Constants.Constants.RETWEETED_STATUS_ID)
        if (retweetedStatusId != null) {
          // retweeted predicate
          writer.write(Constants.Constants.RETWEETED + "(" + user + "," + retweetedStatusId + ")")

          // tweeted predicate
          val retweetedStatusUserId = x.getAs[Long](Constants.Constants.RETWEETED_STATUS_USER_ID)
          writer.write(Constants.Constants.TWEETED + "(" + retweetedStatusUserId + "," + retweetedStatusId + ")")

          // retweetCount predicate
          val retweetedStatusRetweetCount = x.getAs[Long](Constants.Constants.RETWEET_COUNT)
          writer.write(Constants.Constants.RETWEET_COUNT + "(" + retweetedStatusId + "," + retweetedStatusRetweetCount + ")")
        }

        // isPossiblySensitive predicate
        val isPossiblySensitive = x.getAs[Boolean](Constants.Constants.IS_POSSIBLY_SENSITIVE)
        if (isPossiblySensitive) {
          writer.write(Constants.Constants.IS_POSSIBLY_SENSITIVE + "(" + tweetId + ")")
        }

        // statusesCount predicate
        val statusCount = x.getAs[Long](Constants.Constants.STATUS_COUNT)
        writer.write(Constants.Constants.STATUS_COUNT + "(" + user + "," + statusCount + ")")

        // friendsCount predicate
        val friendsCount = x.getAs[Int](Constants.Constants.FRIENDS_COUNT)
        writer.write(Constants.Constants.FRIENDS_COUNT + "(" + user + "," + friendsCount + ")")

        // followersCount Predicate
        val followersCount = x.getAs[Int](Constants.Constants.FOLLOWERS_COUNT)
        writer.write(Constants.Constants.FOLLOWERS_COUNT + "(" + user + "," + followersCount + ")")

        // containsLink Predicate
        var links = x.getAs[Seq[String]](Constants.Constants.URL)
        if(links != null && links.length > 0) {
          for(link <- links) {
            writer.write(Constants.Constants.CONTAINS_LINK + "(" + tweetId + "," + link + ")")
          }
        }

        // mentions Predicate
        var metionedLst = x.getAs[Seq[Row]](Constants.Constants.MENTIONED_LST)
        if(metionedLst != null && metionedLst.length > 0) {
          for(mentions <- metionedLst) {
            writer.write(Constants.Constants.MENTIONS + "(" + tweetId + "," + mentions.getAs[Long]("id") + ")")
          }
        }

        // containsHashtag Predicate
        var hashtags = x.getAs[Seq[Row]](Constants.Constants.HASHTAGS)
        if(hashtags != null && hashtags.length > 0) {
          for(hashtag <- hashtags) {
            writer.write(Constants.Constants.CONTAINS_HASHTAG + "(" + tweetId + "," + hashtag.getAs[String]("text") + ")")
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

  def main(args: Array[String]) = {
    // Validating input arguments.
    if (args.length != 1) {
      println("Usage: GenerateEvidence " + "<TWEETS_FILE> ")
      System.exit(-1)
    } else {
      constructEvidence(args(0))
    }
  }

}
