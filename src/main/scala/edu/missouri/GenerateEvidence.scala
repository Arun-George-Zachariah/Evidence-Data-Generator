package edu.missouri

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object GenerateEvidence {
  def constructEvidence(inFile: String, outFile: String): Unit = {
    // Defining the Spark SQL Context.
    val sqlContext = SparkSession.builder.master("local[*]").appName(Constants.Constants.APP_NAME).getOrCreate()

    var reader:BufferedReader = null
    var writer:BufferedWriter = null
    try {
      reader = new BufferedReader(new FileReader(new File(inFile)))
      writer = new BufferedWriter(new FileWriter(new File(outFile)))

      // Loading the tweets to a table.
      val myTweets = sqlContext.read.json(inFile)
      myTweets.createOrReplaceTempView(Constants.Constants.TWEETS_VIEW)

      // Querying the tweets.
      var results = sqlContext.sql(Constants.Constants.SELECT_QUERY)

      // Creating a string build and broadcasting it across workers.
      val driverSB = StringBuilder.newBuilder
      val sbBroadcast = sqlContext.sparkContext.broadcast(driverSB)

      results.foreach(x => {
        val sb = new StringBuilder()

        // tweeted predicate
        val user = x.getAs[Long](Constants.Constants.USER)
        val tweetId = x.getAs[Long](Constants.Constants.TWEET_ID)
        sb.append(Constants.Constants.TWEETED + "(" + user + "," + tweetId + ")" + "\n")

        // verified predicate
        val isVerified = x.getAs[Boolean](Constants.Constants.VERIFIED)
        if (isVerified) {
          sb.append(Constants.Constants.VERIFIED + "(" + user + ")" + "\n")
        }

        val retweetedStatusId = x.getAs[Long](Constants.Constants.RETWEETED_STATUS_ID)
        if (retweetedStatusId != 0) {
          // retweeted predicate
          sb.append(Constants.Constants.RETWEETED + "(" + user + "," + retweetedStatusId + ")" + "\n")

          // tweeted predicate
          val retweetedStatusUserId = x.getAs[Long](Constants.Constants.RETWEETED_STATUS_USER_ID)
          sb.append(Constants.Constants.TWEETED + "(" + retweetedStatusUserId + "," + retweetedStatusId + ")" + "\n")

          // retweetCount predicate
          val retweetedStatusRetweetCount = x.getAs[Long](Constants.Constants.RETWEETED_STATUS_RETWEET_COUNT)
          sb.append(Constants.Constants.RETWEET_COUNT + "(" + retweetedStatusId + "," + retweetedStatusRetweetCount + ")" + "\n")
        }

        // isPossiblySensitive predicate
        val isPossiblySensitive = x.getAs[Boolean](Constants.Constants.IS_POSSIBLY_SENSITIVE)
        if (isPossiblySensitive) {
          sb.append(Constants.Constants.IS_POSSIBLY_SENSITIVE + "(" + tweetId + ")" + "\n")
        }

        // statusesCount predicate
        val statusCount = x.getAs[Long](Constants.Constants.STATUS_COUNT)
        sb.append(Constants.Constants.STATUS_COUNT + "(" + user + "," + statusCount + ")" + "\n")

        // friendsCount predicate
        val friendsCount = x.getAs[Long](Constants.Constants.FRIENDS_COUNT)
        sb.append(Constants.Constants.FRIENDS_COUNT + "(" + user + "," + friendsCount + ")" + "\n")

        // followersCount Predicate
        val followersCount = x.getAs[Long](Constants.Constants.FOLLOWERS_COUNT)
        sb.append(Constants.Constants.FOLLOWERS_COUNT + "(" + user + "," + followersCount + ")" + "\n")

        // containsLink Predicate
        var links = x.getAs[Seq[String]](Constants.Constants.URL)
        if(links != null && links.length > 0) {
          for(link <- links) {
            sb.append(Constants.Constants.CONTAINS_LINK + "(" + tweetId + "," + link + ")" + "\n")
          }
        }

        // mentions Predicate
        var metionedLst = x.getAs[Seq[Row]](Constants.Constants.MENTIONED_LST)
        if(metionedLst != null && metionedLst.length > 0) {
          for(mentions <- metionedLst) {
            sb.append(Constants.Constants.MENTIONS + "(" + tweetId + "," + mentions.getAs[Long]("id") + ")" + "\n")
          }
        }

        // containsHashtag Predicate
        var hashtags = x.getAs[Seq[Row]](Constants.Constants.HASHTAGS)
        if(hashtags != null && hashtags.length > 0) {
          for(hashtag <- hashtags) {
            sb.append(Constants.Constants.CONTAINS_HASHTAG + "(" + tweetId + "," + hashtag.getAs[String]("text") + ")" + "\n")
          }
        }

        // Writing to the broadcasted string builder.
        sbBroadcast.value.append(sb.toString())
      })

      // Consolidating all the evidence to a file.
      writer.write(sbBroadcast.value.toString())
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
    if (args.length != 2) {
      println("Usage: GenerateEvidence " + "<TWEETS_FILE> <EVIDENCE_OUT_FILE>")
      System.exit(-1)
    } else {
      // Note: The string evidence needs to be converted to a data frame and then written to a file.
      constructEvidence(args(0), args(1))
    }
  }

}
