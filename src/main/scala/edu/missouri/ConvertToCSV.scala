package edu.missouri

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}

import org.apache.spark.sql.{Row, SparkSession}

object ConvertToCSV {

  def convertToCSV(inFile: String, outFile: String): Unit = {
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

      // Creating a string builder.
      val driverSB = StringBuilder.newBuilder

      // Adding the header details.
      driverSB.append("UserId,TweetId,Verified,RetweetedStatusId,RetweetedUserId,RetweetedStatusRetweetCount,PossiblySensitive,StatusCount,FriendsCount,FollowersCount,Link,Mentions,Hashtag")

      // Broadcasting the string builder across workers.
      val sbBroadcast = sqlContext.sparkContext.broadcast(driverSB)

      results.foreach(x => {
        val sb = new StringBuilder()

        // user predicate
        val user = x.getAs[Long](Constants.Constants.USER)
        sb.append(user + ",")

        // tweeted predicate
        val tweetId = x.getAs[Long](Constants.Constants.TWEET_ID)
        sb.append(tweetId + ",")

        // verified predicate
        val isVerified = x.getAs[Boolean](Constants.Constants.VERIFIED)
        sb.append(isVerified + ",")

        val retweetedStatusId = x.getAs[Long](Constants.Constants.RETWEETED_STATUS_ID)
        if (retweetedStatusId != 0) {
          // retweeted predicate
          sb.append(retweetedStatusId + ",")

          // tweeted predicate
          val retweetedStatusUserId = x.getAs[Long](Constants.Constants.RETWEETED_STATUS_USER_ID)
          sb.append(retweetedStatusUserId + ",")

          // retweetCount predicate
          val retweetedStatusRetweetCount = x.getAs[Long](Constants.Constants.RETWEETED_STATUS_RETWEET_COUNT)
          sb.append(retweetedStatusRetweetCount + ",")
        }

        // isPossiblySensitive predicate
        val isPossiblySensitive = x.getAs[Boolean](Constants.Constants.IS_POSSIBLY_SENSITIVE)
        sb.append(isPossiblySensitive + ",")

        // statusesCount predicate
        val statusCount = x.getAs[Long](Constants.Constants.STATUS_COUNT)
        sb.append(statusCount + ",")

        // friendsCount predicate
        val friendsCount = x.getAs[Long](Constants.Constants.FRIENDS_COUNT)
        sb.append(friendsCount + ",")

        // followersCount Predicate
        val followersCount = x.getAs[Long](Constants.Constants.FOLLOWERS_COUNT)
        sb.append(followersCount + ",")

        // containsLink Predicate
        var links = x.getAs[Seq[String]](Constants.Constants.URL)
        if(links != null && links.length > 0) {
          var lastLink = ""
          for(link <- links) {
            //TODO Need to understand how to add URL's in the CSV, as the size would vary.
            lastLink = link
          }
          sb.append(lastLink + ",")
        }

        // mentions Predicate
        var mentionedLst = x.getAs[Seq[Row]](Constants.Constants.MENTIONED_LST)
        if(mentionedLst != null && mentionedLst.length > 0) {
          var lastMentions = -1L
          for(mentions <- mentionedLst) {
            //TODO Need to understand how to add mentions in the CSV, as the size would vary.
            lastMentions = mentions.getAs[Long]("id")
          }
          sb.append(lastMentions + ",")
        }

        // containsHashtag Predicate
        var hashtags = x.getAs[Seq[Row]](Constants.Constants.HASHTAGS)
        if(hashtags != null && hashtags.length > 0) {
          var lastHashtag = ""
          for(hashtag <- hashtags) {
            //TODO Need to understand how to add hashtags in the CSV, as the size would vary.
            lastHashtag = hashtag.getAs[String]("text")
          }
          sb.append(lastHashtag)
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
      println("Usage: ConvertToCSV " + "<TWEETS_FILE> <CSV_OUT_FILE>")
      System.exit(-1)
    } else {
      // Note: The string evidence needs to be converted to a data frame and then written to a file.
      convertToCSV(args(0), args(1))
    }
  }
}
