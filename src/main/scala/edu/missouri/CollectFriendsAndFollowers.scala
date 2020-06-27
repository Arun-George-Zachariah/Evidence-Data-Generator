package edu.missouri

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}
import org.apache.spark.sql.SparkSession
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{IDs, Twitter, TwitterException, TwitterFactory}

object CollectFriendsAndFollowers {
  def writeEvidence(user: Long, twitterInstance: Twitter, listType: String, outFile: String): Unit = {
    System.out.println("CollectFriendsAndFollowers :: writeEvidence :: user :: " + user + " :: listType :: " + listType + " :: outFile :: " + outFile)

    var collectedIds = null: IDs

    var writer:BufferedWriter = null
    try {
      writer = new BufferedWriter(new FileWriter(new File(outFile), true))

      while(true) {
        try {
          if (listType.equalsIgnoreCase(Constants.Constants.FRIENDS)) {
            collectedIds = twitterInstance.getFriendsIDs(user, -1)
          } else if (listType.equalsIgnoreCase(Constants.Constants.FOLLOWERS)) {
            collectedIds = twitterInstance.getFollowersIDs(user, -1)
          } else {
            System.out.println("CollectFriendsAndFollowers :: writeEvidence :: Unidentified Type.")
            return
          }

          // Writes a list of upto 5000 friends and followers. (Reference http://twitter4j.org/oldjavadocs/2.2.6/twitter4j/api/FriendsFollowersMethods.html)
          var ids = collectedIds.getIDs.iterator
          for ( id <- ids) {
            System.out.println("CollectFriendsAndFollowers :: writeEvidence :: Adding :: " + listType + " :: " + id)
            writer.write(listType + "(" + user + "," + id + ")" + "\n")
            writer.flush()
          }

          return
        } catch {
          case e: TwitterException => {
            System.out.println("CollectFriendsAndFollowers :: getList :: Exception encountered :: ")
            e.printStackTrace()
            System.out.println("CollectFriendsAndFollowers :: getList :: Twitter exception while processing user id :: " + user)

            // Waiting for the limit to be replenished.
            if(e.getRateLimitStatus != null) {
              var waitTime = Math.abs(e.getRateLimitStatus.getSecondsUntilReset)
              System.out.println("CollectFriendsAndFollowers :: getList :: Waiting for :: " + waitTime + " :: seconds until rate limit is reset.")
              Thread.sleep(waitTime * 1000)
            } else {
              System.out.println("CollectFriendsAndFollowers :: getList :: Irrevocable twitter exception while processing user id :: " + user)
              return
            }

          } case e: Exception => {
            System.out.println("CollectFriendsAndFollowers :: writeEvidence :: Unknown exception encountered for the user id :: " + user)
            e.printStackTrace()
            return
          }
        }
      }

    } catch {
      case e: Exception =>
        System.out.println("CollectFriendsAndFollowers :: writeEvidence :: Exception encountered while writing to the file for the user id :: " + user)
        e.printStackTrace()
        System.exit(-1)
    } finally {
      try {
        writer.close()
      } catch {
        case e:Exception => {
          System.out.println("CollectFriendsAndFollowers :: writeEvidence :: Exception encountered while closing the BufferedWriter for the user id :: " + user)
          e.printStackTrace()
          System.exit(-1)
        }
      }
    }
  }

  def constructEvidence(inFile: String, outFile: String, twitterInstance: Twitter): Unit = {
    // Defining the Spark and Spark SQL Context.
    val sqlContext = SparkSession.builder.master("local[1]").appName(Constants.Constants.APP_NAME).getOrCreate()

    var reader:BufferedReader = null
    var user: Long = 0
    try {
      reader = new BufferedReader(new FileReader(new File(inFile)))

      // Loading the tweets to a table.
      val myTweets = sqlContext.read.json(inFile)
      myTweets.createOrReplaceTempView(Constants.Constants.TWEETS_VIEW)

      // Querying the tweets.
      var results = sqlContext.sql(Constants.Constants.FF_QUERY)

      results.foreach(x => {
        // Getting the user id.
        user = x.getAs[Long](Constants.Constants.USER)

        // Getting the verified predicate.
        val isVerified = x.getAs[Boolean](Constants.Constants.VERIFIED)

        // Getting the isPossiblySensitive predicate.
        val isPossiblySensitive = x.getAs[Boolean](Constants.Constants.IS_POSSIBLY_SENSITIVE)


        if (isVerified || isPossiblySensitive) {
          // Collecting friends.
          writeEvidence(user, twitterInstance, Constants.Constants.FRIENDS, outFile)

          // Collecting followers.
          writeEvidence(user, twitterInstance, Constants.Constants.FOLLOWERS, outFile)
        }
      })

      print("CollectFriendsAndFollowers :: constructEvidence :: Completed constructing the evidence data.")
    } catch {
      case e: Exception =>
        System.out.println("CollectFriendsAndFollowers :: constructEvidence :: Exception encountered while writing to the file for the user id :: " + user)
        e.printStackTrace()
        System.exit(-1)
    } finally {
      try {
        reader.close()
      } catch {
        case e:Exception => {
          System.out.println("CollectFriendsAndFollowers :: constructEvidence :: Exception encountered while closing the BufferedReader for the user id :: " + user)
          e.printStackTrace()
          System.exit(-1)
        }
      }
    }
  }

  def main(args: Array[String]) = {
    // Validating input arguments.
    if (args.length != 6) {
      println("Usage: CollectFriendsAndFollowers " + "<TWEETS_IN_FILE> <TWEETS_OUT_FILE> <CONSUMER_KEY> <CONSUMER_SECRET> <ACCESS_TOKEN> <ACCESS_TOKEN_SECRET>")
      System.exit(-1)
    } else {
      // Setting the configurations.
      val configurationBuilder = new ConfigurationBuilder
      configurationBuilder.setDebugEnabled(true)
        .setOAuthConsumerKey(args(2))
        .setOAuthConsumerSecret(args(3))
        .setOAuthAccessToken(args(4))
        .setOAuthAccessTokenSecret(args(5))
        .setUseSSL(true)

      // Creating an instance of TwitterFactory.
      val twitterFactory = new TwitterFactory(configurationBuilder.build)
      val twitterInstance = twitterFactory.getInstance
      constructEvidence(args(0), args(1), twitterInstance)
    }
  }

}
