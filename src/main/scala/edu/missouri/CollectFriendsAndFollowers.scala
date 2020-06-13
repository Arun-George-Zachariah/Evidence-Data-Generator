package edu.missouri

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}
import org.apache.spark.sql.SparkSession
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{IDs, Twitter, TwitterException, TwitterFactory}
import scala.collection.mutable.ListBuffer

object CollectFriendsAndFollowers {
  def getList(user: Long, twitterInstance: Twitter, listType: String): List[Long] = {
    System.out.println("CollectFriendsAndFollowers :: getList :: user :: " + user + " listType :: " + listType)

    var statuses = null: IDs
    var retLst  = ListBuffer[Long]()

    while(true) {
      try {
        if (listType.equalsIgnoreCase(Constants.Constants.FRIENDS)) {
          statuses = twitterInstance.getFriendsIDs(user)
        } else if (listType.equalsIgnoreCase(Constants.Constants.FOLLOWERS)) {
          statuses = twitterInstance.getFollowersIDs(user)
        } else {
          System.out.println("CollectFriendsAndFollowers :: getList :: Unidentified Type.")
          return null
        }
        // Return a list of upto 5000 friends and followers. (Reference http://twitter4j.org/oldjavadocs/2.2.6/twitter4j/api/FriendsFollowersMethods.html)
        return retLst.toList
      } catch {
        case e: TwitterException => {
          System.out.println("CollectFriendsAndFollowers :: getList :: Twitter exception while proccessing user id :: " + user)

          // Return back, if the user id is not found.
          if(e.resourceNotFound() == true && e.getErrorCode() == 34 && e.getStatusCode() == 404){
            System.out.println("CollectFriendsAndFollowers :: getList :: User not found.")
            return null
          }

          // Waiting for limit to be reset.
          var waitTime = Math.abs(e.getRateLimitStatus.getSecondsUntilReset)
          Thread.sleep(waitTime * 1000)
        }
      }
    }
    return null
  }

  def constructEvidence(inFile: String, outFile: String, twitterInstance: Twitter): Unit = {
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

      results.foreach(x => {
        // Getting the user id.
        val user = x.getAs[Long](Constants.Constants.USER)

        // Getting the verified predicate.
        val isVerified = x.getAs[Boolean](Constants.Constants.VERIFIED)

        // Getting the isPossiblySensitive predicate.
        val isPossiblySensitive = x.getAs[Boolean](Constants.Constants.IS_POSSIBLY_SENSITIVE)


        if (isVerified || isPossiblySensitive) {
          // Collecting the list of friends.
          val friends = getList(user, twitterInstance, Constants.Constants.FRIENDS)
          for (friend <- friends) {
            // Writing each friend to the evidence file.
            writer.write(Constants.Constants.FRIENDS + "(" + user + "," + friend + ")")
          }

          // Collecting the list of friends.
          val followers = getList(user, twitterInstance, Constants.Constants.FRIENDS)
          for (follower <- followers) {
            // Writing each follower to the evidence file.
            writer.write(Constants.Constants.FOLLOWERS + "(" + user + "," + follower + ")")
          }
        }
      })

      print("CollectFriendsAndFollowers :: constructEvidence :: Completed constructing the evidence data.")
    } catch {
      case e: Exception =>
        System.out.println("CollectFriendsAndFollowers :: constructEvidence :: Exception encountered while writing to the file")
        e.printStackTrace()
        System.exit(-1)
    } finally {
      try {
        reader.close()
      } catch {
        case e:Exception => {
          System.out.println("CollectFriendsAndFollowers :: constructEvidence :: Exception encountered while closing the BufferedReader")
          System.exit(-1)
        }
      }
      try {
        writer.close()
      } catch {
        case e:Exception => {
          System.out.println("CollectFriendsAndFollowers :: constructEvidence :: Exception encountered while closing the BufferedWriter")
          System.exit(-1)
        }
      }
    }
  }

  def main(args: Array[String]) = {
    // Validating input arguments.
    if (args.length != 1) {
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
