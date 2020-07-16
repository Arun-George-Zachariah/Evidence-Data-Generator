package edu.missouri

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}

import twitter4j.conf.ConfigurationBuilder
import twitter4j.{IDs, TwitterException, TwitterFactory}

object CollectFriendsAndFollowers {
  def writeEvidence(user: Long, consumerKey: String, consumerSecret: String, accessToken: String, accessTokenSecret: String, listType: String, outFile: String): Unit = {
    System.out.println("CollectFriendsAndFollowers :: writeEvidence :: user :: " + user + " :: listType :: " + listType + " :: outFile :: " + outFile)

    var collectedIds = null: IDs
    var writer:BufferedWriter = null

    // Setting the configurations.
    val configurationBuilder = new ConfigurationBuilder
    configurationBuilder.setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
      .setUseSSL(true)

    // Creating an instance of TwitterFactory.
    val twitterFactory = new TwitterFactory(configurationBuilder.build)
    val twitterInstance = twitterFactory.getInstance

    try {
      writer = new BufferedWriter(new FileWriter(new File(outFile), true))

      while(true) {
        try {
          if (listType.equalsIgnoreCase(Constants.Constants.FRIENDS)) {
            System.out.println("CollectFriendsAndFollowers :: writeEvidence :: collecting friends for the user :: " + user)
            collectedIds = twitterInstance.getFriendsIDs(user, -1)
          } else if (listType.equalsIgnoreCase(Constants.Constants.FOLLOWERS)) {
            System.out.println("CollectFriendsAndFollowers :: writeEvidence :: collecting followers for the user :: " + user)
            collectedIds = twitterInstance.getFollowersIDs(user, -1)
          } else {
            System.out.println("CollectFriendsAndFollowers :: writeEvidence :: Unidentified Type.")
            return
          }

          // Writes a list of upto 5000 friends and followers. (Reference http://twitter4j.org/oldjavadocs/2.2.6/twitter4j/api/FriendsFollowersMethods.html)
          val ids = collectedIds.getIDs.iterator

          for ( id <- ids) {
            System.out.println("CollectFriendsAndFollowers :: writeEvidence :: Adding :: " + listType + " :: " + id)
            writer.write(listType + "(" + user + "," + id + ")" + "\n")
            writer.flush()
          }

          System.out.println("CollectFriendsAndFollowers :: writeEvidence :: Completed collecting " + listType + " for the user :: " + user)
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
              Thread.sleep((waitTime + 20) * 1000)
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
      twitterInstance.shutdown()
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

  def constructEvidence(inFile: String, outFile: String, consumerKey: String, consumerSecret: String, accessToken: String, accessTokenSecret: String): Unit = {
    var reader:BufferedReader = null
    var userId: String = null

    try {
      // Reading user id's from the file.
      reader = new BufferedReader(new FileReader(new File(inFile)))

      while ({userId = reader.readLine; userId != null}) {
        // Collecting friends.
        writeEvidence(userId.toLong, consumerKey, consumerSecret, accessToken, accessTokenSecret, Constants.Constants.FRIENDS, outFile)

        // Collecting followers.
        writeEvidence(userId.toLong, consumerKey, consumerSecret, accessToken, accessTokenSecret, Constants.Constants.FOLLOWERS, outFile)
      }

      print("CollectFriendsAndFollowers :: constructEvidence :: Completed constructing the evidence data.")
    } catch {
      case e: Exception =>
        System.out.println("CollectFriendsAndFollowers :: constructEvidence :: Exception encountered while writing to the file for the user id :: " + userId)
        e.printStackTrace()
        System.exit(-1)
    } finally {
      try {
        reader.close()
      } catch {
        case e:Exception => {
          System.out.println("CollectFriendsAndFollowers :: constructEvidence :: Exception encountered while closing the BufferedReader for the user id :: " + userId)
          e.printStackTrace()
          System.exit(-1)
        }
      }
    }
  }

  def main(args: Array[String]) = {
    // Validating input arguments.
    if (args.length != 6) {
      println("Usage: CollectFriendsAndFollowers " + "<USER_ID_IN_FILE> <EVIDENCE_OUT_FILE> <CONSUMER_KEY> <CONSUMER_SECRET> <ACCESS_TOKEN> <ACCESS_TOKEN_SECRET>")
      System.exit(-1)
    } else {
      // Constructing the evidence.
      constructEvidence(args(0), args(1), args(2), args(3), args(4), args(5))
    }
  }
}
