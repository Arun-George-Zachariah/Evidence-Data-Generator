package edu.missouri

import java.io.{File, FileWriter}

import org.apache.spark.sql.SparkSession

object GenerateTweetIds {
  def main(args: Array[String]) = {
    // Validating input arguments.
    if (args.length != 2) {
      println("Usage: GenerateTweetIds " + "<TWEETS_IN_FILE> <TWEETS_OUT_FILE>")
      System.exit(-1)
    } else {
      getUserIds(args(0), args(1))
    }
  }

  def getUserIds(inFile: String, outFile: String): Unit = {
    System.out.println("GenerateTweetIds :: getUserIds :: Start")

    // Creating a Spark SQL context.
    val sqlContext = SparkSession.builder.master("local[1]").appName(Constants.Constants.APP_NAME).getOrCreate()

    // Loading the tweets to a table.
    val myTweets = sqlContext.read.json(inFile)
    myTweets.createOrReplaceTempView(Constants.Constants.TWEETS_VIEW)

    // Querying the tweets.
    var results = sqlContext.sql(Constants.Constants.TWEET_ID_QUERY)

    results.foreach(x => {
      var writer: FileWriter = null
      try {
        writer = new FileWriter(new File(outFile), true)
        // Writing the user id to a file.
        writer.write(x.getAs[Long](Constants.Constants.TWEET_ID) + "\n")
        writer.flush()
      } catch {
        case e: Exception =>
          System.out.println("GenerateTweetIds :: userIdList :: Exception encountered while writing to the file")
          e.printStackTrace()
          System.exit(-1)
      } finally {
        try {
          writer.close()
        } catch {
          case e: Exception => {
            System.out.println("GenerateTweetIds :: userIdList :: Exception encountered while closing the FileWriter")
            e.printStackTrace()
            System.exit(-1)
          }
        }
      }
    })
    System.out.println("GenerateTweetIds :: getUserIds :: End")
  }
}
