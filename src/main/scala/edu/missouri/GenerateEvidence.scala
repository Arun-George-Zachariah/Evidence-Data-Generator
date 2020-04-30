package edu.missouri

import org.apache.spark.sql.SparkSession

class GenerateEvidence {
  def main(args: Array[String]) = {
    //Validating the input.
    if (args.length < 4) {
      println("Usage: GenerateEvidence " + "<input_tweets_directory> " + "<domains_file> <urls_file> <output_file> <collect_rel_bool> {rao,katib}")
      System.exit(-1)
    }

    val tweetDataPath = args(0)

    // Creating the Spark SQL context.
    val sqlContext = SparkSession.builder.master("local").getOrCreate()

    // Loading the tweets file.
    val myTweets = sqlContext.read.json(tweetDataPath)
    myTweets.createOrReplaceTempView("Tweets")

    // Forming the Spark SQL query.
    var query = "SELECT id as t, retweetCount, isPossiblySensitive as sensitive, " +
      "userMentionEntities as mentioned, " +
      "hashtagEntities as hashtags, " +
      "urlEntities.expandedURL as fullLinks, " +
      "user.id as user, " +
      "user.isVerified as isVerified, " +
      "user.followersCount as followersCount, " +
      "user.friendsCount as friendsCount, " +
      "user.statusesCount as statusesCount, " +
      "retweetedStatus.id as rsT, " +
      "retweetedStatus.retweetCount as rsRetweetCount, " +
      "retweetedStatus.user.id as rsUser " +
      "FROM Tweets"

    var results = sqlContext.sql(query)
    //results.registerTempTable("Tweets")

    results.foreach(x => {
      val usr = x.getAs[Long]("user")
      val tid = x.getAs[Long]("t")
      val verified = x.getAs[Boolean]("isVerified")
      val retweetedStatusTid = x.getAs[Long]("rsT")

      println("Output :: usr :: " + usr + " :: tid :: " + tid)

    })

  }
}
