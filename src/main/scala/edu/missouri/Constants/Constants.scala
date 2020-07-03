package edu.missouri.Constants

object Constants {
  // General Constants
  val APP_NAME = "Evidence-Data-Generator"
  val TWEETS_VIEW = "Tweets"
  val USER = "user"
  val TWEET_ID = "tweetId"
  val RETWEETED_STATUS_ID = "retweetedStatusId"
  val RETWEETED_STATUS_USER_ID = "retweetedStatusUserId"
  val URL = "url"
  val MENTIONED_LST = "mentionedLst"
  val HASHTAGS = "hashtags"
  val FRIENDS = "friend"
  val FOLLOWERS = "isFollowedBy"

  // Predicate Constants
  val TWEETED = "tweeted"
  val CONTAINS_LINK = "containsLink"
  val MENTIONS = "mentions"
  val RETWEETED = "retweeted"
  val CONTAINS_HASHTAG = "containsHashtag"
  val VERIFIED = "verified"
  val IS_POSSIBLY_SENSITIVE = "isPossiblySensitive"
  val FRIENDS_COUNT = "friendsCount"
  val FOLLOWERS_COUNT = "followersCount"
  val STATUS_COUNT = "statusesCount"
  val RETWEET_COUNT = "retweetCount"

  // Query Constants
  var SELECT_QUERY = "SELECT id as " + TWEET_ID + ", " +
    "retweetCount as " + RETWEET_COUNT + ", " +
    "isPossiblySensitive as " + IS_POSSIBLY_SENSITIVE + ", " +
    "userMentionEntities as " + MENTIONED_LST + ", " +
    "hashtagEntities as  " + HASHTAGS + ", " +
    "urlEntities.expandedURL as " + URL + ", " +
    "user.id as " + USER + ", " +
    "user.isVerified as " + VERIFIED + ", " +
    "user.followersCount as " + FOLLOWERS_COUNT + ", " +
    "user.friendsCount as " + FRIENDS_COUNT + ", " +
    "user.statusesCount as " + STATUS_COUNT + ", " +
    "retweetedStatus.id as " + RETWEETED_STATUS_ID + ", " +
    "retweetedStatus.retweetCount as " + RETWEET_COUNT + ", " +
    "retweetedStatus.user.id as " + RETWEETED_STATUS_USER_ID + " " +
    "FROM " + TWEETS_VIEW

  var FF_QUERY = "SELECT isPossiblySensitive as " + IS_POSSIBLY_SENSITIVE + ", " +
    "user.id as " + USER + ", " +
    "user.isVerified as " + VERIFIED + " " +
    "FROM " + TWEETS_VIEW

  var USER_ID_QUERY = "SELECT DISTINCT user.id as " + USER + " FROM " + TWEETS_VIEW
}
