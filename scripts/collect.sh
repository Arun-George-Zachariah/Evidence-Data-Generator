#!/bin/bash

# Declaring the variables.
N=1
TWEETS_FILE="tweets.json"

# Usage.
usage()
{
    echo "usage: collect.sh [-tweets TWEETS_FILE] [-n THREADS] [-h | --help]"
}

# Read input parameters.
if [ "$1" == "" ]; then usage; exit 1; fi
while [ "$1" != "" ]; do
    case $1 in
    	-tweets)
    	  shift
        TWEETS_FILE=$1
        ;;
      -n)
        shift
        N=$1
        ;;
      -h | --help )
        usage
        exit
        ;;
        * )
        usage
        exit
    esac
    shift
done

# Deleting and recreating a temporary directory for the tweets.
rm -rvf ../data ../data/data_in ../data/data_tmp ../data/data_out data && mkdir ../data ../data/data_in ../data/data_tmp ../data/data_out

# Reading the configuration file.
. ../conf/app.config

# Obtaining the user ids from the tweets.
echo "Collecting user id's from the tweets."
spark-submit --class edu.missouri.GenerateUserIds ../target/scala-2.11/Evidence-Data-Generator-assembly-0.1.jar $TWEETS_FILE ../data/data_tmp/user_ids.txt

# Splitting the user id's into N splits.
echo "Splitting the user id's"
split -l $(((`cat ../data/data_tmp/user_ids.txt | wc -l`/$N) + 1)) --numeric-suffixes=1 --additional-suffix=".txt" ../data/data_tmp/user_ids.txt ../data/data_in/

# Iterating over the splits and starting the process
for ((i=1;i<=$N;i++)); do
  # Obtaining the file name
  if (( i < 10 )); then
    file=../data/data_in/0$i.txt
  else
    file=../data/data_in/$i.txt
  fi

  # Obtaining the secret keys.
  consumerKey="CONSUMER_KEY_"$i
  consumerSecret="CONSUMER_SECRET_"$i
  accessToken="ACCESS_TOKEN_"$i
  accessTokenSecret="ACCESS_TOKEN_SECRET_"$i

  # Start the process
  echo "Collecting friends and followers for the split "$i
  spark-submit --class edu.missouri.CollectFriendsAndFollowers ../target/scala-2.11/Evidence-Data-Generator-assembly-0.1.jar $file ../data/data_tmp/evidence_$i.db ${!consumerKey} ${!consumerSecret} ${!accessToken} ${!accessTokenSecret} &
done

# Waiting for all the process to finish.
wait

# Consolidating all the evidence constructed.
cat ../data/data_tmp/evidence_*.db >> ../data/data_out/evidence.db

echo "Completed collecting friends and followers"