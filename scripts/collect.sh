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

# Splitting the input into N splits.
split -l $(((`cat $TWEETS_FILE | wc -l`/$N) + 1)) --numeric-suffixes=1 --additional-suffix=".json" $TWEETS_FILE ../data/data_in/

## Iterating over the splits and starting the process
for ((i=1;i<=$N;i++)); do
  # Obtaining the file name
  if (( i < 10 )); then
    file=../data/data_in/0$i.json
  else
    file=../data/data_in/$i.json
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

#To Do: A way to get a list of failed user id's.

echo "Completed collecting friends and followers"