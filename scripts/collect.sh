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
rm -rvf data_in data_out data && mkdir data_in data_out

# Reading the configuration file.
. ../conf/app.config

# Splitting the input into N splits.
split -l $((`cat $TWEETS_FILE | wc -l`/$N)) --numeric-suffixes=1 --additional-suffix=".json" $TWEETS_FILE data_in/

## Iterating over the splits and starting the process
for ((i=1;i<=$N;i++)); do
  # Obtaining the file name
  if (( i < 10 )); then
    file=data_in/0$i.json
  else
    file=data_in/$i.json
  fi

  # Obtaining the secret keys.
  consumerKey="CONSUMER_KEY_"$i
  consumerSecret="CONSUMER_SECRET_"$i
  accessToken="ACCESS_TOKEN_"$i
  accessTokenSecret="ACCESS_TOKEN_SECRET_"$i

  # Start the process
  echo "starting task "$i
  task "$thing" &
done

# Waiting for all the process to finish.
wait
echo "Completed collecting friends and followers"