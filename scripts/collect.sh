#!/bin/bash

N=1

# Usage.
usage()
{
    echo "usage: collect.sh [-n THREADS] [-h | --help]"
}

# Read input parameters.
if [ "$1" == "" ]; then usage; exit 1; fi
while [ "$1" != "" ]; do
    case $1 in
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
split -l

# Iterating over the splits and starting the process
for ((i=1;i<=$N;i++));
do

done