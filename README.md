# Evidence-Data-Generator
Evidence-Data-Generator holds redesigned versions of the utility code used for constructing an evidence dataset from tweets, collected from [Twitter](https://twitter.com), which could be used with various MLN inference tools such as [Tuffy](http://i.stanford.edu/hazy/tuffy/)

## Environment Setup
```
cd scripts && source setup.sh
```

## Build the Code
```
sbt publishLocal && sbt clean assembly
```


## Generate Evidence  
* Update `conf/app.config` with the twitter keys and keywords.

* To collect tweets:
    ```
    spark-submit --class edu.missouri.CollectTweets target/scala-2.11/Evidence-Data-Generator-assembly-0.1.jar <NO_OF_TWEETS> <TWEETS_OUT_FILE>
    ```

    Eg:
    ```
    spark-submit --class edu.missouri.CollectTweets target/scala-2.11/Evidence-Data-Generator-assembly-0.1.jar 1000 /mydata/tweets.json
    ```

* To construct evidence:
    ```
    spark-submit --class edu.missouri.GenerateEvidence --driver-memory <DRIVER_MEMORY> target/scala-2.11/Evidence-Data-Generator-assembly-0.1.jar <TWEETS_INP_FILE> <EVIDENCE_OUT_FILE>
    ```
    
    Eg:
    ```
    spark-submit --class edu.missouri.GenerateEvidence --driver-memory 30g target/scala-2.11/Evidence-Data-Generator-assembly-0.1.jar /mydata/tweets.json /mydata/evidence.db
    ```

* To collect friends and followers:
    ```
    cd scripts && bash collect.sh bash collect.sh -tweets <TWEETS_INP_FILE> -n <NO_OF_THREADS>
    ```
    
    Eg:
    ```
    cd scripts && bash collect.sh bash collect.sh -tweets /mydata/tweets.json -n 10
    ```
    The constructed evidence for friends and followers would be present at `data/data_out/evidence.db`

## References
* Praveen Rao, Anas Katib, Charles Kamhoua, Kevin Kwiat, and Laurent Njilla. "Probabilistic Inference on Twitter Data to Discover Suspicious Users and Malicious Content." In the 2nd IEEE International Symposium on Security and Privacy in Social Networks and Big Data (SocialSec 2016), pages 407-414, Nadi, Fiji, December 2016. [[PDF](http://r.web.umkc.edu/raopr/SocialKB-SocialSec-2016.pdf)] [[Code](https://github.com/UMKC-BigDataLab/SocialKB)]
* Monica Senapati, Laurent Njilla, Praveen Rao. "A Method for Scalable First-Order Rule Learning on Twitter Data." In Proc. of 35th IEEE International Conference on Data Engineering Workshops (ICDEW) , pages 274-277, Macau, China, 2019.[[PDF](http://r.web.umkc.edu/raopr/SRLearn-ICDEW-2019.pdf)]
    
