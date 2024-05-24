# twittercornovirustweetsegregation

This project is a good starting point for those who have little or no experience with Kafka & Apache Spark Streaming and batch processing.


Input data:CSV file
Main model: Divide the tweets based in topics and store them in Mysql Database
Output: Text with all the tweets and also stored in Mysql databse.



## Part 1: Ingest Data using Kafka 

Can use data in Real Time using API CSV file. The streamed data should be in CSV format


## Part 2: Tweet preprocessing and Segregation 
In this part, we receive tweets from Kafka and preprocess them with the pyspark library which is python's API for spark. We then divide them into different topics.

After we send them to consumer file and then it is stored in Databse and also the streaming is shown in console.
