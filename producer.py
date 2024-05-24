from kafka import KafkaProducer
import csv
import json
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

with open('Corona_NLP_test.csv', 'r') as csvfile:
    csvreader = csv.DictReader(csvfile)
    for row in csvreader:
        tweet = row['OriginalTweet']
        username = row['UserName']
        location =row['Location']
        if '#Covid_19' or '#COVID19' or '#covid19' in tweet:
            producer.send('covid19', value={'tweet': tweet, 'username': username,'location':location})
        if '#Coronavirus' or '#coronavirus'  in tweet:
            producer.send('coronavirus', value={'tweet': tweet, 'username': username,'location':location})
        if '#lockdown' or '#LOCKDOWN' or '#Lockdown' in tweet:
            producer.send('lockdown', value={'tweet': tweet, 'username': username,'location':location})
            
        if 'NYC' or 'USA' or 'US' or 'United-States' in location:
        	producer.send('USA', value={'tweet': tweet, 'username': username,'location':'USA'})
        elif 'CA' or 'Cannada' in location:
        	producer.send('Cannada', value={'tweet': tweet, 'username': username,'location':'Cannada'})
        elif "London" in location :
        	producer.send('London', value={'tweet': tweet, 'username': username,'location':'London'})
        else:
        	producer.send('Others', value={'tweet': tweet, 'username': username,'location':'Others'})
        
        	
        	
        

        time.sleep(1) # add delay to avoid overloading Kafka broker

producer.close()

