from kafka import KafkaProducer
import csv
import json
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

with open('/home/varshini/Sem 6/DBT Project/Dataset/news.csv', 'r') as csvfile:
    csvreader = csv.DictReader(csvfile)
    for row in csvreader:
        if row['news_category'] == 'sports':
            producer.send('sports', value=row)
        elif row['news_category'] == 'technology':
            producer.send('technology', value=row)
        elif row['news_category'] == 'politics':
            producer.send('politics', value=row)
        elif row['news_category'] == 'entertainment':
            producer.send('entertainment', value=row)
        elif row['news_category'] == 'world':
            producer.send('world', value=row)
        elif row['news_category'] == 'automobile':
            producer.send('automobile', value=row)
        elif row['news_category'] == 'science':
            producer.send('science', value=row)
            
        # add more topics as needed
        time.sleep(1) # add delay to avoid overloading Kafka broker

producer.close()

