from kafka import KafkaProducer
import json

broker = 'localhost:29092'  
topic = 'test_event_data'

producer = KafkaProducer(
    bootstrap_servers=[broker],
    #key_serializer=str.encode, 
    value_serializer=lambda v: json.dumps(v).encode('utf-8') 
)

messages = [
{"event":{"concept:name":"A_SUBMITTED","time:timestamp":1703603442},"trace":{"concept:name":199}},
{"event":{"concept:name":"A_PARTLYSUBMITTED","time:timestamp":1703603522},"trace":{"concept:name":199}},
{"event":{"concept:name":"A_DECLINED","time:timestamp":1703603535},"trace":{"concept:name":200}},

]

for message in messages:
    #key = message["id"]
    producer.send(topic,value=message)
    print(f"Sent: {message}")

producer.flush()
