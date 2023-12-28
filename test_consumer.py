from kafka import KafkaConsumer


topic_name = 'test_label_topic'
bootstrap_servers = ['localhost:29092']  

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest'         
)
 
print("Start")
for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")
consumer.close()    