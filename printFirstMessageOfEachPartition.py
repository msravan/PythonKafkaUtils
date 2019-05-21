from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers=['kafka:9092'], auto_offset_reset='earliest')
consumer.subscribe(['topic_name'])

for message in consumer:
    file_extension = str(message.partition)
    open("message_"+file_extension+ ".txt", "a").write(str(message)+ "\n")

    print(message.partition + message)
    print("\n")
consumer.close()




