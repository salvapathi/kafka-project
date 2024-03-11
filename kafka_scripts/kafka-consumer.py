kafka_bootstrap_servers = 'host.docker.internal:9092'
kafka_topic = 'bikes'
def kafka_consumer(kafka_bootstrap_servers):
    conf = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': 'bikergroup',
        'auto.offset.reset': 'earliest'
    }

    # consumer = Consumer(conf)
    # # Subscribe to the Kafka topic
    # consumer.subscribe([kafka_topic])

    # try:
    #     while True:
    #         msg = consumer.poll(timeout=1.0)
    #         if msg is None:
    #             continue
    #         if msg.error():
    #             if msg.error().code() == KafkaError._PARTITION_EOF:
    #                 # End of partition event
    #                 print('%% %s [%d] reached end at offset %d\n' %
    #                     (msg.topic(), msg.partition(), msg.offset()))
    #             elif msg.error():
    #                 # Error
    #                 print('Error: %s' % msg.error())
    #         else:
    #             # Message received
    #             print('Received message: %s' % msg.value().decode('utf-8'))

    # except KeyboardInterrupt:
    #    pass
        
    client = server
    db =  client[data_base_name]
    collection = db[collections_name]

    consumer = Consumer(conf)
    consumer.subscribe([kafka_topic])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print('%% %s [%d] reached end at offset %d\n' %
                        (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    # Error
                    print('Error: %s' % msg.error())
            else:
                # Message received
                print('Received message: %s' % msg.value().decode('utf-8'))
                record = json.loads(msg.value().decode('utf-8'))
                record["timestamp"] = datetime.now()
                collection.insert_one(record)
                consumer.commit(asynchronous=False) 
    except KeyboardInterrupt:
        pass
