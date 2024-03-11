kafka_bootstrap_servers = 'host.docker.internal:9092'
kafka_topic = 'bikes'

def delivery_callback(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to topic {msg.topic()} partition {msg.partition()} offset {msg.offset()}')


def kafka_producer(json_file,batch_size=5):
    producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

    # with open(json_file,"r") as f:
    #     data = json.load(f)
    #     for record in data:
    #         producer.produce(kafka_topic, json.dumps(record).encode('utf-8'), callback=delivery_callback)
    #     # Flush the produce queue to ensure the messages are delivered
    #     producer.flush()

    # producer.flush()
    with open(json_file) as f:
        data = json.load(f)
        for i in range(0,len(data),batch_size):
            batch=data[i:i+batch_size]
            for record in batch:
                producer.produce(kafka_topic, json.dumps(record).encode('utf-8'), callback=delivery_callback)
                # Flush the produce queue to ensure the messages are delivered
            producer.flush()
            time.sleep(20)
        producer.flush()  # flush any remaining messages
    
    
   
    

