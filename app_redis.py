from confluent_kafka import Consumer, KafkaError
import redis
kafka_config = {
    'bootstrap.servers': 'localhost:29092',  
    'group.id': 'my-kafka-consumer',           
    'auto.offset.reset': 'earliest'              
}

redis_host = 'localhost'
redis_port = 6379
redis_db = 0

consumer = Consumer(kafka_config)
topic = 'probando'
consumer.subscribe([topic])

redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)

id=0
while True:
    msg = consumer.poll(1.0) 
    id=id+1
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('No más mensajes en esta partición')
        else:
            print('Error al consumir mensaje: {}'.format(msg.error()))
    else:
        kafka_message = msg.value()
        

        redis_client.set(id, kafka_message)
        print('Mensaje almacenado en Redis:', kafka_message)
