import faust
import json
from pymongo import MongoClient
from decouple import config

MONGO_DB_URI = config('MONGO_DB_URI')
MONGODB_DATABASE = config('MONGODB_DATABASE')
MONGODB_COLLECTION = config('MONGODB_COLLECTION')

client = MongoClient(MONGO_DB_URI)
db = client[MONGODB_DATABASE]
collection = db[MONGODB_COLLECTION]

app = faust.App(
    'my-kafka-consumer',
    broker='kafka://localhost:29092',
    value_serializer='json',
)

topic = app.topic('probando')

def insert_into_mongodb(data):
    try:
        collection.insert_one(data)
    except Exception as e:
        print(f"Error al insertar en MongoDB: {str(e)}")

@app.agent(topic)
async def process_messages(messages):
    async for message in messages:
        try:
            insert_into_mongodb(message)
            print('Received and processed message')
        except Exception as e:
            print(f"Error al procesar el mensaje: {str(e)}")

if __name__ == "__main__":
    app.main()