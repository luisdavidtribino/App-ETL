import faust
import json
import asyncio

#configuracion sencilla de faust
app = faust.App(
    'my-kafka-consumer',
    broker='kafka://localhost:29092',  # Replace with your Kafka broker URL
    value_serializer='json',
)

#Define a Kafka topic to consume from
topic = app.topic('probando')

# @app.agent(topic)
# async def print_messages(messages):
#     async for message in messages:
#         #data = json.loads(message.value)
#         #print("Received:", data)
#         print("Received message:", message)
#         await asyncio.sleep(0.5)  # Espera 0.5 segundo antes de procesar el siguiente mensaje

@app.agent(topic)
async def process_messages(messages):
    personal_data = {}
    professional_data = {}

    async for message in messages:
        data = message
        passport = data.get('passport')

        if passport:
            if passport in personal_data:
                # Mensaje de datos personales ya existente, agregar datos profesionales
                professional_data[passport] = data
            else:
                # Nuevo mensaje de datos personales
                personal_data[passport] = data

    # Ahora tienes los datos personales y profesionales relacionados en diccionarios
    # Puedes procesarlos o almacenarlos según sea necesario
            # Imprimir datos relacionados
        if passport in personal_data and passport in professional_data:
            print("Personal Data:")
            print(personal_data[passport])
            print("Professional Data:")
            print(professional_data[passport])
            print("\n")  # Separador para una mejor legibilidad


if __name__ == '__main__':
    app.main()


