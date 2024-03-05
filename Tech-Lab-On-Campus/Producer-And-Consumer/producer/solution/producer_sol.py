import pika
import os
from producer_interface import mqProducerInterface  # Adjust the import path as needed

class mqProducer(mqProducerInterface):
    def __init__(self, exchange_name, routing_key):
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.setupRMQConnection()

    def setupRMQConnection(self):
        # Establish connection to the RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(con_params)
        self.channel = self.connection.channel()

        # Declare the exchange if not already present
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct', durable=True)

    def publishOrder(self, message):
        # Publish a simple UTF-8 string message
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=message.encode('utf-8')
        )
        print(f"Sent: {message}")

        # Close Channel and Connection after publishing
        self.closeConnection()

    def closeConnection(self):
        if self.channel is not None:
            self.channel.close()
        if self.connection is not None:
            self.connection.close()
