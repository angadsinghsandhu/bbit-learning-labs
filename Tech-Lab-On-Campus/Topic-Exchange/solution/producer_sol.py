import pika
import os
import sys
import json

from producer_interface import mqProducerInterface  # Adjust the import path as needed

class mqProducer(mqProducerInterface):
    def __init__(self, exchange_name: str):
        self.exchange_name = exchange_name
        super().__init__(exchange_name)  # This calls the parent class's __init__ method
        self.setupRMQConnection()

    def __init__(self, routing_key, exchange_name: str):
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        super().__init__(exchange_name)  # This calls the parent class's __init__ method
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Establish connection to the RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(con_params)
        self.channel = self.connection.channel()

        # Create the topic exchange if not already present
        print(f"Declaring exchange: {self.exchange_name}")
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic', durable=True)

    def publishOrder(self, message: str) -> None:
        # Assuming routing key and message are passed correctly, publish message to exchange
        # This assumes that the command line will pass the routing key and message separately
        # routing_key, message = message.split(':', 1)
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=message.encode('utf-8')
        )
        print(f"Sent: {self.routing_key}:{message}")

        # Close channel and connection
        self.closeConnection()

    def closeConnection(self) -> None:
        if self.channel is not None:
            self.channel.close()
        if self.connection is not None:
            self.connection.close()