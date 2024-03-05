import pika
import os
from consumer_interface import mqConsumerInterface  # Assuming this is the correct import path

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key, exchange_name, queue_name):
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.setupRMQConnection()

    def setupRMQConnection(self):
        # Connect to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(con_params)
        self.channel = self.connection.channel()

        # Declare exchange
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct', durable=True)

        # Declare queue
        self.channel.queue_declare(queue=self.queue_name, durable=True)

        # Bind queue to exchange
        self.channel.queue_bind(queue=self.queue_name, exchange=self.exchange_name, routing_key=self.binding_key)

        # Set up callback function for receiving messages
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.on_message_callback, auto_ack=True)

    def on_message_callback(self, channel, method, properties, body):
        message = body.decode('utf-8')
        print("Received:", message)
        # Here you might want to acknowledge the message manually if auto_ack=False
        # self.channel.basic_ack(delivery_tag=method.delivery_tag)

    def startConsuming(self):
        print(" [*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()

    def __del__(self):
        print("Closing RMQ connection on destruction")
        if self.channel is not None:
            self.channel.close()
        if self.connection is not None:
            self.connection.close()
