import pika
import os
import json

from consumer_interface import mqConsumerInterface  # Adjust the import path as needed

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key, exchange_name, queue_name):
        super().__init__(exchange_name)
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.setupRMQConnection()

    def setupRMQConnection(self):
        # Connect to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(con_params)
        self.channel = self.connection.channel()

        # Declare a topic exchange instead of direct
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic', durable=True)


        # Create queue and bind it to the exchange
        self.createQueue(self.queue_name)
        self.bindQueueToExchange(self.queue_name, self.binding_key)

    def bindQueueToExchange(self, queueName: str, topic: str) -> None:
        # Bind Binding Key to Queue on the exchange
        # Bind the binding key to the queue on the exchange
        self.channel.queue_bind(queue=queueName, exchange=self.exchange_name, routing_key=topic)

    def createQueue(self, queueName: str) -> None:
        # Create Queue if not already present
        # Create a queue if not already present
        self.channel.queue_declare(queue=queueName, durable=True)
        
        # Set-up Callback function for receiving messages
        # Set up a callback function for receiving messages
        self.channel.basic_consume(queue=queueName, on_message_callback=self.on_message_callback, auto_ack=True)

    def on_message_callback(self, channel, method, properties, body):
        # Check if the body is not empty
        if body:
            try:
                # Attempt to decode the body as JSON
                message = body.decode('utf-8')
                print(f"Received: {message}")
            except json.JSONDecodeError as e:
                # If JSON decoding fails, print the error and the original message
                print(f"Failed to decode JSON message: {body.decode('utf-8')}, error: {e}")
        else:
            print("Received a message with no body.")

        # message = json.loads(body.decode('utf-8'))
        # print(f"Received: {message}")
        # # Acknowledge the message if necessary (since auto_ack=True, it's not needed here)

    def startConsuming(self):
        print(f" [*] Waiting for messages in queue: {self.queue_name}. To exit press CTRL+C")
        self.channel.start_consuming()

    def __del__(self):
        print(f"Closing RMQ connection for queue: {self.queue_name}")
        if self.channel is not None:
            self.channel.close()
        if self.connection is not None:
            self.connection.close()
