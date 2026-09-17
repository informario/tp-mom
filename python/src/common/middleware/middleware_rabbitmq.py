import pika
from pika.exceptions import DuplicateConsumerTag, ReentrancyError
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareMessageError


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)
        return

    def start_consuming(self, on_message_callback):
        """
        pika recibe una funcion:
            def callback(channel, method, properties, body):
        pero el modelo de negocio utiliza una funcion:
            callback(body, ack, nack)
            (que obtiene desde message_consumer_tester.py)
        """
        def pika_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack(requeue=True):
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=requeue)
            on_message_callback(body, ack, nack)
        """
        tipos de erores que pueden ocurrir:
            pika.exceptions.DuplicateConsumerTag – if consumer with given consumer_tag is already present.
            pika.exceptions.ReentrancyError – if called from the scope of a BlockingConnection or BlockingChannel callback
            eleva MessageMiddlewareMessageError
            
            ChannelClosed – when this channel is closed by broker.
            eleva MessageMiddlewareDisconnectedError
        """
        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=pika_callback, auto_ack=False)
            self.channel.start_consuming()
        except DuplicateConsumerTag:
            raise MessageMiddlewareMessageError
        except ReentrancyError:
            raise MessageMiddlewareMessageError
        

        return

    def stop_consuming(self):
        self.channel.stop_consuming()
        pass

    def send(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        return

    def close(self):
        self.connection.close()
        return

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        self.exchange_name = exchange_name
        self.queue_name = None
        self.routing_keys = routing_keys
        return

    def start_consuming(self, on_message_callback):
        def pika_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack(requeue=True):
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=requeue)
            on_message_callback(body, ack, nack)
        result = self.channel.queue_declare(queue="",exclusive=True,auto_delete=True)
        self.queue_name = result.method.queue
        for routing_key in self.routing_keys:
            self.channel.queue_bind(exchange=self.exchange_name,queue=self.queue_name,routing_key=routing_key)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=pika_callback, auto_ack=False)
        self.channel.start_consuming()
        return

    def stop_consuming(self):
        self.channel.stop_consuming()
        return

    def send(self, message):
        self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.routing_keys[0], body=message)

    def close(self):
        self.connection.close()
        return