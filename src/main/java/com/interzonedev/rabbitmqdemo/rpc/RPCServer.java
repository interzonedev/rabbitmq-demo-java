package com.interzonedev.rabbitmqdemo.rpc;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class RPCServer {

	private static final String RPC_QUEUE_NAME = "rpc_queue";

	private static int fib(int n) {
		if (n == 0) {
			return 0;
		}
		if (n == 1) {
			return 1;
		}
		return fib(n - 1) + fib(n - 2);
	}

	public static void main(String[] argv) {
		Connection connection = null;
		Channel channel = null;
		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");

			connection = factory.newConnection();
			channel = connection.createChannel();

			DeclareOk queueDeclare = channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
			System.out.println("queueDeclare = " + queueDeclare);

			channel.basicQos(1);

			QueueingConsumer consumer = new QueueingConsumer(channel);
			String consumerTag = channel.basicConsume(RPC_QUEUE_NAME, false, consumer);

			System.out.println(" [x] Awaiting RPC requests on " + consumerTag);

			while (true) {
				String response = null;

				QueueingConsumer.Delivery delivery = consumer.nextDelivery();

				BasicProperties requestProps = delivery.getProperties();

				String correlationId = requestProps.getCorrelationId();
				System.out.println("Received message with correlationId = " + correlationId);

				BasicProperties responseProps = new BasicProperties.Builder().correlationId(correlationId).build();

				try {
					String message = new String(delivery.getBody(), "UTF-8");
					int n = Integer.parseInt(message);

					System.out.println(" [.] fib(" + message + ")");
					response = "" + fib(n);
				} catch (Exception e) {
					System.out.println(" [.] " + e.toString());
					response = "";
				} finally {
					String replyTo = requestProps.getReplyTo();
					System.out.println("replyTo = " + replyTo);

					channel.basicPublish("", replyTo, responseProps, response.getBytes("UTF-8"));

					long deliveryTag = delivery.getEnvelope().getDeliveryTag();
					System.out.println("deliveryTag = " + deliveryTag);

					channel.basicAck(deliveryTag, false);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception ignore) {
				}
			}
		}
	}
}
