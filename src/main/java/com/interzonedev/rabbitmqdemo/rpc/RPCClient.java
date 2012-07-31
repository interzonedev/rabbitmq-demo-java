package com.interzonedev.rabbitmqdemo.rpc;

import java.util.UUID;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class RPCClient {

	private Connection connection;
	private Channel channel;
	private String requestQueueName = "rpc_queue";
	private String replyQueueName;
	private QueueingConsumer consumer;

	public RPCClient() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		connection = factory.newConnection();
		channel = connection.createChannel();

		replyQueueName = channel.queueDeclare().getQueue();
		consumer = new QueueingConsumer(channel);
		String consumerTag = channel.basicConsume(replyQueueName, true, consumer);
		System.out.println("consumerTag = " + consumerTag);
	}

	public String call(String message) throws Exception {
		String response = null;
		String corrId = UUID.randomUUID().toString();

		BasicProperties props = new BasicProperties.Builder().correlationId(corrId).replyTo(replyQueueName).build();

		channel.basicPublish("", requestQueueName, props, message.getBytes());

		while (true) {
			QueueingConsumer.Delivery delivery = consumer.nextDelivery();
			if (delivery.getProperties().getCorrelationId().equals(corrId)) {
				response = new String(delivery.getBody(), "UTF-8");
				break;
			}
		}

		return response;
	}

	public void close() throws Exception {
		connection.close();
	}

	public static void main(String[] argv) {
		RPCClient fibonacciRpc = null;
		String response = null;
		try {
			fibonacciRpc = new RPCClient();

			String arg = getArg(argv);

			System.out.println(" [x] Requesting fib(" + arg + ")");
			response = fibonacciRpc.call(arg);
			System.out.println(" [.] Got '" + response + "'");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (fibonacciRpc != null) {
				try {
					fibonacciRpc.close();
				} catch (Exception ignore) {
				}
			}
		}
	}

	private static String getArg(String[] argv) {
		String arg = "30";

		if (argv.length > 0) {
			String arg1 = argv[0];
			try {
				Integer.parseInt(arg1);
				arg = arg1.trim();
			} catch (NumberFormatException nfe) {
				System.out.println(arg1 + " can not be parsed as an integer");
			}
		}

		return arg;
	}
}
