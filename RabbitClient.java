import java.io.IOException;
import java.time.LocalDateTime;
import java.util.UUID;

//http://www.tutorialspoint.com/jackson/jackson_objectmapper.htm
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

//http://www.rabbitmq.com/java-client.html
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;


public class RabbitClient {

	private Connection connection;
	private Channel channel;
	private String replyQueueName;
	private QueueingConsumer consumer;
	private ObjectMapper mapper;

	private String host = "jaguar.rmq.cloudamqp.com";
	private int port = 5672;
	private String virtualhost = "zbpsjoml";
	private String username = "zbpsjoml";
	private String password = "uH1mSI-IHSl1b7jy1BvdTtuU3hJKKkxJ";
	private String routingKey;
	private String exchange;	
	
	public static void main(String[] args) {
		try {
			String exchangeName = "easy_net_q_rpc";
			String routingKey = "DummyQueue.DummyRequest:DummyQueue";
			RabbitClient writer = new RabbitClient(exchangeName,routingKey);
			DummyRequest request = writer.new DummyRequest();
			request.setValue1(UUID.randomUUID().toString());
			request.setValue2(LocalDateTime.now().toLocalTime().toString());
			writer.request(request, 10);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	

	public RabbitClient(String exchange, String routingKey) throws Exception {
		this.exchange = exchange;
		this.routingKey = routingKey;
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(this.host);
		factory.setPort(this.port);
		factory.setUsername(this.username);
		factory.setPassword(this.password);
		factory.setVirtualHost(this.virtualhost);

		this.mapper = new ObjectMapper();
		this.connection = factory.newConnection();
		this.channel = connection.createChannel();
		this.replyQueueName = channel.queueDeclare().getQueue();
		this.consumer = new QueueingConsumer(channel);
		this.channel.basicConsume(replyQueueName, true, consumer);
		this.channel.queueBind(replyQueueName, this.exchange, replyQueueName);
	}

	public DummyResponse request(DummyRequest request, Integer timeoutSeconds) throws Exception {
		DummyResponse response = null;
		try {
			String corrId = java.util.UUID.randomUUID().toString();
			String messageRequest = mapper.writeValueAsString(request);

			BasicProperties props = new BasicProperties.Builder()
					.correlationId(corrId)
					.replyTo(this.replyQueueName)
					.type(this.routingKey)
					.expiration(Integer.toString(timeoutSeconds * 1000))
					.build();

			this.channel.basicPublish(this.exchange, this.routingKey, props, messageRequest.getBytes());
			System.out.println("Request >> " + messageRequest);

			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				if (delivery.getProperties().getCorrelationId().equals(corrId)) {
					response = mapper.readValue(new String(delivery.getBody()), DummyResponse.class);
					System.out.println("Response >> " + mapper.writeValueAsString(response));
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		return response;
	}

	public void close() throws Exception {
		this.connection.close();
	}

	public class DummyRequest {
		private String Value1;
		private String Value2;

		public String getValue1() {
			return this.Value1;
		}

		public void setValue1(String value1) {
			this.Value1 = value1;
		}

		public String getValue2() {
			return this.Value2;
		}

		public void setValue2(String value2) {
			this.Value2 = value2;
		}

	}

	public static class DummyResponse {

		private String Value3;
		private String Value4;

		@JsonProperty("Value3")
		public String getValue3() {
			return this.Value3;
		}

		public void setValue3(String value3) {
			this.Value3 = value3;
		}

		@JsonProperty("Value4")
		public String getValue4() {
			return this.Value4;
		}

		public void setValue4(String value4) {
			this.Value4 = value4;		}

	}
}
