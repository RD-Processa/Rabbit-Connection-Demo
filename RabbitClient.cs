// Instalar estos paquetes de NuGet antes de continuar
// https://docs.nuget.org/consume/nuget-faq
// Install - Package Newtonsoft.Json
// Install - Package RabbitMQ.Client
//
// Agregar estas directivas a su código.
// using System;
// using Newtonsoft.Json;
// using RabbitMQ.Client;
// using RabbitMQ.Client.Events;

void Main()
{
	// TODO: Coloque aquí el queueName o routingKey proporcionado.
	var routingKey = "DummyQueue.DummyRequest:DummyQueue";
	
	// TODO: Coloque aquí el exchangeName proporcionado.
	var exchangeName = "easy_net_q_rpc";
	
	using (RabbitClient client = new RabbitClient(exchangeName, routingKey))
	{
		var request = new DummyRequest {Value1 = Guid.NewGuid().ToString(), Value2 = DateTime.Now.ToShortTimeString()};
		var response = client.Request<DummyRequest,DummyResponse>(request);
		Console.WriteLine(response);
	}
}


public class DummyRequest
{
	public string Value1 { get; set; }
	public string Value2 { get; set; }
}
public class DummyResponse
{
	public string Value3 { get; set; }
	public string Value4 { get; set; }
}

public class RabbitClient : IDisposable
{
	private string exchangeName;
	private string routingKey;
	private IModel model ;
	private IBasicProperties basicProperties;
	private EventingBasicConsumer consumer;
	private string correlationId = Guid.NewGuid().ToString("N");
	private IConnection connection;
	
	public RabbitClient(string exchangeName, string routingKey)
	{
		this.exchangeName = exchangeName;
		this.routingKey = routingKey;
	}

	public TResponse Request<TRequest, TResponse>(TRequest request, int timeoutSeconds = 10)
	{
		this.SetupConnection();
		var jsonRequest = JsonConvert.SerializeObject(request);
		var rawRequest = Encoding.UTF8.GetBytes(jsonRequest);
		
		TimeSpan timeout = TimeSpan.FromSeconds(timeoutSeconds);
		this.basicProperties.Expiration = timeout.TotalMilliseconds.ToString();	
		var response = default(TResponse);
		
		this.consumer.Received += (sender, args) =>
		{
		   if (args.BasicProperties != null && args.BasicProperties.CorrelationId == this.correlationId)
		   {
			   var jsonResponse = Encoding.UTF8.GetString(args.Body);
			   this.connection.Dispose();
			   response = JsonConvert.DeserializeObject<TResponse>(jsonResponse);
		   }
	    };

		this.model.BasicPublish(this.exchangeName, this.routingKey, this.basicProperties, rawRequest);

		DateTime startTime = DateTime.UtcNow;
		DateTime endTime = startTime + timeout;
		
		while (EqualityComparer<TResponse>.Default.Equals(response,default(TResponse)))
		{
			if (DateTime.UtcNow >= endTime)
			{
				throw new TimeoutException();
			}
		}
		
		return response;
	}

	public void Dispose()
	{
		if (this.connection != null)
		{
			this.connection.Dispose();
		}
	}

	private void SetupConnection()
	{
		// TODO: Coloque aquí los datos de conexión proporcionados.
		var factory = new ConnectionFactory() { HostName = "hostname", UserName="username", Password="password", VirtualHost="vhost"};
		var replyQueueName = Guid.NewGuid().ToString("N");
		this.connection = factory.CreateConnection();
		this.model = this.connection.CreateModel();
		this.consumer = new EventingBasicConsumer(model);
		this.model.QueueDeclare(replyQueueName, false, true, true, new Dictionary<string, object>());
		this.model.QueueBind(replyQueueName, this.exchangeName, replyQueueName);
		this.model.BasicConsume(replyQueueName, true, this.consumer);
		this.basicProperties = model.CreateBasicProperties();
		this.basicProperties.ReplyTo = replyQueueName;
		this.basicProperties.CorrelationId = this.correlationId;
		this.basicProperties.DeliveryMode = 2;
		this.basicProperties.Headers = new Dictionary<string, object>();
		this.basicProperties.Type = this.routingKey;
	}
}
