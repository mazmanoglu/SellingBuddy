using EventBus.Base;
using EventBus.Base.Events;
using Newtonsoft.Json;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace EventBus.RabbitMQ
{
	public class EventBusRabbitMQ : BaseEventBus
	{
		RabbitMQPersistentConnection persistentConnection;
		private readonly IConnectionFactory _connectionFactory;
		private readonly IModel _consumerChannel;

		public EventBusRabbitMQ(EventBusConfig config, IServiceProvider serviceProvider) : base(config, serviceProvider)
		{
			if(config.Connection!=null)
			{
				var connJson = JsonConvert.SerializeObject(EventBusConfig.Connection, new JsonSerializerSettings()
				{
					// self referencing loop detected for property
					ReferenceLoopHandling = ReferenceLoopHandling.Ignore
				}
				);
				_connectionFactory = JsonConvert.DeserializeObject<ConnectionFactory>(connJson);
			}
			else
			{
				_connectionFactory = new ConnectionFactory();
			}
			persistentConnection = new RabbitMQPersistentConnection(_connectionFactory, config.ConnectionRetryCount);
			_consumerChannel = CreateConsumerChannel();

			SubsManager.OnEventRemoved += SubsManager_OnEventRemoved;
		}


		public override void Publish(IntegrationEvent @event)
		{
			if(!persistentConnection.IsConnected)
			{
				persistentConnection.TryConnect();
			}

			var policy = Policy.Handle<BrokerUnreachableException>()
				.Or<SocketException>()
				.WaitAndRetry(EventBusConfig.ConnectionRetryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
				{
					// logging
				}
				);

			var eventName = @event.GetType().Name;
			eventName = ProcessEventName(eventName);

			_consumerChannel.ExchangeDeclare(exchange: EventBusConfig.DefaultTopicName, type: "direct"); // ensure exchange exists while publishing

			var message = JsonConvert.SerializeObject(@event);
			var body = Encoding.UTF8.GetBytes(message);

			policy.Execute(() =>
			{
				var properties = _consumerChannel.CreateBasicProperties();
				properties.DeliveryMode = 2; //persistent

				_consumerChannel.QueueDeclare(queue: GetSubName(eventName), //ensure queue exists while publishing
					durable: true,
					exclusive: false,
					autoDelete: false,
					arguments:null
					);

				_consumerChannel.BasicPublish(
					exchange:EventBusConfig.DefaultTopicName,
					routingKey:eventName,
					mandatory: true,
					basicProperties: properties,
					body: body
					);
			});
		}

		public override void Subscribe<T, TH>()
		{
			var eventName = typeof(T).Name;
			eventName = ProcessEventName(eventName);

			if(!SubsManager.HasSubscriptionsForEvent(eventName))
			{
				if (!persistentConnection.IsConnected)
				{
					persistentConnection.TryConnect();
				}
				_consumerChannel.QueueDeclare(queue: GetSubName(eventName), //ensure queue exists while consuming
					durable: true,
					exclusive:false,
					autoDelete: false,
					arguments: null);

				_consumerChannel.QueueBind(queue: GetSubName(eventName),
					exchange: EventBusConfig.DefaultTopicName,
					routingKey:eventName);
			}

			SubsManager.AddSubscription<T, TH>();
			StartBasicConsume(eventName);
		}

		public override void Unsubscribe<T, TH>()
		{
			SubsManager.RemoveSubscription<T, TH>();
		}

		private IModel CreateConsumerChannel()
		{
			if (!persistentConnection.IsConnected)
			{
				persistentConnection.TryConnect();
			}
			var channel = persistentConnection.CreateModel();
			channel.ExchangeDeclare(exchange: EventBusConfig.DefaultTopicName, type: "direct");
			return channel;
		}

		private void StartBasicConsume(string eventName)
		{
			if(_consumerChannel != null)
			{
				var consumer = new EventingBasicConsumer(_consumerChannel);
				consumer.Received += Consumer_Received;

				_consumerChannel.BasicConsume(
					queue: GetSubName(eventName),
					autoAck: false,
					consumer: consumer
					);
			}
		}

		private async void Consumer_Received(object sender, BasicDeliverEventArgs eventArgs)
		{
			var eventName = eventArgs.RoutingKey;
			eventName = ProcessEventName(eventName);
			var message = Encoding.UTF8.GetString(eventArgs.Body.Span);

			try
			{
				await ProcessEvent(eventName, message);
			}
			catch(Exception ex)
			{
				// logging
				Console.WriteLine(ex);
			}
			_consumerChannel.BasicAck(eventArgs.DeliveryTag, multiple: false);
		}

		private void SubsManager_OnEventRemoved(object? sender, string eventName)
		{
			eventName = ProcessEventName(eventName);
			if (!persistentConnection.IsConnected)
			{
				persistentConnection.TryConnect();
			}

			_consumerChannel.QueueBind(
				queue: eventName,
				exchange: EventBusConfig.DefaultTopicName,
				routingKey: eventName
				);

			if (SubsManager.IsEmpty)
			{
				_consumerChannel.Close();
			}
		}

	}
}
