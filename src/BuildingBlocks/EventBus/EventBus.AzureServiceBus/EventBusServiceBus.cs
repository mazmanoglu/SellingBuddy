using EventBus.Base;
using EventBus.Base.Events;
using System;
using Azure.Messaging.ServiceBus;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus.Administration;
using Newtonsoft.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;

namespace EventBus.AzureServiceBus
{
	#region MyRegion
	//public class EventBusServiceBus : BaseEventBus
	//{
	//	private ServiceBusAdministrationClient managementClient;
	//	private ServiceBusClient client;
	//	private ServiceBusSender topicClient; //= client.CreateSender(EventBusConfig.DefaultTopicName);
	//	private ILogger logger;
	//	private ServiceBusRuleManager ruleManager;


	//	public EventBusServiceBus(EventBusConfig config, IServiceProvider serviceProvider) : base(config, serviceProvider)
	//	{
	//		logger = serviceProvider.GetService(typeof(ILogger<EventBusServiceBus>)) as ILogger<EventBusServiceBus>;
	//		managementClient = new ServiceBusAdministrationClient(config.EventBusConnectionString);
	//		client = new ServiceBusClient(config.EventBusConnectionString);
	//		topicClient = CreateTopicClient();
	//	}

	//	private ServiceBusSender CreateTopicClient()
	//	{
	//		if(topicClient == null || topicClient.IsClosed)
	//		{
	//			topicClient = new ServiceBusClient(EventBusConfig.EventBusConnectionString).CreateSender(EventBusConfig.DefaultTopicName);
	//		}
	//		// Ensure that topic already exists
	//		if (!managementClient.TopicExistsAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult()) // use getawaiter and getresult
	//		{
	//			 managementClient.CreateTopicAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult(); // coz, our method is not async
	//		}
	//		return topicClient;
	//	}


	//	public override void Publish(IntegrationEvent @event)
	//	{

	//		var eventName = @event.GetType().Name; // example: OrderCreatedIntegrationEvent
	//		eventName = ProcessEventName(eventName); // example: OrderCreated

	//		var eventStr = JsonConvert.SerializeObject(@event);
	//		var bodyArr = Encoding.UTF8.GetBytes(eventStr);

	//		var message = new ServiceBusMessage(bodyArr)
	//		{
	//			MessageId = Guid.NewGuid().ToString(),
	//			//Body = bodyArr, // its used old Microsoft ServiceBus library version
	//			Subject = eventName
	//		};
	//		topicClient.SendMessageAsync(message).GetAwaiter().GetResult();
	//	}

	//	public override void Subscribe<T, TH>()
	//	{
	//		var eventName = typeof(T).Name;
	//		eventName = ProcessEventName(eventName);

	//		if(!SubsManager.HasSubscriptionsForEvent(eventName))
	//		{
	//			CreateSubscriptionReceiver(eventName);
	//		}
	//	}

	//	public override void Unsubscribe<T, TH>()
	//	{
	//	}

	//	/*
	//	 we dont use this method because SubscriptionClient was in Microsoft ServiceBus

	//	private SubScriptionClient createSubScriptionClient(string eventName)
	//	{
	//		return new SubScriptionClient(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName, GetSubName(eventName));
	//	}
	//	*/

	//	private ServiceBusReceiver CreateSubscriptionReceiver(string eventName)
	//	{
	//		//ServiceBusClient client = new ServiceBusClient(connectionString);
	//		//return client.CreateReceiver(topicName, eventName);
	//		return new ServiceBusClient(EventBusConfig.EventBusConnectionString).CreateReceiver(EventBusConfig.DefaultTopicName, GetSubName(eventName));
	//	}

	//	private ServiceBusReceiver CreateSubscriptionReceiverIfNotExists(string eventName)
	//	{
	//		var subClient = CreateSubscriptionReceiver(eventName);
	//		var exists = managementClient.SubscriptionExistsAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();

	//		if (!exists)
	//		{
	//			var rule = managementClient.GetRuleAsync(EventBusConfig.DefaultTopicName, eventName, eventName).GetAwaiter().GetType();
	//			RemoveDefaultRule(subClient);
	//		}
	//	}

	//	private void RemoveDefaultRule(SubscriptionClient subscriptionClient)
	//	{
	//		try
	//		{
	//			managementClient.DeleteRuleAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName));
	//		}
	//		catch (ServiceBusException ex) // instead MessagingEntitynotFoundException
	//		{
	//			if (ex.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
	//			{
	//				// Handle the case where the messaging entity is not found
	//				Console.WriteLine("Messaging entity not found.");
	//			}
	//			else
	//			{
	//				// Handle other Service Bus exceptions
	//				Console.WriteLine($"An error occurred: {ex.Message}");
	//			}
	//		}
	//	}

	//	private RemoveDefaultRuleAsync(ILogger logger)
	//	{
	//		try
	//		{
	//			.DeleteRuleAsync(RuleProperties.DefaultRuleName).;
	//		}
	//		catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
	//		{
	//			logger.LogWarning($"The default rule '{RuleProperties.DefaultRuleName}' does not exist.");
	//		}
	//	}



	//} 
	#endregion

	public class EventBusServiceBus : BaseEventBus
	{
		private ITopicClient topicClient;
		private ManagementClient managementClient;
		private ILogger logger;

		public EventBusServiceBus(EventBusConfig config, IServiceProvider serviceProvider) : base(config, serviceProvider)
		{
			logger = serviceProvider.GetService(typeof(ILogger<EventBusServiceBus>)) as ILogger<EventBusServiceBus>;
			managementClient = new ManagementClient(config.EventBusConnectionString);
			topicClient = createTopicClient();
		}
		private ITopicClient createTopicClient()
		{
			if (topicClient == null || topicClient.IsClosedOrClosing)
			{
				topicClient = new TopicClient(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName, RetryPolicy.Default);
			}

			// ensure that the topic already exists
			if (!managementClient.TopicExistsAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult())
			{
				managementClient.CreateTopicAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult();
			}
			return topicClient;
		}
		public override void Publish(IntegrationEvent @event)
		{
			var eventName = @event.GetType().Name; // example OrderCreatedIntegrationEvent
			eventName = ProcessEventName(eventName); // example OrderCreated
			var eventStr = JsonConvert.SerializeObject(@event);
			var bodyArr = Encoding.UTF8.GetBytes(eventStr);

			var message = new Message()
			{
				MessageId = Guid.NewGuid().ToString(),
				Label = "",
				Body = bodyArr
			};
			topicClient.SendAsync(message).GetAwaiter().GetResult();
		}



		public override void Subscribe<T, TH>()
		{
			var eventName = typeof(T).Name;
			eventName = ProcessEventName(eventName);

			if (!SubsManager.HasSubscriptionsForEvent(eventName))
			{
				var subScriptionClient = CreateSubscriptionClientIfNotExists(eventName);
				RegisterSubscriptionClientMessageHandler(subScriptionClient);

			}
			logger.LogInformation($"Subscribing to event {eventName} with {typeof(TH).Name}");
			SubsManager.AddSubscription<T, TH>()
		}
		public override void Unsubscribe<T, TH>()
		{
			var eventName = typeof(T).Name;

			try
			{
				//subscription will be there but we dont subscribe
				var subscriptionClient = CreateSubscriptionClient(eventName);

				subscriptionClient
					.RemoveRuleAsync(eventName)
					.GetAwaiter()
					.GetResult();
			}
			catch (MessagingEntityNotFoundException)
			{
				logger.LogWarning($"The messaging entity {eventName} could not be found");
			}
			logger.LogInformation($"Unsubscribing from event {eventName}");
			SubsManager.RemoveSubscription<T, TH>();
		}

		private void RegisterSubscriptionClientMessageHandler(ISubscriptionClient subscriptionClient)
		{
			subscriptionClient.RegisterMessageHandler(
				async (message, token) =>
				{
					var eventName = $"{message.Label}";
					var messageData = Encoding.UTF8.GetString(message.Body);

					// complete the message so that it is not received again
					if (await ProcessEvent(ProcessEventName(eventName), messageData))
					{
						await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
					}
				},
				new MessageHandlerOptions(ExceptionReceivedHandler) { MaxConcurrentCalls = 10, AutoComplete = false };
		}

		private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
		{
			var ex = exceptionReceivedEventArgs.Exception;
			var context = exceptionReceivedEventArgs.ExceptionReceivedContext;

			logger.LogError(ex, $"Error handling message {ex.Message} - Context {context}");

			return Task.CompletedTask;
		}

		private ISubscriptionClient CreateSubscriptionClientIfNotExists(string eventName)
		{
			var subClient = CreateSubscriptionClient(eventName);
			var exists = managementClient.SubscriptionExistsAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();

			if (!exists)
			{
				managementClient.CreateSubscriptionAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();
				RemoveDefaultRule(subClient);
			}

			CreateRuleIfNotExists(ProcessEventName(eventName), subClient);

			return subClient;
		}

		private void CreateRuleIfNotExists(string eventName, ISubscriptionClient subscriptionClient)
		{
			bool ruleExists = false;
			try
			{
				var rule = managementClient.GetRuleAsync(EventBusConfig.DefaultTopicName, eventName, eventName).GetAwaiter().GetResult();
			}
			catch (MessagingEntityNotFoundException)
			{
				// azure management client doesnt have RuleExists method
				ruleExists = false;
			}

			if (!ruleExists)
			{
				subscriptionClient.AddRuleAsync(new RuleDescription
				{
					Filter = new CorrelationFilter { Label = eventName },
					Name = eventName,
				}).GetAwaiter().GetResult();
			}
		}

		private void RemoveDefaultRule(SubscriptionClient subscriptionClient)
		{
			try
			{
				subscriptionClient
					.RemoveRuleAsync(RuleDescription.DefaultRuleName)
					.GetAwaiter()
					.GetResult();
			}
			catch (MessagingEntityNotFoundException)
			{
				logger.LogWarning($"The messaging entity {RuleDescription.DefaultRuleName} could not be found");
			}
		}

		private SubscriptionClient CreateSubscriptionClient(string eventName)
		{
			return new SubscriptionClient(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName, GetSubName(eventName));
		}

		public override void Dispose()
		{
			base.Dispose();

			topicClient.CloseAsync().GetAwaiter().GetResult();
			managementClient.CloseAsync().GetAwaiter().GetResult();

			topicClient = null;
			managementClient = null;
		}
	}
}
