// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Microsoft.Azure.WebJobs.Extensions.RabbitMQ;

public class RabbitMQModel : IRabbitMQModel
{
    private readonly ILogger logger;
    private readonly ConcurrentDictionary<ulong, byte> deliveredTags = new();

    public RabbitMQModel(IModel model, ILogger logger)
    {
        this.Model = model;
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public IModel Model { get; }

    public IBasicPublishBatch CreateBasicPublishBatch()
    {
        return this.Model.CreateBasicPublishBatch();
    }

    public QueueDeclareOk QueueDeclarePassive(string queue)
    {
        return this.Model.QueueDeclarePassive(queue);
    }

    public QueueDeclareOk QueueDeclare(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object> arguments)
    {
        return this.Model.QueueDeclare(queue, durable, exclusive, autoDelete, arguments);
    }

    public void QueueBind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments)
    {
        this.Model.QueueBind(queue, exchange, routingKey, arguments);
    }

    public void BasicQos(uint prefetchSize, ushort prefetchCount, bool global)
    {
        this.Model.BasicQos(prefetchSize, prefetchCount, global);
    }

    public string BasicConsume(string queue, bool autoAck, IBasicConsumer consumer)
    {
        return this.Model.BasicConsume(queue, autoAck, consumer);
    }

    public void OnMessageConsumed(string consumerTag, ulong deliveryTag)
    {
        this.deliveredTags.TryAdd(deliveryTag, 0);
    }

    public void BasicAck(ulong deliveryTag, bool multiple)
    {
        if (this.deliveredTags.TryRemove(deliveryTag, out _))
        {
            this.Model.BasicAck(deliveryTag, multiple);
        }
        else
        {
            this.logger.LogError($"Failed to acknowledge the message. DeliveryTag ({deliveryTag}) not found.");
        }
    }

    public void BasicReject(ulong deliveryTag, bool requeue)
    {
        if (this.deliveredTags.TryRemove(deliveryTag, out _))
        {
            this.Model.BasicReject(deliveryTag, requeue);
        }
        else
        {
            this.logger.LogError($"Failed to reject the message. DeliveryTag ({deliveryTag}) not found.");
        }
    }

    public void BasicPublish(string exchange, string routingKey, IBasicProperties basicProperties, ReadOnlyMemory<byte> body)
    {
        this.Model.BasicPublish(exchange, routingKey, basicProperties, body);
    }

    public void BasicCancel(string consumerTag)
    {
        this.Model.BasicCancel(consumerTag);
    }

    public void ExchangeDeclare(string exchange, string exchangeType)
    {
        this.Model.ExchangeDeclare(exchange, exchangeType);
    }

    public void Close()
    {
        this.Model.Close();
    }
}
