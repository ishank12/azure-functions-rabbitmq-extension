// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Microsoft.Azure.WebJobs.Extensions.RabbitMQ;

public class RabbitMQMessageActions
{
    private readonly IModel channel;
    private readonly BasicDeliverEventArgs message;

    internal RabbitMQMessageActions(IModel channel, BasicDeliverEventArgs message)
    {
        this.channel = channel;
        this.message = message;
    }

    public async Task Reject()
    {
        await Task.Run(() => this.channel.BasicReject(this.message.DeliveryTag, requeue: false));
    }

    public async Task Acknowledge()
    {
        await Task.Run(() => this.channel.BasicAck(this.message.DeliveryTag, multiple: false));
    }
}
