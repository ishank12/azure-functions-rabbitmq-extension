// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System.Threading.Tasks;
using RabbitMQ.Client.Events;

namespace Microsoft.Azure.WebJobs.Extensions.RabbitMQ;

public class RabbitMQMessageActions
{
    private readonly IRabbitMQModel rabbitMQMOdel;
    private readonly BasicDeliverEventArgs message;

    internal RabbitMQMessageActions(IRabbitMQModel rabbitMQMOdel, BasicDeliverEventArgs message)
    {
        this.rabbitMQMOdel = rabbitMQMOdel;
        this.message = message;
    }

    public async Task Reject()
    {
        await Task.Run(() => this.rabbitMQMOdel.BasicReject(this.message.DeliveryTag, requeue: false));
    }

    public async Task Acknowledge()
    {
        await Task.Run(() => this.rabbitMQMOdel.BasicAck(this.message.DeliveryTag, multiple: false));
    }
}
