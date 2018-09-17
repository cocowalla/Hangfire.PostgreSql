using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EasyNetQ;
using EasyNetQ.Topology;
using Hangfire.Storage;

namespace Hangfire.PostgreSql.AMQP
{
    public class AmqpJob
    {
        public string JobId { get; set; }
    }

    public class AmqpJobQueue : IJobQueue
    {
        private readonly IBus _bus;
        public AmqpJobQueue()
        {
            _bus = RabbitHutch.CreateBus("host=localhost;publisherConfirms=true;timeout=10");
        }

        public void Enqueue(string queue, string jobId)
        {
            _bus.Advanced.Publish(Exchange.GetDefault(), queue, true, new Message<AmqpJob>(new AmqpJob { JobId = jobId }));
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            var queue = queues.First();
            var q = _bus.Advanced.QueueDeclare(queue);
            var result = _bus.Advanced.Get<AmqpJob>(q);

            while (result == null)
            {
                result = _bus.Advanced.Get<AmqpJob>(new Queue(queue, true));
                cancellationToken.ThrowIfCancellationRequested();
            }

            return new AmqpJobQueueConsumer(result.Message.Body.JobId, queue, _bus);
        }
    }
}
