using EasyNetQ;
using Hangfire.Storage;

namespace Hangfire.PostgreSql.AMQP
{
    public class AmqpJobQueueConsumer : IFetchedJob
    {
        private readonly string _queue;
        private readonly IBus _bus;
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;

        public AmqpJobQueueConsumer(string jobId, string queue, IBus bus)
        {
            JobId = jobId;
            _queue = queue;
            _bus = bus;
        }

        public string JobId { get; }

        public void RemoveFromQueue()
        {
            _removedFromQueue = true;
        }

        public void Requeue()
        {
            _bus.Publish(JobId, config => config.WithQueueName(_queue));
            _requeued = true;
        }

        public void Dispose()
        {
            if (_disposed) return;

            if (!_removedFromQueue && !_requeued)
            {
                Requeue();
            }

            _disposed = true;
        }
    }
}
