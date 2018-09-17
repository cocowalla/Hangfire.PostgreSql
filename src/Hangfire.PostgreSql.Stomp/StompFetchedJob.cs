using System.Text;
using Hangfire.Storage;
using Stomp.Net;

namespace Hangfire.PostgreSql.Stomp
{
    public class StompFetchedJob : IFetchedJob
    {
        private readonly IBytesMessage _message;
        private readonly IMessageConsumer _consumer;
        private readonly ISession _session;

        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;
        
        public StompFetchedJob(IBytesMessage message, IMessageConsumer consumer, ISession session)
        {
            _message = message;
            _consumer = consumer;
            _session = session;
        }

        public void RemoveFromQueue()
        {
            _message.Acknowledge();
            _removedFromQueue = true;
        }

        public void Requeue()
        {
            _requeued = true;
        }

        public void Dispose()
        {
            if (_disposed) return;

            if (!_removedFromQueue && !_requeued)
            {
                Requeue();
            }

            _consumer?.Dispose();
            _session?.Dispose();
            _disposed = true;
        }

        public string JobId => Encoding.UTF8.GetString(_message.Content);
    }
}
