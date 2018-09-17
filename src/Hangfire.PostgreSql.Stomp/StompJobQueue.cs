using System;
using System.Linq;
using System.Text;
using System.Threading;
using Hangfire.Storage;
using Stomp.Net;
using Stomp.Net.Stomp.Commands;

namespace Hangfire.PostgreSql.Stomp
{
    public class StompJobQueue : IJobQueue
    {
        private readonly string _brokerUri;
        private readonly StompConnectionSettings _settings;
        private readonly ConnectionFactory _factory;

        [ThreadStatic]
        private static IConnection Connection;

        public StompJobQueue(string brokerUri, StompConnectionSettings settings)
        {
            _brokerUri = brokerUri;
            _settings = settings;
            _factory = new ConnectionFactory(brokerUri, _settings);
        }

        public void Enqueue(string queue, string jobId)
        {
            if (Connection == null)
            {
                Connection = _factory.CreateConnection();
                Connection.Start();
            }

            using (var session = Connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
            {
                var q = session.GetQueue(queue);
                using (var producer = session.CreateProducer(q))
                {
                    var message = producer.CreateBytesMessage(Encoding.UTF8.GetBytes(jobId));
                    producer.Send(message);
                }
            }
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            var queue = queues.First();
            if (Connection == null)
            {
                Connection = _factory.CreateConnection();
                Connection.Start();
            }

            var session = Connection.CreateSession(AcknowledgementMode.IndividualAcknowledge);
            var q = session.GetQueue(queue);
            var consumer = session.CreateConsumer(q);
            var message = consumer.Receive();
            return new StompFetchedJob(message, consumer, session);
        }
    }
}
