using System.Threading;
using Hangfire.Storage;

namespace Hangfire.PostgreSql
{
    public interface IJobQueue
    {
        void Enqueue(string queue, string jobId);
        IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken);
    }
}
