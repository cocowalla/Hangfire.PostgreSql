using System;
using System.Collections.Generic;
using System.Linq;
using EasyNetQ.Management.Client;
using EasyNetQ.Management.Client.Model;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.PostgreSql.AMQP
{
    public class AmqpMonitoringApiDecorator : IMonitoringApi
    {
        private readonly ManagementClient _client;
        private readonly IMonitoringApi _monitoringApi;

        public AmqpMonitoringApiDecorator(IMonitoringApi monitoringApi)
        {
            _client = new ManagementClient("localhost", "guest", "guest"); ;
            _monitoringApi = monitoringApi;
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            var queues = _client.GetQueuesAsync().GetAwaiter().GetResult().ToList();

            var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Count);
            foreach (var queue in queues)
            {
                result.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Name = queue.Name,
                    Length = queue.Messages,
                    Fetched = queue.MessagesReady,
                    FirstJobs = new JobList<EnqueuedJobDto>(new KeyValuePair<string, EnqueuedJobDto>[0])
                });
            }

            return result;
        }

        public IList<ServerDto> Servers()
        {
            return _monitoringApi.Servers();
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            return _monitoringApi.JobDetails(jobId);
        }

        public StatisticsDto GetStatistics()
        {
            return _monitoringApi.GetStatistics();
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int @from, int perPage)
        {
            return _monitoringApi.EnqueuedJobs(queue, @from, perPage);
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int @from, int perPage)
        {
            return _monitoringApi.FetchedJobs(queue, @from, perPage);
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int @from, int count)
        {
            return _monitoringApi.ProcessingJobs(@from, count);
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int @from, int count)
        {
            return _monitoringApi.ScheduledJobs(@from, count);
        }

        public JobList<SucceededJobDto> SucceededJobs(int @from, int count)
        {
            return _monitoringApi.SucceededJobs(@from, count);
        }

        public JobList<FailedJobDto> FailedJobs(int @from, int count)
        {
            return _monitoringApi.FailedJobs(@from, count);
        }

        public JobList<DeletedJobDto> DeletedJobs(int @from, int count)
        {
            return _monitoringApi.DeletedJobs(@from, count);
        }

        public long ScheduledCount()
        {
            return _monitoringApi.ScheduledCount();
        }

        public long EnqueuedCount(string queue)
        {
            var q = _client.GetQueueAsync(queue, new Vhost { Name = "/" }).GetAwaiter().GetResult();
            return q.Messages;
        }

        public long FetchedCount(string queue)
        {
            var q = _client.GetQueueAsync(queue, new Vhost { Name = "/" }).GetAwaiter().GetResult();
            return q.MessagesReady;
        }

        public long FailedCount()
        {
            return _monitoringApi.FailedCount();
        }

        public long ProcessingCount()
        {
            return _monitoringApi.ProcessingCount();
        }

        public long SucceededListCount()
        {
            return _monitoringApi.SucceededListCount();
        }

        public long DeletedListCount()
        {
            return _monitoringApi.DeletedListCount();
        }

        public IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return _monitoringApi.SucceededByDatesCount();
        }

        public IDictionary<DateTime, long> FailedByDatesCount()
        {
            return _monitoringApi.FailedByDatesCount();
        }

        public IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return _monitoringApi.HourlySucceededJobs();
        }

        public IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return _monitoringApi.HourlyFailedJobs();
        }
    }
}
