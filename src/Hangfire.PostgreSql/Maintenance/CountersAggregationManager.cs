﻿using System;
using System.Data;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.Server;

namespace Hangfire.PostgreSql.Maintenance
{
#pragma warning disable 618 // TODO Remove when Hangfire 2.0 will be released
    internal sealed class CountersAggregationManager : IBackgroundProcess, IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(ExpirationManager));

        private static readonly string[] ProcessedCounters =
        {
            "stats:succeeded",
            "stats:deleted",
        };

        private readonly IConnectionProvider _connectionProvider;
        private readonly TimeSpan _checkInterval;

        public CountersAggregationManager(IConnectionProvider connectionProvider)
            : this(connectionProvider, TimeSpan.FromHours(1))
        {
        }

        public CountersAggregationManager(IConnectionProvider connectionProvider, TimeSpan checkInterval)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfValueIsNotPositive(checkInterval, nameof(checkInterval));

            _connectionProvider = connectionProvider;
            _checkInterval = checkInterval;
        }

        public override string ToString() => "PostgreSql Counters Aggregation Manager";

        public void Execute(BackgroundProcessContext context)
        {
            Execute(context.CancellationToken);
        }

        public void Execute(CancellationToken cancellationToken)
        {
            AggregateCounters(cancellationToken);
            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        private void AggregateCounters(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var processedCounter in ProcessedCounters)
            {
                AggregateCounter(processedCounter);
                cancellationToken.ThrowIfCancellationRequested();
            }
        }

        private void AggregateCounter(string counterName)
        {
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            using (var transaction = connectionHolder.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                const string aggregateQuery = @"
WITH counters AS (
DELETE FROM counter
WHERE key = @counterName
AND expireat IS NULL
RETURNING *
)

SELECT SUM(value) FROM counters;
";

                var aggregatedValue = connectionHolder.Connection.ExecuteScalar<long>(aggregateQuery, new { counterName }, transaction);
                transaction.Commit();

                if (aggregatedValue > 0)
                {
                    const string query = @"INSERT INTO counter (key, value) VALUES (@key, @value);";
                    connectionHolder.Connection.Execute(query, new { key = counterName, value = aggregatedValue });
                }
                Logger.InfoFormat("Aggregated counter \'{0}\', value: {1}", counterName, aggregatedValue);
            }
        }
    }
}
