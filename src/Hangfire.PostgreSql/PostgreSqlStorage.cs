using System;
using System.Collections.Generic;
using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Maintenance;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
    [PublicAPI]
    public sealed class PostgreSqlStorage : JobStorage, IDisposable
    {
        private readonly PostgreSqlStorageOptions _options;
        private readonly IConnectionProvider _connectionProvider;
        private readonly string _storageInfo;
        private readonly StorageConnection _storageConnection;
        public IMonitoringApi MonitoringApi { get; set; }

        /// <summary>
        /// Initializes PostgreSqlStorage with the provided connection string and default PostgreSqlStorageOptions.
        /// </summary>
        /// <param name="connectionString">A Postgres connection string</param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="connectionString"/> is not valid PostgreSql connection string </exception>
        public PostgreSqlStorage(string connectionString)
            : this(connectionString, new PostgreSqlStorageOptions())
        {
        }

        /// <summary>
        /// Initializes PostgreSqlStorage with the provided connection string and the provided PostgreSqlStorageOptions.
        /// </summary>
        /// <param name="connectionString">A Postgres connection string</param>
        /// <param name="options"></param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="connectionString"/> is not valid PostgreSql connection string.</exception>
        public PostgreSqlStorage(string connectionString, PostgreSqlStorageOptions options, IJobQueue queue = null)
        {
            Guard.ThrowIfNull(connectionString, nameof(connectionString));
            Guard.ThrowIfNull(options, nameof(options));
            Guard.ThrowIfConnectionStringIsInvalid(connectionString);

            _options = options;

            var builder = new NpgsqlConnectionStringBuilder(connectionString);
            _connectionProvider = CreateConnectionProvider(connectionString, builder);

            queue = queue ?? new JobQueue(_connectionProvider, _options);
            _storageConnection = new StorageConnection(_connectionProvider, queue, _options);
            MonitoringApi = new MonitoringApi(_connectionProvider);
            _storageInfo = $"PostgreSQL Server: Host: {builder.Host}, DB: {builder.Database}, Schema: {builder.SearchPath}, Pool: {_connectionProvider.GetType().Name}";

            PrepareSchemaIfNecessary(builder.SearchPath);
        }

        private static IConnectionProvider CreateConnectionProvider(string connectionString, NpgsqlConnectionStringBuilder connectionStringBuilder)
        {
            return connectionStringBuilder.Pooling
                ? new NpgsqlConnectionProvider(connectionString)
                : (IConnectionProvider)new DefaultConnectionProvider(connectionString);
        }

        private void PrepareSchemaIfNecessary(string schemaName)
        {
            if (_options.PrepareSchemaIfNecessary)
            {
                new DatabaseInitializer(_connectionProvider, schemaName).Initialize();
            }
        }

        internal IConnectionProvider ConnectionProvider => _connectionProvider;

        public override IMonitoringApi GetMonitoringApi() => MonitoringApi;

        public override IStorageConnection GetConnection() => _storageConnection;

#pragma warning disable 618 // TODO Remove when Hangfire 2.0 will be released
        public override IEnumerable<IServerComponent> GetComponents()
            => new IServerComponent[]
#pragma warning restore 618
            {
                new ExpirationManager(_connectionProvider),
                new ExpiredLocksManager(_connectionProvider, _options.DistributedLockTimeout),
                new CountersAggregationManager(_connectionProvider)
            };

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for SQL Server job storage:");
            logger.Info(_storageInfo);
            logger.InfoFormat("    Prepare schema: {0}.", _options.PrepareSchemaIfNecessary);
            logger.InfoFormat("    Queue poll interval: {0}.", _options.QueuePollInterval);
            logger.InfoFormat("    Invisibility timeout: {0}.", _options.InvisibilityTimeout);
            logger.InfoFormat("    Distributed lock timeout: {0}.", _options.DistributedLockTimeout);
        }

        public override string ToString() => _storageInfo;

        public void Dispose()
        {
            _connectionProvider.Dispose();
        }
    }
}
