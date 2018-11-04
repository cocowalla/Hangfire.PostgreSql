using System;
using Npgsql;

namespace Hangfire.PostgreSql.Connectivity
{
    public class DefaultConnectionBuilder : IConnectionBuilder
    {
        private readonly Action<NpgsqlConnection> _connectionAction;
        public NpgsqlConnectionStringBuilder ConnectionStringBuilder { get; }

        public DefaultConnectionBuilder(string connectionString, 
            Action<NpgsqlConnection> connectionAction = null) 
            : this(new NpgsqlConnectionStringBuilder(connectionString), connectionAction)
        {
            Guard.ThrowIfNull(connectionString, nameof(connectionString));
            Guard.ThrowIfConnectionStringIsInvalid(connectionString);
        }

        public DefaultConnectionBuilder(NpgsqlConnectionStringBuilder connectionStringBuilder, 
            Action<NpgsqlConnection> connectionAction = null)
        {
            Guard.ThrowIfNull(connectionStringBuilder, nameof(connectionStringBuilder));

            ConnectionStringBuilder = connectionStringBuilder;
            _connectionAction = connectionAction;
        }

        public NpgsqlConnection Build()
        {
            var connection = new NpgsqlConnection(ConnectionStringBuilder.ConnectionString);

            // Fixup the connection, if required
            _connectionAction?.Invoke(connection);

            return connection;
        }
    }
}
