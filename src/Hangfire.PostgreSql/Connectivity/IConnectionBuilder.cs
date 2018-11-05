using Npgsql;

namespace Hangfire.PostgreSql.Connectivity
{
    /// <summary>
    /// Builds <see cref="NpgsqlConnection"/> Postgres connections
    /// </summary>
    public interface IConnectionBuilder
    {
        /// <summary>
        /// Used when returning connections in <see cref="Build"/>, this allows you to 'peek'
        /// at the configuration used by an <see cref="IConnectionBuilder"/>, without actually
        /// building a connection
        /// </summary>
        NpgsqlConnectionStringBuilder ConnectionStringBuilder { get; }

        /// <summary>
        /// Create a new <see cref="NpgsqlConnection"/> Postgres connection
        /// </summary>
        /// <returns>A new connection</returns>
        NpgsqlConnection Build();
    }
}
