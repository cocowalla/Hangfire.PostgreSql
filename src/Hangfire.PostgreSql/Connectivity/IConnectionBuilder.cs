using Npgsql;

namespace Hangfire.PostgreSql.Connectivity
{
    public interface IConnectionBuilder
    {
        NpgsqlConnectionStringBuilder ConnectionStringBuilder { get; }

        NpgsqlConnection Build();
    }
}
