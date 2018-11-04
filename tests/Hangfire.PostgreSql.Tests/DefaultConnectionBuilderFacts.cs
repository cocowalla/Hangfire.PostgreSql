using System;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Utils;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class DefaultConnectionBuilderFacts
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new DefaultConnectionBuilder(connectionString: null));

            Assert.Equal("connectionString", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsInvalid()
        {
            var exception = Assert.Throws<ArgumentException>(
                () => new DefaultConnectionBuilder("testtest"));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringBuilderIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new DefaultConnectionBuilder(connectionStringBuilder: null));

            Assert.Equal("connectionStringBuilder", exception.ParamName);
        }

        [Fact]
        public void ConnectionStringBuilder_Is_Readable()
        {
            var connectionString = ConnectionUtils.GetConnectionString();
            var builder = new DefaultConnectionBuilder(connectionString);

            Assert.NotNull(builder.ConnectionStringBuilder);
            Assert.NotNull(builder.ConnectionStringBuilder.ConnectionString);
        }

        [Fact]
        public void Build_CreatesConnection()
        {
            var builder = new DefaultConnectionBuilder(ConnectionUtils.GetConnectionString());

            using (var connection = builder.Build())
            {
                Assert.NotNull(connection);
            }
        }

        [Fact]
        public void Build_CreatedFixedUpConnection()
        {
            void FixUpAction(NpgsqlConnection conn) =>
                conn.UserCertificateValidationCallback += (sender, cert, chain, errors) => true;

            var builder = new DefaultConnectionBuilder(ConnectionUtils.GetConnectionString(), FixUpAction);

            using (var connection = builder.Build())
            {
                Assert.NotNull(connection);
                Assert.NotNull(connection.UserCertificateValidationCallback);
            }
        }
    }
}
