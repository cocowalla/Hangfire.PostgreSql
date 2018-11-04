using System;
using System.Threading.Tasks;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Utils;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class ConnectionProviderFacts
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionBuilderIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new DefaultConnectionProvider(null));

            Assert.Equal("connectionBuilder", exception.ParamName);
        }

        [Fact]
        public void AcquireConnection_CreatesOneConnection()
        {
            var provider = CreateProvider();

            using (var connectionHolder = provider.AcquireConnection())
            {
                Assert.Equal(1, provider.ActiveConnections);
            }
        }

        [Fact]
        public void AcquireConnection_CanReuseConnection()
        {
            var provider = CreateProvider();

            using (var connectionHolder = provider.AcquireConnection())
            {
                Assert.Equal(1, provider.ActiveConnections);
            }

            using (var connectionHolder2 = provider.AcquireConnection())
            {
                Assert.Equal(1, provider.ActiveConnections);
            }
        }

        [Fact]
        public void Dispose_ReleasesConnections()
        {
            const int connectionsCount = 10;
            var provider = CreateProvider();

            for (var i = 0; i < connectionsCount; i++)
            {
                var connection = provider.AcquireConnection();
                Task.Run(async () =>
                {
                    await Task.Delay(3000);
                    connection.Dispose();
                });
            }

            provider.Dispose();

            Assert.Equal(0, provider.ActiveConnections);
        }

        [Fact]
        public void Dispose_ThrowsTimeoutException_WhenConnectionsAreNotDisposedForTooLong()
        {
            var provider = CreateProvider();

            Assert.Throws<TimeoutException>(() =>
            {
                var connection = provider.AcquireConnection();
                Task.Run(async () =>
                {
                    await Task.Delay(DefaultConnectionProvider.DisposeTimeout + TimeSpan.FromSeconds(1));
                    connection.Dispose();
                });
                provider.Dispose();
            });
        }

        private static DefaultConnectionProvider CreateProvider() => new DefaultConnectionProvider(ConnectionUtils.GetConnectionBuilder());
    }
}
