﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Tests.Utils;
using Hangfire.Server;
using Hangfire.Storage;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class StorageConnectionFacts
    {
        private readonly Mock<IJobQueue> _queue;
        private readonly PostgreSqlStorageOptions _options;

        public StorageConnectionFacts()
        {
            _queue = new Mock<IJobQueue>();
            _options = new PostgreSqlStorageOptions();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new StorageConnection(null, _queue.Object, _options));

            Assert.Equal("connectionProvider", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new StorageConnection(ConnectionUtils.GetConnectionProvider(), null, _options));

            Assert.Equal("queue", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new StorageConnection(ConnectionUtils.GetConnectionProvider(), _queue.Object, null));

            Assert.Equal("options", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Dispose_DoesNotDisposeTheConnection()
        {
            var sqlConnection = ConnectionUtils.GetConnectionProvider();
            var connection = new StorageConnection(sqlConnection, _queue.Object, _options);

            connection.Dispose();

            // Assert.Equal(sqlConnection.); TODO
        }

        [Fact, CleanDatabase]
        public void FetchNextJob_DelegatesItsExecution_ToTheQueue()
        {
            UseConnection(connection =>
            {
                var token = new CancellationToken();
                var queues = new[] { "default" };

                connection.FetchNextJob(queues, token);

                _queue.Verify(x => x.Dequeue(queues, token));
            });
        }

        [Fact, CleanDatabase]
        public void CreateWriteTransaction_ReturnsNonNullInstance()
        {
            UseConnection(connection =>
            {
                var transaction = connection.CreateWriteTransaction();
                Assert.NotNull(transaction);
            });
        }

        [Fact, CleanDatabase]
        public void AcquireLock_ReturnsNonNullInstance()
        {
            UseConnection(connection =>
            {
                var @lock = connection.AcquireDistributedLock("1", TimeSpan.FromSeconds(1));
                Assert.NotNull(@lock);
            });
        }

        [Fact, CleanDatabase]
        public void CreateExpiredJob_ThrowsAnException_WhenJobIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.CreateExpiredJob(
                        null,
                        new Dictionary<string, string>(),
                        DateTime.UtcNow,
                        TimeSpan.Zero));

                Assert.Equal("job", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void CreateExpiredJob_ThrowsAnException_WhenParametersCollectionIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.CreateExpiredJob(
                        Common.Job.FromExpression(() => Worker.DoWork("hello")),
                        null,
                        DateTime.UtcNow,
                        TimeSpan.Zero));

                Assert.Equal("parameters", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void CreateExpiredJob_CreatesAJobInTheStorage_AndSetsItsParameters()
        {
            UseConnections((sql, connection) =>
            {
                var createdAt = new DateTime(2012, 12, 12);
                var jobId = connection.CreateExpiredJob(
                    Common.Job.FromExpression(() => Worker.DoWork("Hello")),
                    new Dictionary<string, string> { { "Key1", "Value1" }, { "Key2", "Value2" } },
                    createdAt,
                    TimeSpan.FromDays(1));

                Assert.NotNull(jobId);
                Assert.NotEmpty(jobId);

                var sqlJob = sql.Query(@"select * from """ + GetSchemaName() + @""".""job""").Single();
                Assert.Equal(jobId, sqlJob.id.ToString());
                Assert.Equal(createdAt, sqlJob.createdat);
                Assert.Null((int?)sqlJob.stateid);
                Assert.Null((string)sqlJob.statename);

                var invocationData = JobHelper.FromJson<InvocationData>((string)sqlJob.invocationdata);
                invocationData.Arguments = sqlJob.arguments;

                var job = invocationData.Deserialize();
                Assert.Equal(typeof(Worker), job.Type);
                Assert.Equal(nameof(Worker.DoWork), job.Method.Name);
                Assert.Equal("Hello", job.Args[0]);

                Assert.True(createdAt.AddDays(1).AddMinutes(-1) < sqlJob.expireat);
                Assert.True(sqlJob.expireat < createdAt.AddDays(1).AddMinutes(1));

                var parameters = sql.Query(
                        @"select * from """ + GetSchemaName() + @""".""jobparameter"" where ""jobid"" = @id",
                        new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) })
                    .ToDictionary(x => (string)x.name, x => (string)x.value);

                Assert.Equal("Value1", parameters["Key1"]);
                Assert.Equal("Value2", parameters["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void GetJobData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>(
                () => connection.GetJobData(null)));
        }

        [Fact, CleanDatabase]
        public void GetJobData_ReturnsNull_WhenThereIsNoSuchJob()
        {
            UseConnection(connection =>
            {
                var result = connection.GetJobData("1");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetJobData_ReturnsResult_WhenJobExists()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""statename"", ""createdat"")
values (@invocationData, @arguments, @stateName, now() at time zone 'utc') returning ""id""";

            UseConnections((sql, connection) =>
            {
                var job = Common.Job.FromExpression(() => Worker.DoWork("wrong"));

                var jobId = (int)sql.Query(
                    arrangeSql,
                    new
                    {
                        invocationData = JobHelper.ToJson(InvocationData.Serialize(job)),
                        stateName = "Succeeded",
                        arguments = "[\"\\\"Arguments\\\"\"]"
                    }).Single().id;

                var result = connection.GetJobData(jobId.ToString());
                var now = DateTime.UtcNow.AddMinutes(-1);

                Assert.NotNull(result);
                Assert.NotNull(result.Job);
                Assert.Equal("Succeeded", result.State);
                Assert.Equal("Arguments", result.Job.Args[0]);
                Assert.Null(result.LoadException);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
                Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
            });
        }

        [Fact, CleanDatabase]
        public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(
                connection => Assert.Throws<ArgumentNullException>(
                    () => connection.GetStateData(null)));
        }

        [Fact, CleanDatabase]
        public void GetStateData_ReturnsNull_IfThereIsNoSuchState()
        {
            UseConnection(connection =>
            {
                var result = connection.GetStateData("1");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetStateData_ReturnsCorrectData()
        {
            string createJobSql = @"
INSERT INTO """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""statename"", ""createdat"")
    VALUES ('', '', '', now() at time zone 'utc') RETURNING ""id"";
            ";

            string createStateSql = @"
insert into """ + GetSchemaName() + @""".""state"" (""jobid"", ""name"", ""createdat"")
VALUES(@jobId, 'old-state', now() at time zone 'utc');

insert into """ + GetSchemaName() + @""".""state"" (""jobid"", ""name"", ""reason"", ""data"", ""createdat"")
VALUES(@jobId, @name, @reason, @data, now() at time zone 'utc')
returning ""id"";";

            string updateJobStateSql = @"
    update """ + GetSchemaName() + @""".""job""
    set ""stateid"" = @stateId
    where ""id"" = @jobId;
";

            UseConnections((sql, connection) =>
            {
                var data = new Dictionary<string, string>
                {
                    {"Key", "Value"}
                };

                var jobId = (int)sql.Query(createJobSql).Single().id;

                var stateId = (int)sql.Query(
                    createStateSql,
                    new { jobId = jobId, name = "Name", reason = "Reason", @data = JobHelper.ToJson(data) }).Single().id;

                sql.Execute(updateJobStateSql, new { jobId = jobId, stateId = stateId });

                var result = connection.GetStateData(jobId.ToString(CultureInfo.InvariantCulture));
                Assert.NotNull(result);

                Assert.Equal("Name", result.Name);
                Assert.Equal("Reason", result.Reason);
                Assert.Equal("Value", result.Data["Key"]);
            });
        }

        [Fact, CleanDatabase]
        public void GetJobData_ReturnsJobLoadException_IfThereWasADeserializationException()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""statename"", ""createdat"")
values (@invocationData, @arguments, @stateName, now() at time zone 'utc') returning ""id""";

            UseConnections((sql, connection) =>
            {
                var jobId = sql.Query(
                    arrangeSql,
                    new
                    {
                        invocationData = JobHelper.ToJson(new InvocationData(null, null, null, null)),
                        stateName = "Succeeded",
                        arguments = "['Arguments']"
                    }).Single();

                var result = connection.GetJobData(((int)jobId.id).ToString());

                Assert.NotNull(result.LoadException);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetJobParameter(null, "name", "value"));

                Assert.Equal("id", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetJobParameter("1", null, "value"));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameters_CreatesNewParameter_WhenParameterWithTheGivenNameDoesNotExists()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values ('', '', now() at time zone 'utc') returning ""id""";

            UseConnections((sql, connection) =>
            {
                var job = sql.Query(arrangeSql).Single();
                string jobId = job.id.ToString();

                connection.SetJobParameter(jobId, "Name", "Value");

                var parameter = sql.Query(
                    @"select * from """ + GetSchemaName() +
                    @""".""jobparameter"" where ""jobid"" = @id and ""name"" = @name",
                    new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture), name = "Name" }).Single();

                Assert.Equal("Value", parameter.value);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_UpdatesValue_WhenParameterWithTheGivenName_AlreadyExists()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values ('', '', now() at time zone 'utc') returning ""id""";

            UseConnections((sql, connection) =>
            {
                var job = sql.Query(arrangeSql).Single();
                string jobId = job.id.ToString();

                connection.SetJobParameter(jobId, "Name", "Value");
                connection.SetJobParameter(jobId, "Name", "AnotherValue");

                var parameter = sql.Query(
                    @"select * from """ + GetSchemaName() +
                    @""".""jobparameter"" where ""jobid"" = @id and ""name"" = @name",
                    new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture), name = "Name" }).Single();

                Assert.Equal("AnotherValue", parameter.value);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_CanAcceptNulls_AsValues()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values ('', '', now() at time zone 'utc') returning ""id""";

            UseConnections((sql, connection) =>
            {
                var job = sql.Query(arrangeSql).Single();
                string jobId = job.id.ToString();

                connection.SetJobParameter(jobId, "Name", null);

                var parameter = sql.Query(
                    @"select * from """ + GetSchemaName() +
                    @""".""jobparameter"" where ""jobid"" = @id and ""name"" = @name",
                    new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture), name = "Name" }).Single();

                Assert.Equal((string)null, parameter.value);
            });
        }

        [Fact, CleanDatabase]
        public void GetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobParameter(null, "hello"));

                Assert.Equal("id", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetJobParameter("1", null));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetParameter_ReturnsNull_WhenParameterDoesNotExists()
        {
            UseConnection(connection =>
            {
                var value = connection.GetJobParameter("1", "hello");
                Assert.Null(value);
            });
        }

        [Fact, CleanDatabase]
        public void GetParameter_ReturnsParameterValue_WhenJobExists()
        {
            string arrangeSql = @"
WITH ""insertedjob"" AS (
    INSERT INTO """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
    VALUES ('', '', now() at time zone 'utc') RETURNING ""id""
)
INSERT INTO """ + GetSchemaName() + @""".""jobparameter"" (""jobid"", ""name"", ""value"")
SELECT ""insertedjob"".""id"", @name, @value
FROM ""insertedjob""
RETURNING ""jobid"";
";
            UseConnections((sql, connection) =>
            {
                var id = sql.Query<int>(
                    arrangeSql,
                    new { name = "name", value = "value" }).Single();

                var value = connection.GetJobParameter(Convert.ToString(id, CultureInfo.InvariantCulture), "name");

                Assert.Equal("value", value);
            });
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetFirstByLowestScoreFromSet(null, 0, 1));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_ToScoreIsLowerThanFromScore()
        {
            UseConnection(connection => Assert.Throws<ArgumentException>(
                () => connection.GetFirstByLowestScoreFromSet("key", 0, -1)));
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenTheKeyDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetFirstByLowestScoreFromSet(
                    "key", 0, 1);

                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ReturnsTheValueWithTheLowestScore()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""set"" (""key"", ""score"", ""value"")
values 
('key', 1.0, '1.0'),
('key', -1.0, '-1.0'),
('key', -5.0, '-5.0'),
('another-key', -2.0, '-2.0')";

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql);

                var result = connection.GetFirstByLowestScoreFromSet("key", -1.0, 3.0);

                Assert.Equal("-1.0", result);
            });
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.AnnounceServer(null, new ServerContext()));

                Assert.Equal("serverId", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_ThrowsAnException_WhenContextIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.AnnounceServer("server", null));

                Assert.Equal("context", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_CreatesOrUpdatesARecord()
        {
            UseConnections((sql, connection) =>
            {
                var context1 = new ServerContext
                {
                    Queues = new[] { "critical", "default" },
                    WorkerCount = 4
                };
                connection.AnnounceServer("server", context1);

                var server = sql.Query(@"select * from """ + GetSchemaName() + @""".""server""").Single();
                Assert.Equal("server", server.id);
                Assert.True(((string)server.data).StartsWith(
                        "{\"WorkerCount\":4,\"Queues\":[\"critical\",\"default\"],\"StartedAt\":"),
                    server.data);
                Assert.NotNull(server.lastheartbeat);

                var context2 = new ServerContext
                {
                    Queues = new[] { "default" },
                    WorkerCount = 1000
                };
                connection.AnnounceServer("server", context2);
                var sameServer = sql.Query(@"select * from """ + GetSchemaName() + @""".""server""").Single();
                Assert.Equal("server", sameServer.id);
                Assert.Contains("1000", sameServer.data);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveServer_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>(
                () => connection.RemoveServer(null)));
        }

        [Fact, CleanDatabase]
        public void RemoveServer_RemovesAServerRecord()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""server"" (""id"", ""data"", ""lastheartbeat"")
values ('Server1', '', now() at time zone 'utc'),
('Server2', '', now() at time zone 'utc')";

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql);

                connection.RemoveServer("Server1");

                var server = sql.Query(@"select * from """ + GetSchemaName() + @""".""server""").Single();
                Assert.NotEqual("Server1", server.Id, StringComparer.OrdinalIgnoreCase);
            });
        }

        [Fact, CleanDatabase]
        public void Heartbeat_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection(connection => Assert.Throws<ArgumentNullException>(
                () => connection.Heartbeat(null)));
        }

        [Fact, CleanDatabase]
        public void Heartbeat_UpdatesLastHeartbeat_OfTheServerWithGivenId()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""server"" (""id"", ""data"", ""lastheartbeat"")
values
('server1', '', '2012-12-12 12:12:12'),
('server2', '', '2012-12-12 12:12:12')";

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql);

                connection.Heartbeat("server1");

                var servers = sql.Query(@"select * from """ + GetSchemaName() + @""".""server""")
                    .ToDictionary(x => (string)x.id, x => (DateTime)x.lastheartbeat);

                Assert.NotEqual(2012, servers["server1"].Year);
                Assert.Equal(2012, servers["server2"].Year);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveTimedOutServers_ThrowsAnException_WhenTimeOutIsNegative()
        {
            UseConnection(connection => Assert.Throws<ArgumentException>(
                () => connection.RemoveTimedOutServers(TimeSpan.FromMinutes(-5))));
        }

        [Fact, CleanDatabase]
        public void RemoveTimedOutServers_DoItsWorkPerfectly()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""server"" (""id"", ""data"", ""lastheartbeat"")
values (@id, '', @heartbeat)";

            UseConnections((sql, connection) =>
            {
                sql.Execute(
                    arrangeSql,
                    new[]
                    {
                        new {id = "server1", heartbeat = DateTime.UtcNow.AddDays(-1)},
                        new {id = "server2", heartbeat = DateTime.UtcNow.AddHours(-12)}
                    });

                connection.RemoveTimedOutServers(TimeSpan.FromHours(15));

                var liveServer = sql.Query(@"select * from """ + GetSchemaName() + @""".""server""").Single();
                Assert.Equal("server2", liveServer.id);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
                Assert.Throws<ArgumentNullException>(() => connection.GetAllItemsFromSet(null)));
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ReturnsEmptyCollection_WhenKeyDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllItemsFromSet("some-set");

                Assert.NotNull(result);
                Assert.Empty(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ReturnsAllItems()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""set"" (""key"", ""score"", ""value"")
values (@key, 0.0, @value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new {key = "some-set", value = "1"},
                    new {key = "some-set", value = "2"},
                    new {key = "another-set", value = "3"}
                });

                // Act
                var result = connection.GetAllItemsFromSet("some-set");

                // Assert
                Assert.Equal(2, result.Count);
                Assert.Contains("1", result);
                Assert.Contains("2", result);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash(null, new Dictionary<string, string>()));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash("some-hash", null));

                Assert.Equal("keyValuePairs", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_MergesAllRecords()
        {
            UseConnections((sql, connection) =>
            {
                connection.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    {"Key1", "Value1"},
                    {"Key2", "Value2"}
                });

                var result = sql.Query(
                        @"select * from """ + GetSchemaName() + @""".""hash"" where ""key"" = @key",
                        new { key = "some-hash" })
                    .ToDictionary(x => (string)x.field, x => (string)x.value);

                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
                Assert.Throws<ArgumentNullException>(() => connection.GetAllEntriesFromHash(null)));
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ReturnsNull_IfHashDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllEntriesFromHash("some-hash");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ReturnsAllKeysAndTheirValues()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""hash"" (""key"", ""field"", ""value"")
values (@key, @field, @value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new {key = "some-hash", field = "Key1", value = "Value1"},
                    new {key = "some-hash", field = "Key2", value = "Value2"},
                    new {key = "another-hash", field = "Key3", value = "Value3"}
                });

                // Act
                var result = connection.GetAllEntriesFromHash("some-hash");

                // Assert
                Assert.NotNull(result);
                Assert.Equal(2, result.Count);
                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void GetSetCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetSetCount(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetSetCount_ReturnsZero_WhenSetDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetSetCount("my-set");
                Assert.Equal(0, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetSetCount_ReturnsNumberOfElements_InASet()
        {
            string arrangeSql =
                $@"insert into ""{GetSchemaName()}"".set (key, value, score) values (@key, @value, 0.0)";

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql, new List<dynamic>
                {
                    new {key = "set-1", value = "value-1"},
                    new {key = "set-2", value = "value-1"},
                    new {key = "set-1", value = "value-2"}
                });

                var result = connection.GetSetCount("set-1");

                Assert.Equal(2, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetAllItemsFromList(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllItemsFromList("my-list");
                Assert.Empty(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ReturnsAllItems_FromAGivenList()
        {
            string arrangeSql = $@"insert into ""{GetSchemaName()}"".list (key, value) values (@key, @value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new {key = "list-1", value = "1"},
                    new {key = "list-2", value = "2"},
                    new {key = "list-1", value = "3"}
                });

                // Act
                var result = connection.GetAllItemsFromList("list-1");

                // Assert
                Assert.Equal(new[] { "3", "1" }, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetCounter_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetCounter(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetCounter_ReturnsZero_WhenKeyDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetCounter("my-counter");
                Assert.Equal(0, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetCounter_ReturnsSumOfValues_InCounterTable()
        {
            string arrangeSql = $@"insert into ""{GetSchemaName()}"".counter (key, value) values (@key, @value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new {key = "counter-1", value = 1},
                    new {key = "counter-2", value = 1},
                    new {key = "counter-1", value = 1}
                });

                // Act
                var result = connection.GetCounter("counter-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetListCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetListCount(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetListCount_ReturnsZero_WhenListDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetListCount("my-list");
                Assert.Equal(0, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetListCount_ReturnsTheNumberOfListElements()
        {
            string arrangeSql = $@"insert into ""{GetSchemaName()}"".list (key) values (@key)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new {key = "list-1"},
                    new {key = "list-1"},
                    new {key = "list-2"}
                });

                // Act
                var result = connection.GetListCount("list-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetListTtl(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ReturnsNegativeValue_WhenListDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetListTtl("my-list");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ReturnsExpirationTimeForList()
        {
            string arrangeSql = $@"insert into ""{GetSchemaName()}"".list (key, expireat) values (@key, @expireAt)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new {key = "list-1", expireAt = (DateTime?) DateTime.UtcNow.AddHours(1)},
                    new {key = "list-2", expireAt = (DateTime?) null}
                });

                // Act
                var result = connection.GetListTtl("list-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetRangeFromList(null, 0, 1));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetRangeFromList("my-list", 0, 1);
                Assert.Empty(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ReturnsAllEntries_WithinGivenBounds()
        {
            string arrangeSql = $@"insert into ""{GetSchemaName()}"".list (key, value) values (@key, @value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new {key = "list-1", value = "1"},
                    new {key = "list-2", value = "2"},
                    new {key = "list-1", value = "3"},
                    new {key = "list-1", value = "4"},
                    new {key = "list-1", value = "5"}
                });

                // Act
                var result = connection.GetRangeFromList("list-1", 1, 2);

                // Assert
                Assert.Equal(new[] { "4", "3" }, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetHashCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => { Assert.Throws<ArgumentNullException>(() => connection.GetHashCount(null)); });
        }

        [Fact, CleanDatabase]
        public void GetHashCount_ReturnsZero_WhenKeyDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetHashCount("my-hash");
                Assert.Equal(0, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetHashCount_ReturnsNumber_OfHashFields()
        {
            string arrangeSql = $@"insert into ""{GetSchemaName()}"".hash (key, field) values (@key, @field)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new {key = "hash-1", field = "field-1"},
                    new {key = "hash-1", field = "field-2"},
                    new {key = "hash-2", field = "field-1"}
                });

                // Act
                var result = connection.GetHashCount("hash-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetHashTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetHashTtl(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetHashTtl_ReturnsNegativeValue_WhenHashDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetHashTtl("my-hash");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact, CleanDatabase]
        public void GetHashTtl_ReturnsExpirationTimeForHash()
        {
            string arrangeSql =
                $@"insert into ""{GetSchemaName()}"".hash (key, field, expireat) values (@key, @field, @expireAt)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new {key = "hash-1", field = "field", expireAt = (DateTime?) DateTime.UtcNow.AddHours(1)},
                    new {key = "hash-2", field = "field", expireAt = (DateTime?) null}
                });

                // Act
                var result = connection.GetHashTtl("hash-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>(() => connection.GetRangeFromSet(null, 0, 1));
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromSet_ReturnsPagedElements()
        {
            string arrangeSql =
                $@"insert into ""{GetSchemaName()}"".set (key, value, score) values (@key, @value, 0.0)";

            UseConnections((sql, connection) =>
            {
                sql.Execute(arrangeSql, new List<dynamic>
                {
                    new {key = "set-1", value = "1"},
                    new {key = "set-1", value = "2"},
                    new {key = "set-1", value = "3"},
                    new {key = "set-1", value = "4"},
                    new {key = "set-2", value = "4"},
                    new {key = "set-1", value = "5"}
                });

                var result = connection.GetRangeFromSet("set-1", 2, 3);

                Assert.Equal(new[] { "3", "4" }, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection => { Assert.Throws<ArgumentNullException>(() => connection.GetSetTtl(null)); });
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ReturnsNegativeValue_WhenSetDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetSetTtl("my-set");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ReturnsExpirationTime_OfAGivenSet()
        {
            string arrangeSql =
                $@"insert into ""{
                        GetSchemaName()
                    }"".set (key, value, expireat, score) values (@key, @value, @expireAt, 0.0)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new {key = "set-1", value = "1", expireAt = (DateTime?) DateTime.UtcNow.AddMinutes(60)},
                    new {key = "set-2", value = "2", expireAt = (DateTime?) null}
                });

                // Act
                var result = connection.GetSetTtl("set-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetValueFromHash(null, "name"));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetValueFromHash("key", null));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ReturnsNull_WhenHashDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetValueFromHash("my-hash", "name");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ReturnsValue_OfAGivenField()
        {
            string arrangeSql =
                $@"insert into ""{GetSchemaName()}"".hash (key, field, value) values (@key, @field, @value)";

            UseConnections((sql, connection) =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new {key = "hash-1", field = "field-1", value = "1"},
                    new {key = "hash-1", field = "field-2", value = "2"},
                    new {key = "hash-2", field = "field-1", value = "3"}
                });

                // Act
                var result = connection.GetValueFromHash("hash-1", "field-1");

                // Assert
                Assert.Equal("1", result);
            });
        }

        private void UseConnections(Action<NpgsqlConnection, StorageConnection> action)
        {
            var provider = ConnectionUtils.GetConnectionProvider();
            using (var connection = new StorageConnection(provider, _queue.Object, _options))
            {
                using (var con = provider.AcquireConnection())
                {
                    action(con.Connection, connection);
                }
            }
        }

        private void UseConnection(Action<StorageConnection> action)
        {
            using (var connection = new StorageConnection(
                ConnectionUtils.GetConnectionProvider(),
                _queue.Object,
                _options))
            {
                action(connection);
            }
        }

        private static string GetSchemaName()
        {
            return ConnectionUtils.GetSchemaName();
        }

        public static class Worker
        {
            public static void DoWork(string argument)
            {
            }
        }
    }
}
