﻿using System;
using Hangfire.Annotations;

namespace Hangfire.PostgreSql.Entities
{
    [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
    internal class Server
    {
        public string Id { get; set; }
        public string Data { get; set; }
        public DateTime LastHeartbeat { get; set; }
    }
}
