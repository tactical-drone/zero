using System;
using System.Collections.Generic;
using System.Text;

namespace zero.core.data.providers.cassandra
{
    public interface IIoCassandraKeySpace
    {
        string Name { get; }
    }
}
