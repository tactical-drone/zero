using System;
using System.Collections.Generic;
using Newtonsoft.Json.Bson;

namespace zero.core.feat.models.bundle
{
    public interface IIoMessageBundle: IDisposable
    {
        IIoBundleMessage this[int i] { get; protected set; }
        IIoBundleMessage Feed { get; }
        int Count { get; }
        int Capacity { get; }

        bool Flush => Count >= Capacity;

        void Reset();
        //public Dictionary<byte[], Tuple<byte[], List<T>>> GroupBy { get; }
    }
}
