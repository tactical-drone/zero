using System;
using System.Collections.Generic;
using Newtonsoft.Json.Bson;

namespace zero.core.feat.models.bundle
{
    public interface IIoMessageBundle
    {
        IIoBundleMessage this[int i] { get; protected set; }
        IIoBundleMessage Feed();
        int Count { get; }
        int Capacity { get; }

        bool ReadyToFlush => Count >= Capacity;

        void Reset();
        //public Dictionary<byte[], Tuple<byte[], List<T>>> GroupBy { get; }
    }
}
