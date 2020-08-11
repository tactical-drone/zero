using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using zero.core.network.ip;

namespace zero.cocoon.models.services
{
    public class IoCcRecord
    {
        public ConcurrentDictionary<IoCcService.Keys, IoNodeAddress> Endpoints = new ConcurrentDictionary<IoCcService.Keys, IoNodeAddress>();
    }
}
