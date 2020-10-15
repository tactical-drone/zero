using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using zero.core.network.ip;

namespace zero.cocoon.models.services
{
    public class CcRecord
    {
        public ConcurrentDictionary<CcService.Keys, IoNodeAddress> Endpoints = new ConcurrentDictionary<CcService.Keys, IoNodeAddress>();
    }
}
