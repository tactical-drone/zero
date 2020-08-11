using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using zero.core.network.ip;

namespace zero.cocoon.models.services
{
    public class IoCcService
    {
        public IoCcRecord IoCcRecord = new IoCcRecord();
        public IoCcService()
        {
            
        }

        public enum Keys
        {
            peering,
            fpc,
            gossip
        }
    }
}
