using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using zero.core.network.ip;

namespace zero.cocoon.models.services
{
    /// <summary>
    /// A service description
    /// </summary>
    public class CcService
    {
        public CcRecord CcRecord = new CcRecord();
        public CcService()
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
