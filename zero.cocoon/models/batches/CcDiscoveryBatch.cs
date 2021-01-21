using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using Proto;
using zero.core.patterns.bushes.contracts;

namespace zero.cocoon.models.batches
{
    public class CcDiscoveryBatch
    {
        public IIoZero Zero;
        public IMessage EmbeddedMsg;
        public object UserData;
        public Packet Message;
    }
}
