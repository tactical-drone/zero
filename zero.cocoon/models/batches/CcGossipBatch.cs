using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Proto;
using zero.core.patterns.bushes.contracts;

namespace zero.cocoon.models.batches
{ 
    public class CcGossipBatch
    {
        public IIoZero Zero;
        public object UserData;
        public CcGossipMsg Message;
    }
}
