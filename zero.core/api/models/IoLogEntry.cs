using System;
using System.Collections.Generic;
using System.Text;

namespace zero.core.api.models
{
    public class IoLogEntry
    {
        public string logMsg;

        public IoLogEntry(string logMsg)
        {
            this.logMsg = logMsg;
        }
    }
}
