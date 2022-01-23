using System.Runtime.InteropServices;
using System.Security.Cryptography;
using BenchmarkDotNet.Attributes;

namespace zero.gauge.core.misc
{
    public class CastPerf
    {
        readonly byte[] _array = RandomNumberGenerator.GetBytes(sizeof(ulong)*2);

        //[Benchmark]
        //public void MemoryMarshal_ulong()
        //{
        //    var v = MemoryMarshal.Read<ulong>(_array);
        //}

        //[Benchmark]
        //public void MemoryMarshal_uint()
        //{
        //    var v = MemoryMarshal.Read<int>(_array);
        //}

        //[Benchmark]
        //public void BitConverter_ulong()
        //{
        //    var v = BitConverter.ToUInt64(_array);
        //}

        //[Benchmark]
        //public void BitConverter_uint()
        //{
        //    var v = BitConverter.ToUInt32(_array);
        //}


        [Benchmark]
        public int MemoryMarshal_operator()
        {
            var r = 1;
            var v = MemoryMarshal.Read<uint>(_array[(r + sizeof(ushort))..]) == 0;
            return ++r;
        }

        [Benchmark]
        public int MemoryMarshal_manual()
        {
            var r = 1;
            Array.Clear(_array);
            r += sizeof(ushort);
            var v = _array[r + 0] == 0 && _array[r + 1] == 0 && _array[r + 2] == 0 && _array[r + 3] == 0;
            return ++r;
        }

        [Benchmark]
        public int MemoryMarshal_manual_slice()
        {
            var r = 1;
            Array.Clear(_array);
            r += sizeof(ushort);
            var cur = _array[(r + sizeof(uint))..];
            var v = cur[0] == 0 && cur[1] == 0 && cur[2] == 0 && cur[3] == 0;
            return ++r;
        }
    }
}
