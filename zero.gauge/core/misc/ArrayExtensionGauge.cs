using System.Collections;
using System.Security.Cryptography;
using BenchmarkDotNet.Attributes;
using zero.core.misc;

namespace zero.gauge.core.misc
{

    public class ArrayExtensionGauge
    {
        private readonly byte[] _small;
        private readonly byte[] _med;
        private readonly byte[] _large;

        public ArrayExtensionGauge()
        {
            _small = RandomNumberGenerator.GetBytes(3);
            _med = RandomNumberGenerator.GetBytes(4000);
            _large = RandomNumberGenerator.GetBytes(1024 * 1024 * 1024);
        }

        [Benchmark]
        public bool SmallEquals() => _small.ArrayEqual(_small);
        [Benchmark]
        public bool SmallSequenceEqual() => _small.SequenceEqual(_small);

        [Benchmark]
        public bool SmallIStructuralEquatable() => ((IStructuralEquatable)_small).Equals(_small, EqualityComparer<byte>.Default);

        [Benchmark]
        public bool SmallSequenceEqualSpan() => ((ReadOnlySpan<byte>)_small.AsSpan()).ArrayEqual(_small);

        [Benchmark]
        public bool MedEquals() => _med.ArrayEqual(_med);
        [Benchmark]
        public bool MedSequenceEqual() => _med.SequenceEqual(_med);
        [Benchmark]
        public bool MedIStructuralEquatable() => ((IStructuralEquatable)_med).Equals(_med, EqualityComparer<byte>.Default);
        [Benchmark]
        public bool MedSequenceEqualSpan() => ((ReadOnlySpan<byte>)_med.AsSpan()).ArrayEqual(_med);

        [Benchmark]
        public bool LargeEquals() => _large.ArrayEqual(_large);
        [Benchmark]
        public bool LargeSequenceEqual() => _large.SequenceEqual(_large);
        [Benchmark]
        public bool LargeIStructuralEquatable() => ((IStructuralEquatable)_large).Equals(_large, EqualityComparer<byte>.Default);
        [Benchmark]
        public bool LargeSequenceEqualSpan() => ((ReadOnlySpan<byte>)_large.AsSpan()).ArrayEqual(_large);
    }
}
