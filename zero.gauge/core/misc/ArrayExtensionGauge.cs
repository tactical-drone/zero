using System.Collections;
using System.Security.Cryptography;
using BenchmarkDotNet.Attributes;
using zero.core.misc;

namespace zero.gauge.core.misc;

public class ArrayExtensionGauge
{
    private readonly byte[] _large;
    private readonly byte[] _med;
    private readonly byte[] _small;

    public ArrayExtensionGauge()
    {
        _small = RandomNumberGenerator.GetBytes(3);
        _med = RandomNumberGenerator.GetBytes(4000);
        _large = RandomNumberGenerator.GetBytes(1024 * 1024 * 1024);
    }

    [Benchmark]
    public bool SmallEquals()
    {
        return _small.ArrayEqual(_small);
    }

    [Benchmark]
    public bool SmallSequenceEqual()
    {
        return _small.SequenceEqual(_small);
    }

    [Benchmark]
    public bool SmallIStructuralEquatable()
    {
        return ((IStructuralEquatable)_small).Equals(_small, EqualityComparer<byte>.Default);
    }

    [Benchmark]
    public bool SmallSequenceEqualSpan()
    {
        return _small.AsSpan().ArrayEqual(_small);
    }

    [Benchmark]
    public bool MedEquals()
    {
        return _med.ArrayEqual(_med);
    }

    [Benchmark]
    public bool MedSequenceEqual()
    {
        return _med.SequenceEqual(_med);
    }

    [Benchmark]
    public bool MedIStructuralEquatable()
    {
        return ((IStructuralEquatable)_med).Equals(_med, EqualityComparer<byte>.Default);
    }

    [Benchmark]
    public bool MedSequenceEqualSpan()
    {
        return _med.AsSpan().ArrayEqual(_med);
    }

    [Benchmark]
    public bool LargeEquals()
    {
        return _large.ArrayEqual(_large);
    }

    [Benchmark]
    public bool LargeSequenceEqual()
    {
        return _large.SequenceEqual(_large);
    }

    [Benchmark]
    public bool LargeIStructuralEquatable()
    {
        return ((IStructuralEquatable)_large).Equals(_large, EqualityComparer<byte>.Default);
    }

    [Benchmark]
    public bool LargeSequenceEqualSpan()
    {
        return _large.AsSpan().ArrayEqual(_large);
    }
}