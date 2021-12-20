using BenchmarkDotNet.Running;
using zero.gauge.core.misc;

var summary = BenchmarkRunner.Run<ArrayExtensionGauge>();

