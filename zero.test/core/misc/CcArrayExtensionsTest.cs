using System;
using Xunit;
using zero.core.misc;

namespace zero.test.core.misc
{
    public class CcArrayExtensionsTest
    {
        [Fact]
        void ArrayEqualTest()
        {
            ReadOnlySpan<byte> span1 = stackalloc byte[3]{0, 1, 2};
            ReadOnlySpan<byte> span2 = stackalloc byte[3]{0, 1, 2};
            ReadOnlySpan<byte> span3 = stackalloc byte[3]{0, 1, 2};
            ReadOnlySpan<byte> span4 = stackalloc byte[3] { 1, 1, 2 };
            ReadOnlySpan<byte> span5 = stackalloc byte[3] { 0, 1, 3 };
            ReadOnlySpan<byte> span6 = stackalloc byte[3] { 0, 2, 2 };

            Assert.True (span1.ArrayEqual(span2));
            Assert.True (span1.ArrayEqual(span2));
            Assert.True (span1.ArrayEqual(span3));
            Assert.False(span1.ArrayEqual(span4));
            Assert.False(span1.ArrayEqual(span5));
            Assert.False(span1.ArrayEqual(span6));

            var array1 = new byte[3] { 0, 1, 2 };
            var array2 = new byte[3] { 0, 1, 2 };
            var array3 = new byte[3] { 0, 1, 2 };
            var array4 = new byte[3] { 1, 1, 2 };
            var array5 = new byte[3] { 0, 1, 3 };
            var array6 = new byte[3] { 0, 2, 2 };

            Assert.True(array1.ArrayEqual(array2));
            Assert.True(array1.ArrayEqual(array2));
            Assert.True(array1.ArrayEqual(array3));
            Assert.False(array1.ArrayEqual(array4));
            Assert.False(array1.ArrayEqual(array5));
            Assert.False(array1.ArrayEqual(array6));

        }

        [Fact]
        void HashTestSmall()
        {
            ReadOnlySpan<byte> span1 = stackalloc byte[3] { 0, 1, 2 };
            ReadOnlySpan<byte> span2 = stackalloc byte[3] { 0, 1, 2 };
            ReadOnlySpan<byte> span3 = stackalloc byte[3] { 0, 1, 2 };
            ReadOnlySpan<byte> span4 = stackalloc byte[3] { 1, 1, 2 };
            ReadOnlySpan<byte> span5 = stackalloc byte[3] { 0, 1, 3 };
            ReadOnlySpan<byte> span6 = stackalloc byte[3] { 0, 2, 2 };

            Assert.Equal(span1.ZeroHash(), span2.ZeroHash());
            Assert.Equal(span1.ZeroHash(), span3.ZeroHash());
            Assert.NotEqual(span1.ZeroHash(), span4.ZeroHash());
            Assert.NotEqual(span1.ZeroHash(), span5.ZeroHash());
            Assert.NotEqual(span1.ZeroHash(), span6.ZeroHash());
        }

        [Fact]
        void HashTestMed()
        {
            Span<byte> span1 = stackalloc byte[sizeof(int)];
            Span<byte> span2 = stackalloc byte[sizeof(int)];
            Span<byte> span3 = stackalloc byte[sizeof(int) - 1];
            Span<byte> span4 = stackalloc byte[sizeof(int) - 2];
            Span<byte> span5 = stackalloc byte[sizeof(int) - 3];
            Span<byte> span6 = stackalloc byte[sizeof(int) - 4];


            System.Security.Cryptography.RandomNumberGenerator.Fill(span1);
            span1.CopyTo(span2);
            Assert.Equal(span1.ZeroHash(), span2.ZeroHash());

            var cmp = span1.ZeroHash();
            for (var i = 0; i < 1000000; i++)
            {
                System.Security.Cryptography.RandomNumberGenerator.Fill(span1);
                span1.CopyTo(span2);
                var idx = System.Security.Cryptography.RandomNumberGenerator.GetInt32(0, span2.Length - 1);
                span2[idx] = (byte)(span2[idx] ^ 0x1);

                Assert.NotEqual(span1.ZeroHash(), span2.ZeroHash());
            }
        }

        [Fact]
        void HashTestLarge()
        {
            Span<byte> span1 = stackalloc byte[sizeof(long) + 100];
            Span<byte> span2 = stackalloc byte[sizeof(long) + 100];
            
            System.Security.Cryptography.RandomNumberGenerator.Fill(span1);
            span1.CopyTo(span2);
            Assert.Equal(span1.ZeroHash(), span2.ZeroHash());

            var cmp = span1.ZeroHash();
            for (var i = 0; i < 1000000; i++)
            {
                System.Security.Cryptography.RandomNumberGenerator.Fill(span1);
                span1.CopyTo(span2);
                var idx = System.Security.Cryptography.RandomNumberGenerator.GetInt32(0, span2.Length - 1);
                span2[idx] = (byte)(span2[idx] ^ 0x1);

                Assert.NotEqual(span1.ZeroHash(), span2.ZeroHash());
            }
        }
    }
}
