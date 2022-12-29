using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using Google.Protobuf;
using Org.BouncyCastle.Math.EC.Rfc8032;
using Org.BouncyCastle.Security;
using zero.core.misc;

namespace zero.cocoon.identity
{
    public class CcDesignation
    {

        [ThreadStatic]
        private static SHA256 _sha256;
        public static SHA256 Sha256 => _sha256 ??= SHA256.Create();

        public const int KeyLength = 64;
        public const int IdLength = 10;

        private string _id;
        public byte[] PublicKey { get; set; }
        private byte[] SecretKey { get; set; }

        private const string DevKey = "2BgzYHaa9Yp7TW6QjCe7qWb2fJxXg8xAeZpohW3BdqQZp41g3u";

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string MakeKey(byte[] keyBytes)
        {
            return Convert.ToBase64String(keyBytes.AsSpan()[..10])[..^2];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string MakeKey(ByteString keyBytes) => MakeKey(keyBytes.Memory.AsArray());
        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string IdString()
        {
            return _id ??= MakeKey(PublicKey);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static CcDesignation FromPubKey(ReadOnlyMemory<byte> pk)
        {
            var pkBuf = pk.AsArray();
            return new CcDesignation
            {
                PublicKey = pkBuf
            };
        }

        private static SecureRandom SecureRandom;
        public static CcDesignation Generate(bool devMode = false)
        {
            var skBuf = Encoding.ASCII.GetBytes(DevKey);
            var pkBuf = new byte[Ed25519.PublicKeySize];
            
            SecureRandom ??= SecureRandom.GetInstance("SHA512PRNG", true);

            if (!devMode)
                Ed25519.GeneratePrivateKey(SecureRandom, skBuf);
            
            Ed25519.GeneratePublicKey(skBuf, 0, pkBuf, 0);

            return new CcDesignation
            {
                PublicKey = pkBuf,
                SecretKey = skBuf
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte[] Sign(byte[] buffer, int offset, int len)
        {
            var sigBuf = ArrayPool<byte>.Shared.Rent(Ed25519.SignatureSize);
            Ed25519.Sign(SecretKey, 0, buffer, offset, len, sigBuf, 0);
            return sigBuf;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte[] Sign(byte[] buffer, byte[] sigBuf, int offset, int len)
        {
            Ed25519.Sign(SecretKey, 0, buffer, offset, len, sigBuf, 0);
            return sigBuf;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Verify(byte[] msg, int offset, int len, byte[] pubKey, int keyOffset, byte[] signature, int sigOffset)
        {
            return Ed25519.Verify(signature, sigOffset, pubKey, keyOffset, msg, offset, len);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Equals(object obj)
        {
            if (obj is not CcDesignation id)
                throw new ArgumentNullException(nameof(obj));

            return id == this || id.PublicKey.ArrayEqual(PublicKey);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode()
        {
            return MemoryMarshal.Read<int>(PublicKey);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override string ToString()
        {
            return IdString();
        }

        public static bool operator < (CcDesignation left, CcDesignation right)
        {
            Debug.Assert(left != null && right != null);

            var c = (left.PublicKey[left.PublicKey.Length >> 1] + right.PublicKey[right.PublicKey.Length >> 1]) % left.PublicKey.Length;

            byte l;
            byte r;
            do
            {
                l = left.PublicKey[c];
                r = right.PublicKey[c];
                c = (c + 1) % left.PublicKey.Length;
            } while (r == l);
            
                
            return l < r;
        }

        public static bool operator >(CcDesignation left, CcDesignation right)
        {
            Debug.Assert(left != null && right != null);

            var c = (left.PublicKey[left.PublicKey.Length >> 1] + right.PublicKey[right.PublicKey.Length >> 1]) % left.PublicKey.Length;

            byte l;
            byte r;
            do
            {
                l = left.PublicKey[c];
                r = right.PublicKey[c];
                c = (c + 1) % left.PublicKey.Length;
            } while (r == l);


            return l > r;
        }
    }
}
