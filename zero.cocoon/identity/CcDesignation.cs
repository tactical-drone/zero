using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Org.BouncyCastle.Math.EC.Rfc8032;
using Org.BouncyCastle.Security;
using SimpleBase;
using zero.core.misc;

namespace zero.cocoon.identity
{
    public class CcDesignation
    {
        static CcDesignation()
        {
            SecureRandom.SetSeed(SecureRandom.GenerateSeed(PubKeySize));
        }

        private const int PubKeySize = 256;

        [ThreadStatic]
        private static SHA256 _sha256;
        public static SHA256 Sha256 => _sha256 ??= SHA256.Create();
        private byte[] Id { get; set; }
        public byte[] PublicKey { get; set; }
        private byte[] SecretKey { get; set; }

        private const string DevKey = "2BgzYHaa9Yp7TW6QjCe7qWb2fJxXg8xAeZpohW3BdqQZp41g3u";

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string IdString()
        {
            return Base58.Bitcoin.Encode(Id.AsSpan()[..10]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string PkString()
        {
            return Base58.Bitcoin.Encode(PublicKey);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string PkShort()
        {
            return Base58.Bitcoin.Encode(PublicKey.AsSpan()[..10]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static CcDesignation FromPubKey(ReadOnlyMemory<byte> pk)
        {
            var pkBuf = pk.AsArray();
            return new CcDesignation
            {
                PublicKey = pkBuf,
                Id = Sha256.ComputeHash(pkBuf)
            };
        }

        private static readonly SecureRandom SecureRandom = SecureRandom.GetInstance("SHA256PRNG");
        public static CcDesignation Generate(bool devMode = false)
        {
            var skBuf = Base58.Bitcoin.Decode(DevKey).ToArray();
            var pkBuf = new byte[Ed25519.PublicKeySize];

            if (!devMode)
                Ed25519.GeneratePrivateKey(SecureRandom, skBuf);
            
            Ed25519.GeneratePublicKey(skBuf, 0, pkBuf, 0);

            return new CcDesignation
            {
                PublicKey = pkBuf,
                SecretKey = skBuf,
                Id = Sha256.ComputeHash(pkBuf)
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte[] Sign(byte[] buffer, int offset, int len)
        {
            var sigBuf = new byte[Ed25519.SignatureSize];
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

            return id == this || id.PublicKey.SequenceEqual(PublicKey);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode()
        {
            return MemoryMarshal.Read<int>(PublicKey);
        }
    }
}
