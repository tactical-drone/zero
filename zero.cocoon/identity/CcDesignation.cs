﻿using System;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Base58Check;
using Org.BouncyCastle.Math.EC.Rfc8032;
using Org.BouncyCastle.Security;
using SimpleBase;

namespace zero.cocoon.identity
{
    public class CcDesignation
    {
        static CcDesignation()
        {
            SecureRandom.SetSeed(SecureRandom.GenerateSeed(256));
        }

        public const int PubKeySize = 256;
        public const int PubKeyLen = PubKeySize / 8;

        [ThreadStatic]
        private static SHA256 _sha256;
        public static SHA256 Sha256 => _sha256 ??= new SHA256Managed();
        public byte[] Id { get; set; }
        public byte[] PublicKey { get; private set; }
        public byte[] SecretKey { get; set; }

        private static string DevKey = "2BgzYHaa9Yp7TW6QjCe7qWb2fJxXg8xAeZpohW3BdqQZp41g3u";
        //private static string PubKey = "8PJmW2W74rJbFasdSTNaDLhGXyJC29EeyBN1Fmq3yQ2j";

        public string IdString()
        {
            return Base58.Bitcoin.Encode(Id.AsSpan().Slice(0, 8).ToArray());
        }

        public string PkString()
        {
            return Base58.Bitcoin.Encode(PublicKey);
        }

        public static CcDesignation FromPubKey(ReadOnlySpan<byte> pk)
        {
            var a = pk.ToArray();
            return new CcDesignation
            {
                PublicKey = a,
                Id = Sha256.ComputeHash(a)
            };
        }

        private static readonly SecureRandom SecureRandom = SecureRandom.GetInstance("SHA256PRNG");
        public static CcDesignation Generate(bool devMode = false)
        {
            var skBuf = Base58.Bitcoin.Decode(DevKey).ToArray();
            var pkBuf = new byte[Ed25519.PublicKeySize];
            
            if(!devMode)
                Ed25519.GeneratePrivateKey(SecureRandom, skBuf);
            
            Ed25519.GeneratePublicKey(skBuf, 0, pkBuf, 0);

            return new CcDesignation
            {
                PublicKey = pkBuf,
                SecretKey = skBuf.ToArray(),
                Id = Sha256.ComputeHash(pkBuf)
            };
        }

        public byte[] Sign(byte[] buffer, int offset, int len)
        {
            var sigBuf = new byte[Ed25519.SignatureSize];
            Ed25519.Sign(SecretKey, 0, buffer, offset, len, sigBuf, 0);
            return sigBuf;
        }

        public bool Verify(byte[] msg, int offset, int len, byte[] pubKey, int keyOffset, byte[] signature, int sigOffset)
        {
            return Ed25519.Verify(signature, sigOffset, pubKey, 0, msg, offset, len);
        }

        public override bool Equals(object obj)
        {
            var id = obj as CcDesignation;

            if (id == null)
                throw new ArgumentNullException(nameof(obj));

            return id == this || id.PublicKey.SequenceEqual(PublicKey);
        }

        public override int GetHashCode()
        {
            return MemoryMarshal.Read<int>(PublicKey);
        }
    }
}
