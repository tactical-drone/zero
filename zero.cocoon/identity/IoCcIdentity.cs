using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Base58Check;
using MathNet.Numerics;
using Microsoft.AspNetCore.Mvc.ModelBinding.Binders;
using Org.BouncyCastle.Math.EC.Rfc8032;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.Utilities.Encoders;

namespace zero.cocoon.identity
{
    public class IoCcIdentity
    {
        public static SHA256 Sha256 = SHA256.Create();
        public byte[] Id { get; set; }
        public byte[] PublicKey { get; set; }
        public byte[] SecretKey { get; set; }

        private static string DevKey = "2BgzYHaa9Yp7TW6QjCe7qWb2fJxXg8xAeZpohW3BdqQZp41g3u";
        //private static string PubKey = "8PJmW2W74rJbFasdSTNaDLhGXyJC29EeyBN1Fmq3yQ2j";

        public string IdString()
        {
            return Base58CheckEncoding.EncodePlain(Id.AsSpan().Slice(0, 8).ToArray());
        }

        public string PkString()
        {
            return Base58CheckEncoding.EncodePlain(PublicKey);
        }

        public static IoCcIdentity FromPK(string pk)
        {
            var pkBuf = Encoding.ASCII.GetBytes(pk);
            return new IoCcIdentity
            {
                PublicKey = pkBuf,
                Id = Sha256.ComputeHash(pkBuf.AsSpan().Slice(0,8).ToArray())
            };
        }

        public static IoCcIdentity FromPK(ReadOnlySpan<byte> pk)
        {
            return new IoCcIdentity
            {
                PublicKey = pk.ToArray(),
                Id = Sha256.ComputeHash(pk.Slice(0, 8).ToArray())
            };
        }

        public static IoCcIdentity Generate(bool devMode = false)
        {
            //var skBuf = new byte[Ed25519.SecretKeySize];
            var skBuf = Base58CheckEncoding.Decode(DevKey);
            var pkBuf = new byte[Ed25519.PublicKeySize];

            if(!devMode)
                Ed25519.GeneratePrivateKey(SecureRandom.GetInstance("SHA256PRNG"), skBuf);
            
            Ed25519.GeneratePublicKey(skBuf, 0, pkBuf, 0);

            //Console.WriteLine($"SK = {Base58CheckEncoding.Encode(skBuf)}");
            //Console.WriteLine($"PK = {Base58CheckEncoding.EncodePlain(pkBuf)}");
            //Console.WriteLine($"ID = {Base58CheckEncoding.EncodePlain(Sha256.ComputeHash(pkBuf).AsSpan().Slice(0,8).ToArray())}");

            return new IoCcIdentity
            {
                PublicKey = pkBuf,
                SecretKey = skBuf,
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
    }
}
