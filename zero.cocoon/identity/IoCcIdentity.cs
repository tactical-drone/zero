using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Base58Check;
using MathNet.Numerics;
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
        private static string PubKey = "8PJmW2W74rJbFasdSTNaDLhGXyJC29EeyBN1Fmq3yQ2j";


        public static IoCcIdentity Generate()
        {
            //var skBuf = new byte[Ed25519.SecretKeySize];
            var skBuf = Base58CheckEncoding.Decode(DevKey);
            var pkBuf = new byte[Ed25519.PublicKeySize];

            //Ed25519.GeneratePrivateKey(SecureRandom.GetInstance("SHA256PRNG"), skBuf);
            
            Ed25519.GeneratePublicKey(skBuf, 0, pkBuf, 0);

            Console.WriteLine($"SK = {Base58CheckEncoding.Encode(skBuf)}");
            Console.WriteLine($"PK = {Base58CheckEncoding.EncodePlain(pkBuf)}");
            Console.WriteLine($"ID = {Base58CheckEncoding.EncodePlain(Sha256.ComputeHash(pkBuf).AsSpan().Slice(0,8).ToArray())}");

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
    }
}
