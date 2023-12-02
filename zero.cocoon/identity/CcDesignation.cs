using System;
using System.Buffers;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Xml.Serialization;
using Google.Protobuf;
using MathNet.Numerics;
using Org.BouncyCastle.Crypto.Paddings;
using Org.BouncyCastle.Math.EC.Rfc8032;
using Org.BouncyCastle.Security;
using zero.core.misc;

namespace zero.cocoon.identity
{
    public class CcDesignation
    {
        public CcDesignation()
        {
            Reset();
        }

        public void Reset()
        {
            _ssf = null;
            _dh = ECDiffieHellman.Create();
            PrimedSabot = _dh.ExportSubjectPublicKeyInfo();
        }

        [ThreadStatic]
        private static SHA256 _sha256;
        public static SHA256 Sha256 => _sha256 ??= SHA256.Create();

        public const int KeyLength = 64;
        public const int IdLength = 10;

        private string _id;
        public byte[] PublicKey { get; private set; }
        //private byte[] SecretKey { get; private set; }
        private byte[] _secretKey;

        private const string DevKey = "2BgzYHaa9YpTW6QCe7qWb2JxXg8xAeZq";

        private ECDiffieHellman _dh;

        private byte[] _ssf;
        public byte[] Ssf => _ssf;
        public bool Primed => _ssf != null;
        public byte[] PrimedSabot { get; protected set; }

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

        private static SecureRandom _secureRandom;

        public static CcDesignation Generate(bool devMode = false)
        {
            var skBuf = Encoding.ASCII.GetBytes(DevKey);
            var pkBuf = new byte[Ed25519.PublicKeySize];
            
            _secureRandom ??= SecureRandom.GetInstance("SHA512PRNG", true);

            if (!devMode)
                Ed25519.GeneratePrivateKey(_secureRandom, skBuf);
            
            Ed25519.GeneratePublicKey(skBuf, 0, pkBuf, 0);

            return new CcDesignation
            {
                PublicKey = pkBuf,
                _secretKey = skBuf,
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ReadOnlyMemory<byte> HashRe(ReadOnlyMemory<byte> buffer, int offset, int len, byte[] output = null)
        {
            return sabot.Sabot.ComputeHash(buffer.Span, offset, len, output, raw:true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ReadOnlyMemory<byte> Hash(byte[] buffer, int offset, int len)
        {
            return sabot.Sabot.ComputeHash(buffer, offset, len);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ReadOnlyMemory<byte> Hash(ReadOnlyMemory<byte> buffer, int offset, int len)
        {
            return sabot.Sabot.ComputeHash(buffer.Span, offset, len);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ReadOnlyMemory<byte> Hash(byte[] array, int offset, int len, byte[] hash)
        {
            return sabot.Sabot.ComputeHash(array, offset, len, hash, hash.Length - sabot.Sabot.BlockLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Hashed(byte[] array, int offset, int len, byte[] dest, int destOffset, int destLen)
        {
            return array[offset..len].ArrayEqual(dest[destOffset..destLen]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Hashed(byte[] array, byte[] dest, int keySize)
        {
            return array[..(keySize>>3)].ArrayEqual(dest);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Signed(byte[] array, byte[] dest, int keySize)
        {
            return array[..keySize].ArrayEqual(dest);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Signed(byte[] array, ReadOnlyMemory<byte> dest, int keySize)
        {
            return array[..keySize].ArrayEqual(dest);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Hashed(byte[] array, byte[] dest)
        {
            return array.ArrayEqual(dest);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte[] Sign(byte[] buffer, int offset, int len)
        {
            var sigBuf = ArrayPool<byte>.Shared.Rent(Ed25519.SignatureSize);
            Ed25519.Sign(_secretKey, 0, buffer, offset, len, sigBuf, 0);
            return sigBuf;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte[] Sign(byte[] buffer, byte[] sigBuf, int offset, int len)
        {
            Ed25519.Sign(_secretKey, 0, buffer, offset, len, sigBuf, 0);
            return sigBuf;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Verify(byte[] msg, int offset, int len, byte[] pubKey, int keyOffset, byte[] signature, int sigOffset)
        {
            return Ed25519.Verify(signature, sigOffset, pubKey, keyOffset, msg, offset, len);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EnsureSabot(byte[] msg, int offset = 0, int len = 0)
        {
            //if primed do nothing
            if (_ssf != null || msg == null || msg.Length == 0)
                return;

            len = len switch
            {
                0 => msg.Length,
                _ => len
            };

            //if (len != _dh.KeySize >> 3)
            //    throw new ArgumentException($"{nameof(EnsureSabot)}: Invalid key size: got {len}, wanted {_dh.KeySize >> 3}");

            ECDiffieHellman alice = ECDiffieHellman.Create();
            alice.ImportSubjectPublicKeyInfo(msg[offset..len], out var read);
            if (read > 0)
            {
                //var key = ECDiffieHellmanCngPublicKey.FromByteArray(msg[offset..len], CngKeyBlobFormat.EccPublicBlob);
                var frequency = _dh.DeriveKeyFromHash(alice.PublicKey, HashAlgorithmName.SHA512);
                Interlocked.Exchange(ref _ssf, new byte[frequency.Length + sabot.Sabot.BlockLength]);
                frequency.CopyTo(_ssf.AsSpan());
            }
        }


        public ReadOnlyMemory<byte> Sabot(byte[] round) => sabot.Sabot.ComputeHash(round, output: (byte[])_ssf.Clone(), hashLength: _ssf.Length - sabot.Sabot.BlockLength);

        public ReadOnlyMemory<byte> Sabot(ReadOnlySpan<byte> round) => sabot.Sabot.ComputeHash(round, output: (byte[])_ssf.Clone(), hashLength: _ssf.Length - sabot.Sabot.BlockLength);

        public ReadOnlyMemory<byte> Sabot(ReadOnlySpan<byte> round, byte[] hash)
        {
            if (hash == null || _ssf == null)
                return Sabot(round);

            _ssf[..hash.Length].CopyTo(hash, 0);

            return sabot.Sabot.ComputeHash(round, output: hash, raw:true);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnPrime()
        {
            _ssf = null;
        }
    }
}
