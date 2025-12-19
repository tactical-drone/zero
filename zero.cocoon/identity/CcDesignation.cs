using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Google.Protobuf;
using NLog;
using Org.BouncyCastle.Math.EC.Rfc8032;
using Org.BouncyCastle.Security;
using zero.core.misc;
using zero.core.patterns.semaphore.core;

namespace zero.cocoon.identity;

public class CcDesignation
{
    public const int KeyLength = 64;

    private const string DevKey = "2BgzYHaa9YpTW6QCe7qWb2JxXg8xAeZq";

    private const int AesBlockSize = 16;

    /// <summary>
    ///     King stallion
    /// </summary>
    private const int CH53K = 2; //TODO: 2 should be overkill, more is paranoia

    /// <summary>
    ///     Sabot round size in millimeter
    /// </summary>
    public const int SABOT_MM = 64;

    /// <summary>
    ///     Hellman round size in millimeter
    /// </summary>
    public const int HELLM_MM = 32;

    public const int ZeroRoundSize = HELLM_MM + sabot.Sabot.BlockLength;

    private static SecureRandom _secureRandom;

    [ThreadStatic] private static SHA256 _sha256;

    private ECDiffieHellman[] _dh;

    private int _dhr;
    private int _dhrNext;

    private string _id;

    private byte[][] _primedSabot;

    private byte[] _secretKey;

    private byte[][] _ssf;

    public CcDesignation()
    {
        Reset();
    }

    public static SHA256 Sha256 => _sha256 ??= SHA256.Create();
    public byte[] PublicKey { get; private set; }
    private ECDiffieHellman DiffieHellman => _dh[ZeroRound];

    public byte[] Ssf
    {
        get
        {
            try
            {
                return _ssf[ZeroRound];
            }
            catch
            {
                return Ssf;
            }
        }
    }

    public bool Primed => ZeroRound > 0;

    public byte[] PrimedSabot
    {
        get
        {
            try
            {
                return _primedSabot[ZeroRound];
            }
            catch
            {
                return PrimedSabot;
            }
        }
    }

    public byte[][] Iv { get; private set; }

    //public int ZeroRound => Volatile.Read(ref _dhr);
    public int ZeroRound =>
        //Console.WriteLine($"{IdString()}({GetHashCode()}) - {GetRound(_dhr).PayloadSig()}");
        Volatile.Read(ref _dhr);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte[] Sabot(int round)
    {
        return _primedSabot[round];
    }

    public void Reset()
    {
        _dhr = _dhrNext = 0;
        _ssf = new byte[CH53K + 1][];

        var dmz = ECDiffieHellman.Create();
        _dh = [dmz, dmz, ECDiffieHellman.Create()];
        _primedSabot =
        [
            _dh[0].ExportSubjectPublicKeyInfo(), _dh[1].ExportSubjectPublicKeyInfo(),
            _dh[2].ExportSubjectPublicKeyInfo()
        ];
        Iv = new byte[CH53K + 1][];
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string MakeKey(byte[] keyBytes)
    {
        return Convert.ToBase64String(keyBytes.AsSpan()[..10])[..^2];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string MakeKey(ByteString keyBytes)
    {
        return MakeKey(keyBytes.Memory.AsArray());
    }


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
            _secretKey = skBuf
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlyMemory<byte> HashRe(ReadOnlyMemory<byte> buffer, int offset, int len, byte[] output = null)
    {
        return sabot.Sabot.ComputeHash(buffer.Span, offset, len, output, raw: true);
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
        return array[..(keySize >> 3)].ArrayEqual(dest);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool Signed(byte[] array, byte[] dest, int keySize)
    {
        return array[..keySize].ArrayEqual(dest);
    }

    //Console.WriteLine($"VERIFY sabot_# = {hash[..keySize].PayloadSig()}, data_# = {payload[..keySize].Span.PayloadSig()}");
    //Console.WriteLine($"VERIFY `{Convert.ToBase64String(hash[..keySize])}'");
    //Console.WriteLine($"VERIFY `{Convert.ToBase64String(payload[..keySize].Span)}'");

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool VerifyHash(byte[] hash, ReadOnlyMemory<byte> payload, int keySize)
    {
        return hash[..keySize].ArrayEqual(payload[..keySize]);
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
    public static bool Verify(byte[] msg, int offset, int len, byte[] pubKey, int keyOffset, byte[] signature,
        int sigOffset)
    {
        return Ed25519.Verify(signature, sigOffset, pubKey, keyOffset, msg, offset, len);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void EnsureSabot(int aes, byte[] pubKey, byte[] msg, int offset = 0, int len = 0)
    {
        try
        {
            var dhrNext = _dhrNext;
            //if primed do nothing
            if (aes != ZeroRound || msg == null || msg.Length == 0 || dhrNext >= CH53K ||
                (dhrNext = Interlocked.CompareExchange(ref _dhrNext, dhrNext + 1, dhrNext)) != dhrNext)
            {
                if (aes != ZeroRound && ZeroRound < CH53K && aes != 0)
                    LogManager.GetCurrentClassLogger()
                        .Warn($"hellman not ready, aes = {aes}, ZeroRound = {ZeroRound}, max = {CH53K}");
                return;
            }


            len = len switch
            {
                0 => msg.Length,
                _ => len
            };

            //if (len != _dh.KeySize >> 3)
            //    throw new ArgumentException($"{nameof(EnsureSabot)}: Invalid key size: got {len}, wanted {_dh.KeySize >> 3}");

            var alice = ECDiffieHellman.Create();
            alice.ImportSubjectPublicKeyInfo(msg[offset..len], out var read);
            if (read > 0)
            {
                //var key = ECDiffieHellmanCngPublicKey.FromByteArray(msg[offset..len], CngKeyBlobFormat.EccPublicBlob);
                var frequency = DiffieHellman.DeriveKeyFromHash(alice.PublicKey, HashAlgorithmName.SHA512);
                Debug.Assert(frequency.Length == SABOT_MM);
                Interlocked.Exchange(ref _ssf[dhrNext + 1], new byte[ZeroRoundSize]);
                frequency[..ZeroRoundSize].CopyTo(_ssf[dhrNext + 1]);
                SetIv(PublicKey, pubKey, dhrNext + 1);
                Interlocked.CompareExchange(ref _dhr, ZeroRound + 1, ZeroRound);
                Interlocked.MemoryBarrierProcessWide();
                LogManager.GetCurrentClassLogger()
                    .Debug(
                        $"hellman increased to {ZeroRound}, fire = {_ssf[dhrNext + 1].PayloadSig()} <-> {GetRound(dhrNext + 1).PayloadSig()} : {SABOT_MM * sizeof(int)} bit, id = {IdString()}, hash = {GetHashCode()}");
            }
            else
            {
                Interlocked.Decrement(ref _dhrNext);
            }
        }
        catch (Exception e)
        {
            LogManager.GetCurrentClassLogger().Error(e, "Ensuring sabot   [FAILED]");
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlyMemory<byte> Sabot(byte[] premiumAmmo)
    {
        return sabot.Sabot.ComputeHash(premiumAmmo, output: (byte[])Ssf.Clone(),
            hashLength: Ssf.Length - sabot.Sabot.BlockLength);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlyMemory<byte> Sabot(ReadOnlySpan<byte> premiumAmmo, int zeroRound)
    {
        return sabot.Sabot.ComputeHash(premiumAmmo, output: GetRound(zeroRound).ToArray(), raw: true, sabot: true);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlyMemory<byte> Sabot(ReadOnlySpan<byte> premiumAmmo, byte[] hash)
    {
        return sabot.Sabot.ComputeHash(premiumAmmo, output: hash, raw: true, sabot: true);
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

    public static bool operator <(CcDesignation left, CcDesignation right)
    {
        Debug.Assert(left != null && right != null);

        var c = (left.PublicKey[left.PublicKey.Length >> 1] + right.PublicKey[right.PublicKey.Length >> 1]) %
                left.PublicKey.Length;

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

        var c = (left.PublicKey[left.PublicKey.Length >> 1] + right.PublicKey[right.PublicKey.Length >> 1]) %
                left.PublicKey.Length;

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
        if (ZeroRound == 0)
            return;

        var prev = ZeroRound;
        if (_dhr.ZeroPrev(0) != -1)
            LogManager.GetCurrentClassLogger()
                .Debug($"Hellman down from {prev} to {ZeroRound}; id = {IdString()}, h = {GetHashCode()}");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlyMemory<byte> GetRound(int round)
    {
        return _ssf[round];
    }

    /// <summary>
    ///     TODO: What is this?
    /// </summary>
    /// <param name="designationPublicKey"></param>
    /// <param name="publicKey"></param>
    /// <param name="round"></param>
    /// <param name="force"></param>
    public void SetIv(byte[] designationPublicKey, byte[] publicKey, int round, bool force = false)
    {
        if (Iv[round] != null && !force) return;

        Iv[round] = new byte[AesBlockSize];
        try
        {
            for (var i = 0; i < AesBlockSize; i++)
                Iv[round][i] = (byte)(designationPublicKey[i] ^ publicKey[i] ^ _primedSabot[round][i]);
        }
        catch (Exception e)
        {
            LogManager.GetCurrentClassLogger().Error(e, $"{nameof(SetIv)}: failed: ");
            throw;
        }
    }
}