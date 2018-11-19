﻿namespace Tangle.Net.Entity
{
  using System;

  using Tangle.Net.Cryptography;
  using Tangle.Net.Utils;

  /// <summary>
  /// The address.
  /// </summary>
  public class Address : TryteString
  {
    /// <summary>
    /// The length.
    /// </summary>
    public const int Length = 81;

    /// <summary>
    /// Initializes a new instance of the <see cref="Address"/> class.
    /// </summary>
    /// <param name="value">
    /// The value.
    /// </param>
    public Address(string value)
      : base(value)
    {
      if (this.TrytesLength < Length)
      {
        this.Pad(Length);
      }

      if (!InputValidator.IsTrytes(this.Value, Length) && !InputValidator.IsTrytes(this.Value, Length + Checksum.Length))
      {
        throw new ArgumentException(
          "Given value is no address. Address should be of length " + Length + ". Or " + (Length + Checksum.Length) + " if provided with checksum.");
      }

      if (this.TrytesLength == Length)
      {
        return;
      }

      this.Checksum = new Checksum(this.Value.Substring(Length, Checksum.Length));
      this.Value = this.Value.Substring(0, Length);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Address"/> class.
    /// </summary>
    public Address()
      : this(string.Empty)
    {
    }

    /// <summary>
    /// Gets or sets the balance.
    /// </summary>
    public long Balance { get; set; }

    /// <summary>
    /// Gets the checksum.
    /// </summary>
    public Checksum Checksum { get; private set; }

    /// <summary>
    /// Gets or sets the key index.
    /// </summary>
    public int KeyIndex { get; set; }

    /// <summary>
    /// Gets or sets the private key.
    /// </summary>
    public AbstractPrivateKey PrivateKey { get; set; }

    /// <summary>
    /// Gets or sets the security level.
    /// </summary>
    public int SecurityLevel { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether spent from.
    /// </summary>
    public bool SpentFrom { get; set; }

    /// <summary>
    /// The from trits.
    /// </summary>
    /// <param name="addressTrits">
    /// The address trits.
    /// </param>
    /// <returns>
    /// The <see cref="Address"/>.
    /// </returns>
    public static Address FromTrits(int[] addressTrits)
    {
      return new Address(Converter.TritsToTrytes(addressTrits));
    }

    /// <summary>
    /// The has valid checksum.
    /// </summary>
    /// <returns>
    /// The <see cref="bool"/>.
    /// </returns>
    public bool HasValidChecksum()
    {
      return this.Checksum != null && this.Checksum.Value == Checksum.FromAddress(this).Value;
    }

    /// <summary>
    /// The to trytes.
    /// </summary>
    /// <returns>
    /// The <see cref="string"/>.
    /// </returns>
    public string ToTrytes()
    {
      var checksumValue = this.Checksum != null ? this.Checksum.Value : string.Empty;
      return this.Value + checksumValue;
    }

    /// <summary>
    /// The with checksum.
    /// </summary>
    /// <returns>
    /// The <see cref="Address"/>.
    /// </returns>
    public Address WithChecksum()
    {
      if (this.Checksum == null)
      {
        this.Checksum = Checksum.FromAddress(this);
      }

      return this;
    }
  }
}