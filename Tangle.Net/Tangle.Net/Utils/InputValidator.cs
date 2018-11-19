﻿namespace Tangle.Net.Utils
{
  using System.Text.RegularExpressions;

  using Tangle.Net.Entity;

  /// <summary>
  /// The input validator.
  /// </summary>
  public static class InputValidator
  {
    #region Public Methods and Operators

    /// <summary>
    /// The is trytes.
    /// </summary>
    /// <param name="trytes">
    /// The trytes.
    /// </param>
    /// <returns>
    /// The <see cref="bool"/>.
    /// </returns>
    public static bool IsTrytes(string trytes)
    {
      var regex = new Regex("^[9A-Z]*$");
      return regex.IsMatch(trytes);
    }

    /// <summary>
    /// The is trytes.
    /// </summary>
    /// <param name="trytes">
    /// The trytes.
    /// </param>
    /// <param name="length">
    /// The length.
    /// </param>
    /// <returns>
    /// The <see cref="bool"/>.
    /// </returns>
    public static bool IsTrytes(string trytes, int length)
    {
      var regex = new Regex("^[9A-Z]{" + length + "}$");
      return regex.IsMatch(trytes);
    }

    /// <summary>
    /// The is trytes.
    /// </summary>
    /// <param name="trytes">
    /// The trytes.
    /// </param>
    /// <param name="start">
    /// The start.
    /// </param>
    /// <param name="length">
    /// The length.
    /// </param>
    /// <returns>
    /// The <see cref="bool"/>.
    /// </returns>
    public static bool IsTrytes(string trytes, int start, int length)
    {
      var regex = new Regex("^[9A-Z]{" + start + "," + length + "}$");
      return regex.IsMatch(trytes);
    }

    #endregion

    #region Methods

    /// <summary>
    /// The is address.
    /// </summary>
    /// <param name="address">
    /// The address.
    /// </param>
    /// <returns>
    /// The <see cref="bool"/>.
    /// </returns>
    public static bool IsAddress(string address)
    {
      if (address.Length == 90 && !IsTrytes(address, 90))
      {
        return false;
      }

      return IsTrytes(address, Address.Length);
    }

    #endregion
  }
}