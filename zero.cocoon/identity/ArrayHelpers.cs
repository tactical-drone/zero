using System;
using System.Linq;

namespace Base58Check
{
  internal class ArrayHelpers
  {
    public static T[] ConcatArrays<T>(params T[][] arrays)
    {
      var result = new T[arrays.Sum(arr => arr.Length)];
      var offset = 0;

      foreach (var arr in arrays)
      {
        Buffer.BlockCopy(arr, 0, result, offset, arr.Count());
        offset += arr.Length;
      }

      return result;
    }

    public static T[] ConcatArrays<T>(T[] arr1, T[] arr2)
    {
      var result = new T[arr1.Length + arr2.Length];
      //TODO investigate Length vs Count
      Buffer.BlockCopy(arr1, 0, result, 0, arr1.Count());
      Buffer.BlockCopy(arr2, 0, result, arr1.Length, arr2.Count());

      return result;
    }

    public static T[] SubArray<T>(T[] arr, int start, int length)
    {
      var result = new T[length];
      Buffer.BlockCopy(arr, start, result, 0, length);

      return result;
    }

    public static T[] SubArray<T>(T[] arr, int start)
    {
      return SubArray(arr, start, arr.Length - start);
    }
  }
}
