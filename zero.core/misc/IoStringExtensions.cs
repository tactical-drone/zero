using System;
using System.Collections.Generic;
using System.Text;

namespace zero.core.misc
{
    /// <summary>
    /// Replaces first occurrence of a search string with another string
    /// </summary>
    public static class IoStringExtensions
    {
        public static string ReplaceFirst(this string text, string search, string replace)
        {
            var pos = text.IndexOf(search, StringComparison.Ordinal);
            if (pos < 0)
            {
                return text;
            }
            return text[..pos] + replace + text[(pos + search.Length)..];
        }   
    }
}
