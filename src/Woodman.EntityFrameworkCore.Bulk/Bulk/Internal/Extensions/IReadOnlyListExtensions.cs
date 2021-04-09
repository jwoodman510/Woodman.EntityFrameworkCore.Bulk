using System;
using System.Collections.Generic;

namespace Microsoft.EntityFrameworkCore
{
    public static class IReadOnlyListExtensions
    {
        public static bool All<TSource>(this IReadOnlyList<TSource> source, Func<TSource, int, bool> predicate)
        {
            if(source == null)
            {
                return true;
            }

            for(var i = 0; i < source.Count; i++)
            {
                if(!predicate(source[i], i))
                {
                    return false;
                }
            }

            return true;
        }
    }
}
