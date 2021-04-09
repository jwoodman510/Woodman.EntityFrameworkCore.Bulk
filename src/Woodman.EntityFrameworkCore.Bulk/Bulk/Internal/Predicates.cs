using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Woodman.EntityFrameworkCore.Bulk.Bulk.Internal
{
    internal static class Predicates
    {
        internal static Expression<Func<TEntity, bool>> ContainsPrimaryKeys<TEntity>(string keyName, IEnumerable<object> keys) =>
            entity => keys.Contains(EF.Property<object>(entity, keyName));

        internal static Expression<Func<TEntity, bool>> ContainsCompositeKeys<TEntity>(List<string> keyNames, List<object[]> keys)
        {
            var translatableKeys = BuildKeyTuples(keyNames, keys);

            switch (keyNames.Count)
            {
                case 2:
                    return e => translatableKeys.Contains(
                        new Tuple<object, object>(
                        EF.Property<object>(e, keyNames[0]),
                        EF.Property<object>(e, keyNames[1])));
                case 3:
                    return e => translatableKeys.Contains(
                        new Tuple<object, object, object>(
                        EF.Property<object>(e, keyNames[0]),
                        EF.Property<object>(e, keyNames[1]),
                        EF.Property<object>(e, keyNames[2])));
                case 4:
                    return e => translatableKeys.Contains(
                        new Tuple<object, object, object, object>(
                        EF.Property<object>(e, keyNames[0]),
                        EF.Property<object>(e, keyNames[1]),
                        EF.Property<object>(e, keyNames[2]),
                        EF.Property<object>(e, keyNames[3])));
                case 5:
                    return e => translatableKeys.Contains(
                        new Tuple<object, object, object, object, object>(
                        EF.Property<object>(e, keyNames[0]),
                        EF.Property<object>(e, keyNames[1]),
                        EF.Property<object>(e, keyNames[2]),
                        EF.Property<object>(e, keyNames[3]),
                        EF.Property<object>(e, keyNames[4])));
                case 6:
                    return e => translatableKeys.Contains(
                        new Tuple<object, object, object, object, object, object>(
                        EF.Property<object>(e, keyNames[0]),
                        EF.Property<object>(e, keyNames[1]),
                        EF.Property<object>(e, keyNames[2]),
                        EF.Property<object>(e, keyNames[3]),
                        EF.Property<object>(e, keyNames[4]),
                        EF.Property<object>(e, keyNames[5])));
                case 7:
                    return e => translatableKeys.Contains(
                        new Tuple<object, object, object, object, object, object, object>(
                        EF.Property<object>(e, keyNames[0]),
                        EF.Property<object>(e, keyNames[1]),
                        EF.Property<object>(e, keyNames[2]),
                        EF.Property<object>(e, keyNames[3]),
                        EF.Property<object>(e, keyNames[4]),
                        EF.Property<object>(e, keyNames[5]),
                        EF.Property<object>(e, keyNames[6])));
                default:
                    throw new IndexOutOfRangeException(nameof(keys));
            }
        }

        private static IEnumerable<object> BuildKeyTuples(List<string> keyNames, List<object[]> keys)
        {
            var createMethod = typeof(Tuple).GetMethods()
                   .Where(x => x.IsGenericMethod)
                   .Where(x => x.Name == nameof(Tuple.Create))
                   .FirstOrDefault(x => x.ReturnType.IsGenericType && x.ReturnType.GenericTypeArguments.Length == keyNames.Count);

            if (createMethod == null)
            {
                throw new IndexOutOfRangeException(nameof(keyNames.Count));
            }

            var genericParams = new Type[keyNames.Count];

            for (var i = 0; i < keyNames.Count; i++)
            {
                genericParams[i] = typeof(object);
            }

            var genericMethod = createMethod.MakeGenericMethod(genericParams);

            return keys.Select(key => genericMethod.Invoke(null, key));
        }
    }
}
