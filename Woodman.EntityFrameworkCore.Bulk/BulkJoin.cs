using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using Woodman.EntityFrameworkCore.Bulk.EntityInfo;
using Woodman.EntityFrameworkCore.Bulk.Extensions;

namespace Microsoft.EntityFrameworkCore
{
    public static class BulkJoin
    {
        public static IQueryable<TEntity> Join<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<int> keys)
            where TEntity : class
        {
            return queryable.Join(keys.Select(k => k.ToString()));
        }

        public static IQueryable<TEntity> Join<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<long> keys)
            where TEntity : class
        {
            return queryable.Join(keys.Select(k => k.ToString()));
        }

        public static IQueryable<TEntity> Join<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<string> keys, char delimiter = ',')
            where TEntity : class
        {
            var toFind = keys?.ToList() ?? new List<string>();

            if (toFind == null || toFind.Count == 0)
            {
                return queryable.Where(e => false);
            }

            var dbContext = queryable.GetDbContext();
            var entityInfo = dbContext.GetEntityInfo<TEntity>();

            if (entityInfo is InMemEntityInfo inMemEntityInfo)
            {
                return queryable.Join(keys, inMemEntityInfo);
            }
            else if (entityInfo is NpgSqlEntityInfo npgSqlEntityInfo)
            {
                return queryable.Join(keys, delimiter, npgSqlEntityInfo);
            }
            else if (entityInfo is SqlEntityInfo sqlEntityInfo)
            {
                return queryable.Join(keys, delimiter, sqlEntityInfo);
            }
            else
            {
                throw new Exception($"Unsupported {nameof(dbContext.Database.ProviderName)} {dbContext.Database.ProviderName}.");
            }
        }

        private static IQueryable<TEntity> Join<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<string> keys, InMemEntityInfo entityInfo)
            where TEntity : class
        {
            var primKeys = keys.ToDictionary(k => k);

            return queryable.Where(entity => primKeys.ContainsKey(entityInfo.GetPrimaryKey(entity).ToString()));
        }

        private static IQueryable<TEntity> Join<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<string> keys, char delimiter, SqlEntityInfo entityInfo)
            where TEntity : class
        {
            var sql = $@"
                SELECT a.* FROM dbo.{entityInfo.TableName} a
                JOIN dbo.Split({"{0}"}, '{delimiter}') as b ON a.{entityInfo.PrimaryKeyColumnName} = b.[Data]";

            var escapedKeys = keys.Select(k => k.Replace("'", "''"));

            return queryable.FromSql(sql, string.Join($"{delimiter}", escapedKeys));
        }

        private static IQueryable<TEntity> Join<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<string> keys, char delimiter, NpgSqlEntityInfo entityInfo)
            where TEntity : class
        {
            var keyType = entityInfo.PrimaryKeyColumnType;

            var sql = $@"
                SELECT a.* FROM {entityInfo.TableName} a
                JOIN (select regexp_split_to_table({"{0}"}, '{delimiter}') as id) as b ON cast(b.id as {keyType}) = a.{entityInfo.PrimaryKeyColumnName}";

            var escapedKeys = keys.Select(k => k.Replace("'", "''"));

            return queryable.FromSql(sql, string.Join($"{delimiter}", escapedKeys));
        }
    }
}
