using Microsoft.EntityFrameworkCore;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Woodman.EntityFrameworkCore.Bulk.EntityInfo;
using Woodman.EntityFrameworkCore.Bulk.Extensions;

namespace Microsoft.EntityFrameworkCore
{
    public static class BulkRemove
    {
        public static async Task<int> BulkRemoveAsync<TEntity>(this IQueryable<TEntity> queryable)
        where TEntity : class, new()
        {
            return await queryable.BulkRemoveAsync(new string[0], false);
        }

        public static async Task<int> BulkRemoveAsync<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<int> keys)
        where TEntity : class, new()
        {
            return await queryable.BulkRemoveAsync(keys, true);
        }

        public static async Task<int> BulkRemoveAsync<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<long> keys)
            where TEntity : class, new()
        {
            return await queryable.BulkRemoveAsync(keys, true);
        }

        public static async Task<int> BulkRemoveAsync<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<string> keys)
        where TEntity : class, new()
        {
            return await queryable.BulkRemoveAsync(keys, true);
        }

        private static async Task<int> BulkRemoveAsync<TKey, TEntity>(this IQueryable<TEntity> queryable, IEnumerable<TKey> keys, bool hasKeys)
        where TEntity : class, new()
        {
            var toRemove = keys?.ToList() ?? new List<TKey>();

            if (toRemove == null || toRemove.Count == 0 && hasKeys)
            {
                return 0;
            }

            var dbContext = queryable.GetDbContext();
            var entityInfo = dbContext.GetEntityInfo<TEntity>();

            if (entityInfo is InMemEntityInfo inMemEntityInfo)
            {
                return queryable.BulkRemoveAsync(hasKeys, toRemove, dbContext, inMemEntityInfo);
            }
            else if (entityInfo is NpgSqlEntityInfo npgSqlEntityInfo)
            {
                return await queryable.BulkRemoveAsync(keys, hasKeys, dbContext, npgSqlEntityInfo);
            }
            else if (entityInfo is SqlEntityInfo sqlEntityInfo)
            {
                return await queryable.BulkRemoveSqlAsync(keys, hasKeys, dbContext, sqlEntityInfo);
            }
            else
            {
                throw new Exception($"Unsupported {nameof(dbContext.Database.ProviderName)} {dbContext.Database.ProviderName}.");
            }
        }

        private static int BulkRemoveAsync<TKey, TEntity>(this IQueryable<TEntity> queryable, bool hasKeys, List<TKey> toRemove, DbContext dbContext, InMemEntityInfo entityInfo)
            where TEntity : class, new()
        {
            var primKeys = toRemove.ToDictionary(k => k.ToString());

            var entities = hasKeys
                ? queryable
                    .Where(entity => primKeys.ContainsKey(entityInfo.GetPrimaryKey(entity).ToString()))
                    .ToList()
                : queryable.ToList();

            foreach (var entity in entities)
            {
                dbContext.Remove(entity);
            }

            if (entities.Count > 0)
            {
                dbContext.SaveChanges();
            }

            return entities.Count;
        }

        private static async Task<int> BulkRemoveAsync<TKey, TEntity>(this IQueryable<TEntity> queryable, IEnumerable<TKey> keys, bool hasKeys, DbContext dbContext, NpgSqlEntityInfo entityInfo)
            where TEntity : class, new()
        {
            var alias = $"d_{entityInfo.TableName}";
            var qryAlias = $"q_{entityInfo.TableName}";
            var kAlias = $"k_{entityInfo.TableName}";
            var keysParam = $"@keys_{entityInfo.TableName}";

            const string delimiter = ",";

            var qrySql = queryable.ToSql(out IReadOnlyList<NpgsqlParameter> parameters);

            var keyUsingSql = hasKeys
                ? $",(select regexp_split_to_table({keysParam}, '{delimiter}') as id) as {kAlias}"
                : string.Empty;

            var keyFilterSql = hasKeys
                ? $"AND cast({kAlias}.id as {entityInfo.PrimaryKeyColumnType}) = {alias}.{entityInfo.PrimaryKeyColumnName}"
                : string.Empty;

            var sqlCmd = $@"
                DELETE FROM {entityInfo.TableName} {alias}
                USING (
                    {qrySql}
                ) AS {qryAlias}
                {keyUsingSql}
                WHERE {qryAlias}.{entityInfo.PrimaryKeyColumnName} = {alias}.{entityInfo.PrimaryKeyColumnName}
                {keyFilterSql}";

            var sqlParams = new List<NpgsqlParameter>();

            if (hasKeys)
            {
                var escapedKeys = string.Join(delimiter, keys.Select(k => k.ToString().Replace("'", "''")));

                sqlParams.Add(new NpgsqlParameter(keysParam, escapedKeys));
            }

            sqlParams.AddRange(parameters.Select(p => new NpgsqlParameter(p.ParameterName, p.Value)));

            return await dbContext.Database.ExecuteSqlCommandAsync(sqlCmd, sqlParams);
        }

        private static async Task<int> BulkRemoveSqlAsync<TKey, TEntity>(this IQueryable<TEntity> queryable, IEnumerable<TKey> keys, bool hasKeys, DbContext dbContext, SqlEntityInfo entityInfo)
            where TEntity : class, new()
        {
            var alias = $"d_{entityInfo.TableName}";
            var qryAlias = $"q_{entityInfo.TableName}";
            var kAlias = $"k_{entityInfo.TableName}";
            var keysParam = $"@keys_{entityInfo.TableName}";

            const string delimiter = ",";

            var qrySql = queryable.ToSql(out IReadOnlyList<SqlParameter> parameters);

            var keySql = hasKeys
                ? $"JOIN dbo.Split({keysParam}, '{delimiter}') as {kAlias} ON {kAlias}.[Data] = {alias}.{entityInfo.PrimaryKeyColumnName}"
                : string.Empty;

            var sqlCmd = $@"
                DELETE {alias} FROM {entityInfo.TableName} {alias}
                JOIN (
                    {qrySql}
                ) AS {qryAlias} ON {qryAlias}.{entityInfo.PrimaryKeyColumnName} = {alias}.{entityInfo.PrimaryKeyColumnName}
                {keySql}";

            var sqlParams = new List<SqlParameter>();

            if (hasKeys)
            {
                var escapedKeys = string.Join(delimiter, keys.Select(k => k.ToString().Replace("'", "''")));

                sqlParams.Add(new SqlParameter(keysParam, escapedKeys));
            }

            sqlParams.AddRange(parameters.Select(p => new SqlParameter(p.ParameterName, p.Value)));

            return await dbContext.Database.ExecuteSqlCommandAsync(sqlCmd, sqlParams);
        }
    }
}
