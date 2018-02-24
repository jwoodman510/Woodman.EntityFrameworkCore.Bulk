using Microsoft.EntityFrameworkCore;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Woodman.EntityFrameworkCore.Bulk.EntityInfo;
using Woodman.EntityFrameworkCore.Bulk.Extensions;

namespace Microsoft.EntityFrameworkCore
{
    public static class BulkUpdate
    {
        public static async Task<int> BulkUpdateAsync<TEntity>(this IQueryable<TEntity> queryable, Expression<Func<TEntity>> updateFactory)
            where TEntity : class, new()
        {
            var memberInitExpression = EnsureMemberInitExpression(updateFactory);
            var updateFunc = updateFactory.Compile();
            var updatedEntity = updateFunc();

            var updateProperties = memberInitExpression.Bindings.Select(b => b.Member.Name).ToList();

            if (updateProperties.Count == 0)
            {
                return 0;
            }

            var dbContext = queryable.GetDbContext();
            var entityInfo = dbContext.GetEntityInfo<TEntity>();

            if (entityInfo is InMemEntityInfo inMemEntityInfo)
            {
                return queryable.BulkUpdateAsync(updatedEntity, updateProperties, dbContext);
            }
            else if (entityInfo is NpgSqlEntityInfo npgSqlEntityInfo)
            {
                return await queryable.BulkUpdateAsync(updatedEntity, updateProperties, dbContext, npgSqlEntityInfo);
            }
            else if (entityInfo is SqlEntityInfo sqlEntityInfo)
            {
                return await queryable.BulkUpdateAsync(updatedEntity, updateProperties, dbContext, sqlEntityInfo);
            }
            else
            {
                throw new Exception($"Unsupported {nameof(dbContext.Database.ProviderName)} {dbContext.Database.ProviderName}.");
            }
        }

        public static async Task<int> BulkUpdateAsync<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<int> keys, Expression<Func<int, TEntity>> updateFactory)
            where TEntity : class, new()
        {
            return await queryable.BulkUpdateAsync<int, TEntity>(keys, updateFactory);
        }

        public static async Task<int> BulkUpdateAsync<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<long> keys, Expression<Func<long, TEntity>> updateFactory)
            where TEntity : class, new()
        {
            return await queryable.BulkUpdateAsync<long, TEntity>(keys, updateFactory);
        }

        public static async Task<int> BulkUpdateAsync<TEntity>(this IQueryable<TEntity> queryable, IEnumerable<string> keys, Expression<Func<string, TEntity>> updateFactory)
        where TEntity : class, new()
        {
            return await queryable.BulkUpdateAsync<string, TEntity>(keys, updateFactory);
        }

        private static async Task<int> BulkUpdateAsync<TKey, TEntity>(this IQueryable<TEntity> queryable, IEnumerable<TKey> keys, Expression<Func<TKey, TEntity>> updateFactory)
        where TEntity : class, new()
        {
            var toUpdate = keys?.ToList() ?? new List<TKey>();

            if (toUpdate == null || toUpdate.Count == 0)
            {
                return 0;
            }

            var memberInitExpression = EnsureMemberInitExpression(updateFactory);

            var updateProperties = memberInitExpression.Bindings
                .Select(b => b.Member.Name)
                .ToList();

            if (updateProperties.Count == 0)
            {
                return 0;
            }

            var updateFunc = updateFactory.Compile();

            var dbContext = queryable.GetDbContext();
            var entityInfo = dbContext.GetEntityInfo<TEntity>();

            if (entityInfo is InMemEntityInfo inMemEntityInfo)
            {
                return queryable.BulkUpdateAsync(toUpdate, updateProperties, updateFunc, dbContext, inMemEntityInfo);
            }
            else if (entityInfo is NpgSqlEntityInfo npgSqlEntityInfo)
            {
                return await queryable.BulkUpdateAsync(keys, updateProperties, updateFunc, dbContext, npgSqlEntityInfo);
            }
            else if (entityInfo is SqlEntityInfo sqlEntityInfo)
            {
                return await queryable.BulkUpdateAsync(keys, updateProperties, updateFunc, dbContext, sqlEntityInfo);
            }
            else
            {
                throw new Exception($"Unsupported {nameof(dbContext.Database.ProviderName)} {dbContext.Database.ProviderName}.");
            }
        }

        private static async Task<int> BulkUpdateAsync<TEntity>(this IQueryable<TEntity> queryable, TEntity updatedEntity, List<string> updateProperties, DbContext dbContext, SqlEntityInfo entityInfo)
            where TEntity : class, new()
        {
            var alias = $"u_{entityInfo.TableName}";
            var qryAlias = $"q_{entityInfo.TableName}";

            var qrySql = queryable.ToSql(out IReadOnlyList<SqlParameter> parameters);

            var propertyMappings = entityInfo.PropertyMappings
                .Where(p => updateProperties.Contains(p.PropertyName))
                .Select(p => new
                {
                    PropertyMapping = p,
                    SqlParam = new SqlParameter($"@{entityInfo.TableName}_{p.PropertyName}", p.GetPropertyValue(updatedEntity) ?? DBNull.Value)
                }).ToList();

            var sqlParams = new List<SqlParameter>();

            sqlParams.AddRange(parameters.Select(p => new SqlParameter(p.ParameterName, p.Value)));
            sqlParams.AddRange(propertyMappings.Select(p => p.SqlParam));

            var columnSql = string.Join(",", propertyMappings.Select(prop => $@"
                    {alias}.{prop.PropertyMapping.ColumnName} = {prop.SqlParam.ParameterName}"));

            var sqlCmd = $@"
                UPDATE {alias}
                SET {columnSql}
                FROM dbo.{entityInfo.TableName} {alias}
                JOIN (
                   {qrySql}
                ) AS {qryAlias} ON {qryAlias}.{entityInfo.PrimaryKeyColumnName} = {alias}.{entityInfo.PrimaryKeyColumnName}";

            return await dbContext.Database.ExecuteSqlCommandAsync(sqlCmd, sqlParams);
        }

        private static async Task<int> BulkUpdateAsync<TEntity>(this IQueryable<TEntity> queryable, TEntity updatedEntity, List<string> updateProperties, DbContext dbContext, NpgSqlEntityInfo entityInfo)
            where TEntity : class, new()
        {
            var alias = $"u_{entityInfo.TableName}";
            var qryAlias = $"q_{entityInfo.TableName}";

            var qrySql = queryable.ToSql(out IReadOnlyList<NpgsqlParameter> parameters);

            var propertyMappings = entityInfo.PropertyMappings
                .Where(p => updateProperties.Contains(p.PropertyName))
                .Select(p => new
                {
                    PropertyMapping = p,
                    SqlParam = new NpgsqlParameter($"@p_{entityInfo.TableName}_{p.PropertyName}", p.GetPropertyValue(updatedEntity) ?? DBNull.Value)
                }).ToList();

            var sqlParams = new List<NpgsqlParameter>();

            sqlParams.AddRange(parameters.Select(p => new NpgsqlParameter(p.ParameterName, p.Value)));
            sqlParams.AddRange(propertyMappings.Select(p => p.SqlParam));

            var columnSql = string.Join(",", propertyMappings.Select(prop => $@"
                    {prop.PropertyMapping.ColumnName} = {prop.SqlParam.ParameterName}"));

            var sqlCmd = $@"
                UPDATE {entityInfo.TableName} AS {alias}
                SET {columnSql}
                FROM (
                   {qrySql}
                ) AS {qryAlias} WHERE {qryAlias}.{entityInfo.PrimaryKeyColumnName} = {alias}.{entityInfo.PrimaryKeyColumnName}";

            return await dbContext.Database.ExecuteSqlCommandAsync(sqlCmd, sqlParams);
        }

        private static int BulkUpdateAsync<TEntity>(this IQueryable<TEntity> queryable, TEntity updatedEntity, List<string> updateProperties, DbContext dbContext) where TEntity : class, new()
        {
            var updatePropDict = updateProperties.ToDictionary(key => key, value => typeof(TEntity).GetProperty(value));

            var entities = queryable.ToList();

            foreach (var entity in entities)
            {
                foreach (var updateProp in updatePropDict)
                {
                    var propInfo = updatePropDict[updateProp.Key];

                    propInfo.SetValue(entity, propInfo.GetValue(updatedEntity));
                }
            }

            if (entities.Count > 0)
            {
                dbContext.SaveChanges();
            }

            return entities.Count;
        }

        private static async Task<int> BulkUpdateAsync<TKey, TEntity>(this IQueryable<TEntity> queryable, IEnumerable<TKey> keys, List<string> updateProperties, Func<TKey, TEntity> updateFunc, DbContext dbContext, SqlEntityInfo entityInfo)
            where TEntity : class, new()
        {
            var alias = $"u_{entityInfo.TableName}";
            var qryAlias = $"q_{entityInfo.TableName}";
            var deltaAlias = $"d_{entityInfo.TableName}";
            var tableVar = "@Delta";

            var qrySql = queryable.ToSql(out IReadOnlyList<SqlParameter> parameters);

            var propertyMappings = entityInfo.PropertyMappings
                .Where(p => updateProperties.Contains(p.PropertyName))
                .ToDictionary(x => x.PropertyName);

            var deltaSql = string.Join(",", keys.Select(key =>
            {
                var entity = updateFunc(key);
                var keyVal = key.ToString().Replace("'", "''");

                return $@"
                    ('{keyVal}', {string.Join(", ", propertyMappings.Select(m => $"{m.Value.GetDbValue(entity)}"))})";
            }));

            var sqlCmd = $@"
                DECLARE {tableVar} TABLE ({entityInfo.PrimaryKeyColumnName} {entityInfo.PrimaryKeyColumnType}, {string.Join(", ", propertyMappings.Select(m => $"{m.Value.ColumnName} {m.Value.ColumnType}"))})

                SET NOCOUNT ON

                INSERT INTO {tableVar} VALUES
                    {deltaSql}

                SET NOCOUNT OFF

                UPDATE {alias}
                SET
                    {string.Join(",", updateProperties.Select(prop => $@"
                    {alias}.{propertyMappings[prop].ColumnName} = {deltaAlias}.{propertyMappings[prop].ColumnName}"))}
                FROM dbo.{entityInfo.TableName} {alias}
                JOIN (
                   {qrySql}
                ) AS {qryAlias} ON {qryAlias}.{entityInfo.PrimaryKeyColumnName} = {alias}.{entityInfo.PrimaryKeyColumnName}
                JOIN {tableVar} {deltaAlias} ON {deltaAlias}.{entityInfo.PrimaryKeyColumnName} = {alias}.{entityInfo.PrimaryKeyColumnName}";

            return await dbContext.Database.ExecuteSqlCommandAsync(sqlCmd, parameters.Select(p => new SqlParameter(p.ParameterName, p.Value)));
        }

        private static async Task<int> BulkUpdateAsync<TKey, TEntity>(this IQueryable<TEntity> queryable, IEnumerable<TKey> keys, List<string> updateProperties, Func<TKey, TEntity> updateFunc, DbContext dbContext, NpgSqlEntityInfo entityInfo)
            where TEntity : class, new()
        {
            var alias = $"u_{entityInfo.TableName}";
            var qryAlias = $"q_{entityInfo.TableName}";
            var deltaAlias = $"d_{entityInfo.TableName}";
            var tableVar = $"_delta_{typeof(TEntity).Name}";

            var qrySql = queryable.ToSql(out IReadOnlyList<NpgsqlParameter> parameters);

            var propertyMappings = entityInfo.PropertyMappings
                .Where(p => updateProperties.Contains(p.PropertyName))
                .ToDictionary(x => x.PropertyName);

            var keyList = keys.ToList();

            var deltaSql = string.Join(",", keyList.Select(key =>
            {
                var entity = updateFunc(key);
                var keyVal = key.ToString().Replace("'", "''");

                return $@"
                    ('{keyVal}', {string.Join(", ", propertyMappings.Select(m => $"{m.Value.GetDbValue(entity)}"))})";
            }));

            var sqlCmd = $@"
                CREATE TEMP TABLE {tableVar} ({entityInfo.PrimaryKeyColumnName} {entityInfo.PrimaryKeyColumnType}, {string.Join(", ", propertyMappings.Select(m => $"{m.Value.ColumnName} {m.Value.ColumnType}"))});

                INSERT INTO {tableVar} VALUES
                    {deltaSql};

                UPDATE {entityInfo.TableName} AS {alias}
                SET
                    {string.Join(",", updateProperties.Select(prop => $@"
                    {propertyMappings[prop].ColumnName} = {deltaAlias}.{propertyMappings[prop].ColumnName}"))}
                FROM (
                   {qrySql}
                ) AS {qryAlias}
                JOIN {tableVar} {deltaAlias} ON {deltaAlias}.{entityInfo.PrimaryKeyColumnName} = {qryAlias}.{entityInfo.PrimaryKeyColumnName}
                WHERE {qryAlias}.{entityInfo.PrimaryKeyColumnName} = {alias}.{entityInfo.PrimaryKeyColumnName}";

            var count = await dbContext.Database.ExecuteSqlCommandAsync(sqlCmd, parameters.Select(p => new NpgsqlParameter(p.ParameterName, p.Value)));

            return count - keyList.Count;
        }

        private static int BulkUpdateAsync<TKey, TEntity>(this IQueryable<TEntity> queryable, List<TKey> toUpdate, List<string> updateProperties, Func<TKey, TEntity> updateFunc, DbContext dbContext, InMemEntityInfo entityInfo)
            where TEntity : class, new()
        {
            var primKeys = toUpdate.ToDictionary(k => k.ToString());

            var entities = queryable.Where(entity => primKeys.ContainsKey(entityInfo.GetPrimaryKey(entity).ToString()))
                .ToList();

            var updatePropDict = updateProperties.ToDictionary(key => key, value => typeof(TEntity).GetProperty(value));

            foreach (var entity in entities)
            {
                var key = primKeys[entityInfo.GetPrimaryKey(entity).ToString()];
                var updatedEntity = updateFunc(key);

                foreach (var updateProp in updatePropDict)
                {
                    var propInfo = updatePropDict[updateProp.Key];

                    propInfo.SetValue(entity, propInfo.GetValue(updatedEntity));
                }
            }

            if (entities.Count > 0)
            {
                dbContext.SaveChanges();
            }

            return entities.Count;
        }

        private static MemberInitExpression EnsureMemberInitExpression<TEntity>(Expression<Func<TEntity>> updateFactory) where TEntity : class
        {
            return EnsureMemberInitExpression(updateFactory.Body, nameof(updateFactory));
        }

        private static MemberInitExpression EnsureMemberInitExpression<T, TEntity>(Expression<Func<T, TEntity>> updateFactory) where TEntity : class
        {
            return EnsureMemberInitExpression(updateFactory.Body, nameof(updateFactory));
        }

        private static MemberInitExpression EnsureMemberInitExpression(Expression updateExpressionBody, string updateExpressionName)
        {
            while (updateExpressionBody.NodeType == ExpressionType.Convert || updateExpressionBody.NodeType == ExpressionType.ConvertChecked)
            {
                updateExpressionBody = ((UnaryExpression)updateExpressionBody).Operand;
            }
            var memberInitExpression = updateExpressionBody as MemberInitExpression;

            if (memberInitExpression == null)
            {
                throw new Exception($"{updateExpressionName} must be of type {nameof(MemberInitExpression)}.");
            }

            return memberInitExpression;
        }
    }
}
