using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.EntityFrameworkCore
{
    internal class NpgSqlBulkExecutor<TEntity> : RelationalBulkExecutor<TEntity>, IBulkExecutor<TEntity> where TEntity : class
    {
        public NpgSqlBulkExecutor(DbContext dbContext) : base(dbContext) { }

        public IQueryable<TEntity> Join(IQueryable<TEntity> queryable, IEnumerable<string> keys, char delimiter)
        {
            var keyType = PrimaryKeyColumnType;

            var sql = $@"
                SELECT a.* FROM {TableName} a
                JOIN (select regexp_split_to_table({"{0}"}, '{delimiter}') as id) as b ON cast(b.id as {keyType}) = a.{PrimaryKeyColumnName}";

            var escapedKeys = keys.Select(k => k.Replace("'", "''"));

            return queryable.FromSql(sql, string.Join($"{delimiter}", escapedKeys));
        }

        public async Task<int> BulkRemoveAsync<TKey>(IQueryable<TEntity> queryable, bool filterKeys, List<TKey> keys)
        {
            var alias = $"d_{TableName}";
            var qryAlias = $"q_{TableName}";
            var kAlias = $"k_{TableName}";
            var keysParam = $"@keys_{TableName}";

            const string delimiter = ",";

            var qrySql = queryable.ToSql(out IReadOnlyList<NpgsqlParameter> parameters);

            var keyUsingSql = filterKeys
                ? $",(select regexp_split_to_table({keysParam}, '{delimiter}') as id) as {kAlias}"
                : string.Empty;

            var keyFilterSql = filterKeys
                ? $"AND cast({kAlias}.id as {PrimaryKeyColumnType}) = {alias}.{PrimaryKeyColumnName}"
                : string.Empty;

            var sqlCmd = $@"
                DELETE FROM {TableName} {alias}
                USING (
                    {qrySql}
                ) AS {qryAlias}
                {keyUsingSql}
                WHERE {qryAlias}.{PrimaryKeyColumnName} = {alias}.{PrimaryKeyColumnName}
                {keyFilterSql}";

            var sqlParams = new List<NpgsqlParameter>();

            if (filterKeys)
            {
                var escapedKeys = string.Join(delimiter, keys.Select(k => k.ToString().Replace("'", "''")));

                sqlParams.Add(new NpgsqlParameter(keysParam, escapedKeys));
            }

            sqlParams.AddRange(parameters.Select(p => new NpgsqlParameter(p.ParameterName, p.Value)));

            return await DbContext.Database.ExecuteSqlCommandAsync(sqlCmd, sqlParams);
        }

        public async Task BulkAddAsync(List<TEntity> entities)
        {
            var tableVar = $"_ToAdd_{typeof(TEntity).Name}";

            var props = PropertyMappings
                .Where(p => IsPrimaryKeyGenerated ? !p.IsPrimaryKey : true)
                .ToList();

            var columnsSql = $@"{string.Join(",", props.Select(p => $@"
                    {p.ColumnName}"))}";

            var deltaSql = string.Join(",", entities.Select(entity =>
            {
                return $@"
                    ({string.Join(", ", props.Select(m => $"{m.GetDbValue(entity)}"))})";
            }));

            var sqlCmd = $@"
                CREATE TEMP TABLE {tableVar} ({string.Join(", ", props.Select(m => $"{m.ColumnName} {m.ColumnType}"))});

                INSERT INTO {tableVar} VALUES
                    {deltaSql};

                INSERT INTO {TableName}
                ({columnsSql})
                SELECT
                {columnsSql}
                FROM {tableVar}
                RETURNING {TableName}.id";

            await ExecuteSqlCommandAsync(sqlCmd, entities);
        }

        public async Task<int> BulkUpdateAsync(IQueryable<TEntity> queryable, TEntity updatedEntity, List<string> updateProperties)
        {
            var alias = $"u_{TableName}";
            var qryAlias = $"q_{TableName}";

            var qrySql = queryable.ToSql(out IReadOnlyList<NpgsqlParameter> parameters);

            var propertyMappings = PropertyMappings
                .Where(p => updateProperties.Contains(p.PropertyName))
                .Select(p => new
                {
                    PropertyMapping = p,
                    SqlParam = new NpgsqlParameter($"@p_{TableName}_{p.PropertyName}", p.GetPropertyValue(updatedEntity) ?? DBNull.Value)
                }).ToList();

            var sqlParams = new List<NpgsqlParameter>();

            sqlParams.AddRange(parameters.Select(p => new NpgsqlParameter(p.ParameterName, p.Value)));
            sqlParams.AddRange(propertyMappings.Select(p => p.SqlParam));

            var columnSql = string.Join(",", propertyMappings.Select(prop => $@"
                    {prop.PropertyMapping.ColumnName} = {prop.SqlParam.ParameterName}"));

            var sqlCmd = $@"
                UPDATE {TableName} AS {alias}
                SET {columnSql}
                FROM (
                   {qrySql}
                ) AS {qryAlias} WHERE {qryAlias}.{PrimaryKeyColumnName} = {alias}.{PrimaryKeyColumnName}";

            return await DbContext.Database.ExecuteSqlCommandAsync(sqlCmd, sqlParams);
        }

        public async Task<int> BulkUpdateAsync<TKey>(IQueryable<TEntity> queryable, List<TKey> keys, List<string> updateProperties, Func<TKey, TEntity> updateFunc)
        {
            var alias = $"u_{TableName}";
            var qryAlias = $"q_{TableName}";
            var deltaAlias = $"d_{TableName}";
            var tableVar = $"_delta_{typeof(TEntity).Name}";

            var qrySql = queryable.ToSql(out IReadOnlyList<NpgsqlParameter> parameters);

            var propertyMappings = PropertyMappings
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
                CREATE TEMP TABLE {tableVar} ({PrimaryKeyColumnName} {PrimaryKeyColumnType}, {string.Join(", ", propertyMappings.Select(m => $"{m.Value.ColumnName} {m.Value.ColumnType}"))});

                INSERT INTO {tableVar} VALUES
                    {deltaSql};

                UPDATE {TableName} AS {alias}
                SET
                    {string.Join(",", updateProperties.Select(prop => $@"
                    {propertyMappings[prop].ColumnName} = {deltaAlias}.{propertyMappings[prop].ColumnName}"))}
                FROM (
                   {qrySql}
                ) AS {qryAlias}
                JOIN {tableVar} {deltaAlias} ON {deltaAlias}.{PrimaryKeyColumnName} = {qryAlias}.{PrimaryKeyColumnName}
                WHERE {qryAlias}.{PrimaryKeyColumnName} = {alias}.{PrimaryKeyColumnName}";

            var count = await DbContext.Database.ExecuteSqlCommandAsync(sqlCmd, parameters.Select(p => new NpgsqlParameter(p.ParameterName, p.Value)));

            return count - keyList.Count;
        }

        public async Task<int> BulkMergeAsync(IQueryable<TEntity> queryable, List<TEntity> current)
        {
            var tableVar = $"_m_{typeof(TEntity).Name}";

            var deltaSql = string.Join(",", current.Select(entity =>
            {
                return $@"
                    ({string.Join(", ", PropertyMappings.Select(m => $"{m.GetDbValue(entity)}"))})";
            }));

            var qrySql = queryable.ToSql(out IReadOnlyList<NpgsqlParameter> parameters);

            var insertProps = PropertyMappings
                .Where(p => IsPrimaryKeyGenerated ? !p.IsPrimaryKey : true)
                .ToList();

            var updateProperties = PropertyMappings.Where(p => IsPrimaryKeyGenerated ? !p.IsPrimaryKey : true);

            var insertColumnSql = $@"{string.Join(",", insertProps.Select(p => $@"
                    {p.ColumnName}"))}";

            var insertSelectColumnSql = $@"{string.Join(",", insertProps.Select(p => $@"
                    i_m.{p.ColumnName}"))}";

            var sqlCmd = $@"
                CREATE TEMP TABLE {tableVar} ({string.Join(", ", PropertyMappings.Select(m => $"{m.ColumnName} {m.ColumnType}"))});
                CREATE TEMP TABLE _m_results ({PrimaryKeyColumnName} {PrimaryKeyColumnType}, action varchar(10)) ;

                INSERT INTO {tableVar} VALUES
                    {deltaSql};

                WITH _out AS
                (
                    DELETE FROM {TableName} d
                    USING (
                        {qrySql}
                    ) AS d_q
                    LEFT JOIN {tableVar} AS d_m ON d_q.{PrimaryKeyColumnName} = d_m.{PrimaryKeyColumnName}
                    WHERE d_q.{PrimaryKeyColumnName} = d.{PrimaryKeyColumnName}
                    AND d_m.{PrimaryKeyColumnName} IS NULL
                    RETURNING 0 AS id, '{MergeActions.Delete}' AS action
                )
                INSERT INTO _m_results SELECT id, action FROM _out;
                
                WITH _out AS
                (
                    UPDATE {TableName} AS u
                    SET
                        {string.Join(",", updateProperties.Select(prop => $@"
                        {prop.ColumnName} = u_m.{prop.ColumnName}"))}
                    FROM (
                        {qrySql}
                    ) as u_q
                    JOIN {tableVar} u_m ON u_m.{PrimaryKeyColumnName} = u_q.{PrimaryKeyColumnName}
                    WHERE u_q.{PrimaryKeyColumnName} = u.{PrimaryKeyColumnName}
                    RETURNING 0 as id, '{MergeActions.Update}' as action
                )
                INSERT INTO _m_results SELECT id, action FROM _out;

                WITH _out AS
                (
                    INSERT INTO {TableName}
                    ({insertColumnSql})
                    SELECT
                    {insertSelectColumnSql}
                    FROM {tableVar} i_m
                    LEFT JOIN {TableName} q_m ON q_m.{PrimaryKeyColumnName} = i_m.{PrimaryKeyColumnName}
                    WHERE q_m.{PrimaryKeyColumnName} IS NULL
                    RETURNING {TableName}.id as id, '{MergeActions.Insert}' as action
                )
                INSERT INTO _m_results SELECT id, action FROM _out;

                SELECT id, action FROM _m_results;";

            return await ExecuteSqlCommandAsync(sqlCmd, current, parameters.Select(p => new NpgsqlParameter(p.ParameterName, p.Value)));
        }
    }
}
