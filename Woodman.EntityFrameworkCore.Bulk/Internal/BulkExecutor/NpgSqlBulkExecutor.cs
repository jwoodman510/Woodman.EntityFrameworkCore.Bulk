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

        public IQueryable<TEntity> Join(IQueryable<TEntity> queryable, IEnumerable<object[]> keys, char delimiter)
        {
            var sql = $@"
                CREATE TEMP TABLE _Keys ({string.Join(", ", PrimaryKey.Keys.Select(k => $"{k.ColumnName} {k.ColumnType}"))});

                INSERT INTO _Keys VALUES {string.Join(",", keys.Select(k => $@"
                    ({string.Join(", ", k.Select(val => StringifyKeyVal(val)))})"))};

                SELECT a.*
                FROM {TableName} a
                JOIN _Keys k ON {string.Join(@"
                    AND", PrimaryKey.Keys.Select(k => $@" a.{k.ColumnName} = k.{k.ColumnName}"))}";

            return queryable.FromSql(sql);
        }

        public async Task<int> BulkRemoveAsync(IQueryable<TEntity> queryable, bool filterKeys, List<object[]> keys)
        {
            var alias = $"d_{TableName}";
            var qryAlias = $"q_{TableName}";
            var kAlias = $"k_{TableName}";
            var keysParam = $"@keys_{TableName}";

            var qrySql = queryable.ToSql(out IReadOnlyList<NpgsqlParameter> parameters);

            var keyUsingSql = filterKeys
                ? $",_Keys as {kAlias}"
                : string.Empty;

            var keyFilterSql = filterKeys
                ? $@"AND {string.Join(@"
                    AND", PrimaryKey.Keys.Select(k => $@" {kAlias}.{k.ColumnName} = {qryAlias}.{k.ColumnName}"))}"
                : string.Empty;

            var tblSql = filterKeys ? $@"
                CREATE TEMP TABLE _Keys ({string.Join(", ", PrimaryKey.Keys.Select(k => $"{k.ColumnName} {k.ColumnType}"))});
                
                INSERT INTO _Keys VALUES {string.Join(",", keys.Select(k => $@"
                    ({string.Join(", ", k.Select(val => StringifyKeyVal(val)))})"))};"
                : string.Empty;

            var tblJoinSql = filterKeys ? $@""
                : string.Empty;

            var sqlCmd = $@"
                {tblSql}

                DELETE FROM {TableName} {alias}
                USING (
                    {qrySql}
                ) AS {qryAlias}
                {keyUsingSql}
                WHERE {string.Join(@"
                    AND", PrimaryKey.Keys.Select(k => $@" {alias}.{k.ColumnName} = {qryAlias}.{k.ColumnName}"))}
                {keyFilterSql}";

            var rowsAffected = await DbContext.Database.ExecuteSqlCommandAsync(sqlCmd, parameters.Select(p => new NpgsqlParameter(p.ParameterName, p.Value)));

            return rowsAffected - keys.Count;
        }

        public async Task BulkAddAsync(List<TEntity> entities)
        {
            var tableVar = $"_ToAdd_{typeof(TEntity).Name}";

            var props = PropertyMappings.Where(p => !p.IsGenerated).ToList();

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
                RETURNING {string.Join(",", PrimaryKey.Keys.Select(k => $@"
                    {TableName}.{k.ColumnName}"))}";

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
                ) AS {qryAlias} WHERE {string.Join(@"
                    AND", PrimaryKey.Keys.Select(k => $@" {alias}.{k.ColumnName} = {qryAlias}.{k.ColumnName}"))}";

            return await DbContext.Database.ExecuteSqlCommandAsync(sqlCmd, sqlParams);
        }

        public async Task<int> BulkUpdateAsync(IQueryable<TEntity> queryable, List<object[]> keys, List<string> updateProperties, Func<object[], TEntity> updateFunc)
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

                return $@"
                    ({string.Join(", ", key.Select(keyVal => $"{StringifyKeyVal(keyVal)}"))}, {string.Join(", ", propertyMappings.Select(m => $"{m.Value.GetDbValue(entity)}"))})";
            }));

            var sqlCmd = $@"
                CREATE TEMP TABLE {tableVar} ({string.Join(", ", PrimaryKey.Keys.Select(k => $"{k.ColumnName} {k.ColumnType}"))}, {string.Join(", ", propertyMappings.Select(m => $"{m.Value.ColumnName} {m.Value.ColumnType}"))});

                INSERT INTO {tableVar} VALUES
                    {deltaSql};

                UPDATE {TableName} AS {alias}
                SET
                    {string.Join(",", updateProperties.Select(prop => $@"
                    {propertyMappings[prop].ColumnName} = {deltaAlias}.{propertyMappings[prop].ColumnName}"))}
                FROM (
                   {qrySql}
                ) AS {qryAlias}
                JOIN {tableVar} {deltaAlias} ON {string.Join(@"
                    AND", PrimaryKey.Keys.Select(k => $@" {qryAlias}.{k.ColumnName} = {deltaAlias}.{k.ColumnName}"))}
                WHERE {string.Join(@"
                    AND", PrimaryKey.Keys.Select(k => $@" {alias}.{k.ColumnName} = {qryAlias}.{k.ColumnName}"))}";

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

            var upsertProps = PropertyMappings.Where(p => !p.IsGenerated).ToList();

            var insertColumnSql = $@"{string.Join(",", upsertProps.Select(p => $@"
                    {p.ColumnName}"))}";

            var insertSelectColumnSql = $@"{string.Join(",", upsertProps.Select(p => $@"
                    i_m.{p.ColumnName}"))}";

            var sqlCmd = $@"
                CREATE TEMP TABLE {tableVar} ({string.Join(", ", PropertyMappings.Select(m => $"{m.ColumnName} {m.ColumnType}"))});
                CREATE TEMP TABLE _m_results ({string.Join(", ", PrimaryKey.Keys.Select(k => $"{k.ColumnName} {k.ColumnType}"))}, action varchar(10)) ;

                INSERT INTO {tableVar} VALUES
                    {deltaSql};

                WITH _out AS
                (
                    DELETE FROM {TableName} d
                    USING (
                        {qrySql}
                    ) AS d_q
                    LEFT JOIN {tableVar} AS d_m ON {string.Join(@"
                        AND", PrimaryKey.Keys.Select(k => $@" d_q.{k.ColumnName} = d_m.{k.ColumnName}"))}
                    WHERE {string.Join(@"
                        AND", PrimaryKey.Keys.Select(k => $@" d_q.{k.ColumnName} = d.{k.ColumnName}"))}
                    AND {string.Join(@"
                        AND", PrimaryKey.Keys.Select(k => $@" d_m.{k.ColumnName} IS NULL"))}
                    RETURNING {string.Join(", ", PropertyMappings.Select(m => $"0 as {m.ColumnName}"))}, '{MergeActions.Delete}' AS action
                )
                INSERT INTO _m_results SELECT {string.Join(", ", PrimaryKey.Keys.Select(m => m.ColumnName))}, action FROM _out;
                
                WITH _out AS
                (
                    UPDATE {TableName} AS u
                    SET
                        {string.Join(",", upsertProps.Select(prop => $@"
                        {prop.ColumnName} = u_m.{prop.ColumnName}"))}
                    FROM (
                        {qrySql}
                    ) as u_q
                    JOIN {tableVar} u_m ON {string.Join(@"
                        AND", PrimaryKey.Keys.Select(k => $@" u_m.{k.ColumnName} = u_q.{k.ColumnName}"))}
                    WHERE {string.Join(@"
                        AND", PrimaryKey.Keys.Select(k => $@" u_q.{k.ColumnName} = u.{k.ColumnName}"))}
                    RETURNING {string.Join(", ", PropertyMappings.Select(m => $"0 as {m.ColumnName}"))}, '{MergeActions.Update}' as action
                )
                INSERT INTO _m_results SELECT {string.Join(", ", PrimaryKey.Keys.Select(m => m.ColumnName))}, action FROM _out;

                WITH _out AS
                (
                    INSERT INTO {TableName}
                    ({insertColumnSql})
                    SELECT
                    {insertSelectColumnSql}
                    FROM {tableVar} i_m
                    LEFT JOIN {TableName} q_m ON {string.Join(@"
                        AND", PrimaryKey.Keys.Select(k => $@" q_m.{k.ColumnName} = i_m.{k.ColumnName}"))}
                    WHERE {string.Join(@"
                        AND", PrimaryKey.Keys.Select(k => $@" q_m.{k.ColumnName} IS NULL"))}
                    RETURNING {string.Join(", ", PropertyMappings.Select(m => $"{TableName}.{m.ColumnName} as {m.ColumnName}"))}, '{MergeActions.Insert}' as action
                )
                INSERT INTO _m_results SELECT {string.Join(", ", PrimaryKey.Keys.Select(m => m.ColumnName))}, action FROM _out;

                SELECT action, {string.Join(", ", PrimaryKey.Keys.Select(m => m.ColumnName))} FROM _m_results;";

            return await ExecuteSqlCommandAsync(sqlCmd, current, parameters.Select(p => new NpgsqlParameter(p.ParameterName, p.Value)));
        }
    }
}
