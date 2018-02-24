using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Woodman.EntityFrameworkCore.Bulk.EntityInfo;
using Woodman.EntityFrameworkCore.Bulk.Extensions;

namespace Microsoft.EntityFrameworkCore
{
    public static class BulkAdd
    {
        public static async Task<object[]> BulkAddAsync<TEntity>(this DbSet<TEntity> queryable, IEnumerable<TEntity> entities)
            where TEntity : class, new()
        {
            var toAdd = entities?.ToList() ?? new List<TEntity>();
            var ids = new object[toAdd.Count];

            if (toAdd.Count == 0)
            {
                return ids;
            }

            var dbContext = queryable.GetDbContext();
            var entityInfo = dbContext.GetEntityInfo<TEntity>();

            if (entityInfo is InMemEntityInfo inMemEntityInfo)
            {
                return await BulkAddAsync(entities, ids, dbContext, inMemEntityInfo);
            }
            else if (entityInfo is NpgSqlEntityInfo npgSqlEntityInfo)
            {
                return await BulkAddAsync(toAdd, ids, dbContext, npgSqlEntityInfo);
            }
            else if (entityInfo is SqlEntityInfo sqlEntityInfo)
            {
                return await BulkAddAsync(toAdd, ids, dbContext, sqlEntityInfo);
            }
            else
            {
                throw new Exception($"Unsupported {nameof(dbContext.Database.ProviderName)} {dbContext.Database.ProviderName}.");
            }
        }

        private static async Task<object[]> BulkAddAsync<TEntity>(IEnumerable<TEntity> entities, object[] ids, DbContext dbContext, InMemEntityInfo entityInfo)
            where TEntity : class, new()
        {
            var i = 0;
            foreach (var entity in entities)
            {
                var added = await dbContext.AddAsync(entity);

                ids[i] = entityInfo.GetPrimaryKey(entity);

                i++;
            }

            await dbContext.SaveChangesAsync();

            return ids;
        }

        private static async Task<object[]> BulkAddAsync<TEntity>(List<TEntity> toAdd, object[] ids, DbContext dbContext, NpgSqlEntityInfo entityInfo)
            where TEntity : class, new()
        {
            var tableVar = $"_ToAdd_{typeof(TEntity).Name}";

            var props = entityInfo.PropertyMappings
                .Where(p => entityInfo.IsPrimaryKeyGenerated ? !p.IsPrimaryKey : true)
                .ToList();

            var columnsSql = $@"{string.Join(",", props.Select(p => $@"
                    {p.ColumnName}"))}";

            var deltaSql = string.Join(",", toAdd.Select(entity =>
            {
                return $@"
                    ({string.Join(", ", props.Select(m => $"{m.GetDbValue(entity)}"))})";
            }));

            var sqlCmd = $@"
                CREATE TEMP TABLE {tableVar} ({string.Join(", ", props.Select(m => $"{m.ColumnName} {m.ColumnType}"))});

                INSERT INTO {tableVar} VALUES
                    {deltaSql};

                INSERT INTO {entityInfo.TableName}
                ({columnsSql})
                SELECT
                {columnsSql}
                FROM {tableVar}
                RETURNING {entityInfo.TableName}.id";

            var dt = new DataTable();

            var conn = dbContext.Database.GetDbConnection();

            if (conn.State != ConnectionState.Open)
            {
                await conn.OpenAsync();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = sqlCmd;
                cmd.CommandType = CommandType.Text;

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    dt.Load(reader);
                }
            }

            var index = 0;
            foreach (DataRow row in dt.Rows)
            {
                ids[index] = row[0];

                index++;
            }

            return ids;
        }

        private static async Task<object[]> BulkAddAsync<TEntity>(List<TEntity> toAdd, object[] ids, DbContext dbContext, SqlEntityInfo entityInfo)
            where TEntity : class, new()
        {
            var tableVar = "@ToAdd";

            var props = entityInfo.PropertyMappings
                .Where(p => entityInfo.IsPrimaryKeyGenerated ? !p.IsPrimaryKey : true)
                .ToList();

            var columnsSql = $@"{string.Join(",", props.Select(p => $@"
                    {p.ColumnName}"))}";

            var deltaSql = string.Join(",", toAdd.Select(entity =>
            {
                return $@"
                    ({string.Join(", ", props.Select(m => $"{m.GetDbValue(entity)}"))})";
            }));

            var sqlCmd = $@"
                DECLARE {tableVar} TABLE ({string.Join(", ", props.Select(m => $"{m.ColumnName} {m.ColumnType}"))})

                SET NOCOUNT ON

                INSERT INTO {tableVar} VALUES
                    {deltaSql}

                SET NOCOUNT OFF

                INSERT INTO dbo.{entityInfo.TableName}
                ({columnsSql})
                OUTPUT Inserted.{entityInfo.PrimaryKeyColumnName}
                SELECT
                {columnsSql}
                FROM {tableVar}";

            var dt = new DataTable();

            var conn = dbContext.Database.GetDbConnection();

            if (conn.State != ConnectionState.Open)
            {
                await conn.OpenAsync();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = sqlCmd;
                cmd.CommandType = CommandType.Text;

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    dt.Load(reader);
                }
            }

            var index = 0;
            foreach (DataRow row in dt.Rows)
            {
                ids[index] = row[0];

                index++;
            }

            return ids;
        }
    }
}
