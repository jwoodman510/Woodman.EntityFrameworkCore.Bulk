using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.EntityFrameworkCore
{
    internal abstract class RelationalBulkExecutor<TEntity> : BulkExecutor<TEntity> where TEntity : class
    {
        protected string TableName { get; }

        protected List<RelationalPropertyMapping> PropertyMappings { get; }

        protected RelationalBulkExecutor(DbContext dbContext)
            : base(dbContext)
        {
            TableName = EntityType.Relational()?.TableName;
            PropertyMappings = EntityType.GetProperties()?.ToList()?.Select(p => new RelationalPropertyMapping(p))?.ToList();
        }

        protected async Task<int> ExecuteSqlCommandAsync(string sqlCmd, List<TEntity> entities, IEnumerable<DbParameter> cmdParams = null)
        {
            var conn = DbContext.Database.GetDbConnection();

            if (conn.State != ConnectionState.Open)
            {
                await conn.OpenAsync();
            }

            var numRecordsAffected = 0;

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = sqlCmd;
                cmd.CommandType = CommandType.Text;

                if(cmdParams != null)
                {
                    cmd.Parameters.AddRange(cmdParams.ToArray());
                }

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    var entityIdIndex = 0;
                    bool? hasActionColumn = null;

                    while (await reader.ReadAsync())
                    {
                        if(hasActionColumn == null)
                        {
                            hasActionColumn = GetHasActionColumn(reader);
                        }

                        if (hasActionColumn == false || reader["action"].ToString() == MergeActions.Insert)
                        {
                            if (PrimaryKey.IsCompositeKey)
                            {
                                for (var i = entityIdIndex; i < entities.Count; i++)
                                {
                                    if (IsPrimaryKeyUnset(entities[i]))
                                    {
                                        var key = new object[PrimaryKey.Keys.Count];

                                        for(var keyIndex = 0; keyIndex < key.Length; keyIndex++)
                                        {
                                            key[keyIndex] = hasActionColumn.Value ? reader[keyIndex + 1] : reader[keyIndex];
                                        }

                                        SetPrimaryKey(entities[i], key);
                                        entityIdIndex = i + 1;
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                var id = hasActionColumn.Value ? reader[1] : reader[0];

                                for (var i = entityIdIndex; i < entities.Count; i++)
                                {
                                    if (IsPrimaryKeyUnset(entities[i]))
                                    {
                                        SetPrimaryKey(entities[i], id);
                                        entityIdIndex = i + 1;
                                        break;
                                    }
                                }
                            }
                        }

                        numRecordsAffected++;
                    }
                }
            }

            return numRecordsAffected;
        }

        protected static string StringifyKeyVal(object value)
        {
            if (value == null)
            {
                return "NULL";
            }

            return value.ToString().Replace("'", "''");
        }

        private static bool GetHasActionColumn(DbDataReader reader)
        {
            if (reader.CanGetColumnSchema())
            {
                return reader
                    .GetColumnSchema()
                    .Any(c => c.ColumnName != null && c.ColumnName.Equals("action", StringComparison.CurrentCultureIgnoreCase));
            }

            try
            {
                var val = reader["action"];
                return true;
            }
            catch (IndexOutOfRangeException)
            {
                return false;
            }
        }
    }

    internal class RelationalPropertyMapping
    {
        private readonly IProperty _property;

        public string PropertyName => _property.Name;

        public string ColumnName { get; }

        public string ColumnType { get; }

        public bool IsNullable { get; }

        public bool IsPrimaryKey { get; }

        public bool IsGenerated { get; }

        public RelationalPropertyMapping(IProperty property)
        {
            _property = property;
            IsNullable = _property?.IsNullable ?? false;
            IsPrimaryKey = _property.IsPrimaryKey();
            IsGenerated = property.ValueGenerated == ValueGenerated.OnAdd;

            var propertyAnnotations = _property.Relational();

            ColumnName = propertyAnnotations?.ColumnName;
            ColumnType = (propertyAnnotations?.ColumnType ?? string.Empty) + (IsNullable ? " NULL" : string.Empty);
        }

        public object GetPropertyValue(object entity)
        {
            return _property.PropertyInfo.GetValue(entity);
        }

        public string GetDbValue(object entity)
        {
            var val = _property.PropertyInfo.GetValue(entity);

            return val == null
                ? "NULL"
                : $"'{val.ToString().Replace("'", "''")}'";
        }
    }
}
