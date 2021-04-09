using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Npgsql;
using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Linq;
using Microsoft.EntityFrameworkCore.InMemory.Query.Internal;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Reflection;

namespace Microsoft.EntityFrameworkCore
{
    public static class IQueryableExtensions
    {
        internal static IBulkExecutor<TEntity> BuildBulkExecutor<TEntity>(this IQueryable<TEntity> queryable) where TEntity : class
        {
            IBulkExecutor<TEntity> executor = null;

            var dbContext = queryable.GetDbContext();

            if (dbContext.Database.IsSqlServer())
            {
                executor = new SqlServerBulkExecutor<TEntity>(dbContext);
            }
            else if (dbContext.Database.IsNpgsql())
            {
                executor = new NpgSqlBulkExecutor<TEntity>(dbContext);
            }
            else if (dbContext.Database.IsInMemory())
            {
                executor = new InMemBulkExecutor<TEntity>(dbContext);
            }

            if (executor == null)
            {
                throw new Exception($"Unsupported {nameof(dbContext.Database.ProviderName)} {dbContext.Database.ProviderName}.");
            }

            return executor;
        }

        internal static DbContext GetDbContext<T>(this IQueryable<T> queryable)
        {
            var queryCompiler = queryable.Provider.Get<QueryCompiler>(StaticFields.QueryCompilerField);

            var queryContextFactory = queryCompiler.Get<IQueryContextFactory>(StaticFields.QueryContextFactoryField);

            var dependencies = queryContextFactory is InMemoryQueryContextFactory inMemoryQueryContextFactory
                ? inMemoryQueryContextFactory.Get(StaticFields.MemQueryContextDependenciesField)
                : queryContextFactory.Get(StaticFields.RelationalQueryContextDependenciesField);

            if (dependencies is QueryContextDependencies queryContextDependencies  &&
                queryContextDependencies.CurrentContext?.Context != null)
            {
                return queryContextDependencies.CurrentContext.Context;
            }

            var stateManagerDynamic = dependencies.Get(StaticFields.StateManagerProperty);

            if (stateManagerDynamic == null)
            {
                return null;
            }

            if (stateManagerDynamic is IStateManager stateManager)
            {
#pragma warning disable EF1001 // Internal EF Core API usage.
                return stateManager.Context;
#pragma warning restore EF1001 // Internal EF Core API usage.
            }

            return ((dynamic)stateManagerDynamic)?.Value;
        }

        public static string ToSql<TEntity>(this IQueryable<TEntity> queryable)
        {
            return queryable.ToSql(out IReadOnlyDictionary<string, object> _);
        }

        public static string ToSql<TEntity>(this IQueryable<TEntity> queryable, out IReadOnlyList<NpgsqlParameter> parameters)
        {
            var sql = queryable.ToSql(out IReadOnlyDictionary<string, object> baseParams);

            parameters = baseParams.Select(x => new NpgsqlParameter(x.Key, x.Value)).ToList();

            return sql;
        }

        public static string ToSql<TEntity>(this IQueryable<TEntity> queryable, out IReadOnlyList<SqlParameter> parameters)
        {
            var sql = queryable.ToSql(out IReadOnlyDictionary<string, object> baseParams);

            parameters = baseParams.Select(x => new SqlParameter(x.Key, x.Value)).ToList();

            return sql;
        }

        private static string ToSql<TEntity>(this IQueryable<TEntity> queryable, out IReadOnlyDictionary<string, object> parameters)
        {
            var enumerator = queryable.Provider.Execute<IEnumerable<TEntity>>(queryable.Expression).GetEnumerator();

            var reliationalFieldInfo = enumerator.GetType().GetTypeInfo()
                .DeclaredFields
                .FirstOrDefault(x => x.Name == "_relationalCommandCache");

            var reliationalQueryContextFieldInfo = enumerator.GetType().GetTypeInfo()
                .DeclaredFields
                .FirstOrDefault(x => x.Name == "_relationalQueryContext");

            if (reliationalFieldInfo == null || reliationalQueryContextFieldInfo == null)
            {
                throw new InvalidOperationException($"Invalid Provider: {queryable.Provider.GetType().Name}.");
            }

            var relationalCommandCache = enumerator.Get<RelationalCommandCache>(reliationalFieldInfo);
            var queryContext = enumerator.Get<RelationalQueryContext>(reliationalQueryContextFieldInfo);
            var selectExpression = relationalCommandCache.Get<SelectExpression>(StaticFields.SelectExpressionField);

#pragma warning disable EF1001 // Internal EF Core API usage.
            var command = relationalCommandCache.GetRelationalCommand(queryContext.ParameterValues);
#pragma warning restore EF1001 // Internal EF Core API usage.

            parameters = queryContext.ParameterValues
                .Where(x => command.Parameters.Any(p => p.InvariantName == x.Key))
                .ToDictionary(x => x.Key, y => y.Value);

            return command.CommandText;
        }
    }
}
