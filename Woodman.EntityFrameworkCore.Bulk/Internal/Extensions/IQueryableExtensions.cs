using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using Remotion.Linq.Parsing.ExpressionVisitors.TreeEvaluation;
using Remotion.Linq.Parsing.Structure;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;

namespace Microsoft.EntityFrameworkCore
{
    internal static class IQueryableExtensions
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

            var dependencies = typeof(QueryContextFactory)
                .GetProperty("Dependencies", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(queryContextFactory);

            if (dependencies is QueryContextDependencies queryContextDependencies)
            {
                return queryContextDependencies.CurrentDbContext.Context;
            }

            var stateManagerField = typeof(QueryContextDependencies)
                .GetTypeInfo()
                .DeclaredFields
                .Single(x => x.Name == nameof(QueryContextDependencies.StateManager));

            var stateManagerDynamic = stateManagerField.GetValue(dependencies);

            IStateManager stateManager = stateManagerDynamic as IStateManager;

            if (stateManager == null)
            {
                if (stateManagerDynamic is Internal.LazyRef<IStateManager> lazyStateManager)
                {
                    stateManager = lazyStateManager.Value;
                }
            }

            if (stateManager == null)
            {
                stateManager = ((dynamic)stateManagerDynamic).Value;
            }

            return stateManager.Context;
        }

        internal static string ToSql<TEntity>(this IQueryable<TEntity> queryable, out IReadOnlyList<NpgsqlParameter> parameters) where TEntity : class
        {
            var sql = queryable.ToSql(out IReadOnlyList<DbParameter> dbParams, true);

            parameters = dbParams.Select(p => new NpgsqlParameter(p.ParameterName, p.Value)).ToList();

            return sql;
        }

        internal static string ToSql<TEntity>(this IQueryable<TEntity> queryable, out IReadOnlyList<SqlParameter> parameters) where TEntity : class
        {
            var sql = queryable.ToSql(out IReadOnlyList<DbParameter> dbParams, false);

            parameters = dbParams.Select(p => new SqlParameter(p.ParameterName, p.Value)).ToList();

            return sql;
        }

        private static string ToSql<TEntity>(this IQueryable<TEntity> queryable, out IReadOnlyList<DbParameter> parameters, bool isNpgsql)
            where TEntity : class
        {
            var queryCompiler = queryable.Provider.Get<IQueryCompiler>(StaticFields.QueryCompilerField);
            var queryContextFactory = queryCompiler.Get<IQueryContextFactory>(StaticFields.QueryContextFactoryField);
            var queryContext = queryContextFactory.Create();

            var dbDependencies = queryCompiler.Get<Database>(StaticFields.DataBaseField)
                .Get<DatabaseDependencies>(StaticFields.DatabaseDependenciesField);

            var queryCompilationContextFactoryDependencies = dbDependencies.QueryCompilationContextFactory
                .Get<QueryCompilationContextDependencies>(StaticFields.QueryCompilationContextFactoryDependenciesField);

            var parameterExtractingExpressionVisitor = new ParameterExtractingExpressionVisitor(
                (IEvaluatableExpressionFilter)StaticFields.EvaluatableExpressionFilterField.GetValue(queryCompiler),
                queryContext,
                queryCompilationContextFactoryDependencies.Logger,
                true,
                false);

            var newQuery = parameterExtractingExpressionVisitor.ExtractParameters(queryable.Expression);

            var queryparser = (QueryParser)StaticFields.CreateQueryParserMethod
                .Invoke(queryCompiler, new[] { (INodeTypeProvider)StaticFields.NodeTypeProviderField.GetValue(queryCompiler) });

            var queryModel = queryparser.GetParsedQuery(newQuery);

            var queryModelVisitor = (RelationalQueryModelVisitor)dbDependencies.QueryCompilationContextFactory.Create(false).CreateQueryModelVisitor();

            queryModelVisitor.CreateQueryExecutor<TEntity>(queryModel);

            var sqlCommand = queryModelVisitor.Queries.First().CreateDefaultQuerySqlGenerator().GenerateSql(queryContext.ParameterValues);

            var dbCommand = isNpgsql
                ? new NpgsqlCommand(sqlCommand.CommandText) as DbCommand
                : new SqlCommand(sqlCommand.CommandText) as DbCommand;

            foreach (var param in sqlCommand.Parameters)
            {
                var paramVal = queryContext.ParameterValues[param.InvariantName];

                param.AddDbParameter(dbCommand, paramVal);
            }

            parameters = dbCommand.Parameters
                .Cast<DbParameter>()
                .ToList();

            return sqlCommand.CommandText;
        }
    }
}
