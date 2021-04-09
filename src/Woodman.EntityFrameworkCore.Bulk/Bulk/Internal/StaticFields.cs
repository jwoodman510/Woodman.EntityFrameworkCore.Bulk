using Microsoft.EntityFrameworkCore.InMemory.Query.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq;
using System.Reflection;

namespace Microsoft.EntityFrameworkCore
{
    internal static class StaticFields
    {
        internal static readonly FieldInfo DataBaseField = typeof(QueryCompiler)
            .GetTypeInfo()
            .DeclaredFields
            .First(x => x.Name == "_database");

        internal static readonly PropertyInfo DatabaseDependenciesField = typeof(Database)
            .GetTypeInfo()
            .DeclaredProperties
            .First(x => x.Name == "Dependencies");

        internal static readonly FieldInfo QueryCompilerField = typeof(EntityQueryProvider)
            .GetTypeInfo()
            .DeclaredFields
            .First(x => x.Name == "_queryCompiler");
        
        internal static readonly FieldInfo QueryContextFactoryField = typeof(QueryCompiler)
            .GetTypeInfo()
            .DeclaredFields
            .Single(x => x.Name == "_queryContextFactory");

        internal static readonly FieldInfo QueryContextDependenciesField = typeof(QueryCompilationContextFactory)
            .GetTypeInfo()
            .DeclaredFields
            .First(x => x.Name == "_dependencies");

        internal static readonly FieldInfo RelationalQueryContextDependenciesField = typeof(RelationalQueryContextFactory)
            .GetTypeInfo()
            .DeclaredFields
            .First(x => x.Name == "_dependencies");

        internal static readonly FieldInfo MemQueryContextDependenciesField = typeof(InMemoryQueryContextFactory)
            .GetTypeInfo()
            .DeclaredFields
            .First(x => x.Name == "_dependencies");

        internal static readonly PropertyInfo StateManagerProperty = typeof(QueryContextDependencies)
            .GetTypeInfo()
            .DeclaredProperties
            .First(x => x.Name == "StateManager");

        internal static FieldInfo SelectExpressionField = typeof(RelationalCommandCache)
            .GetTypeInfo()
            .DeclaredFields
            .First(x => x.Name == "_selectExpression");

        internal static FieldInfo QuerySqlGeneratorFactoryField = typeof(RelationalCommandCache)
            .GetTypeInfo()
            .DeclaredFields
            .First(x => x.Name == "_querySqlGeneratorFactory");
    }

    internal static class MergeActions
    {
        internal static string Insert => "INSERT";
        internal static string Update => "UPDATE";
        internal static string Delete => "DELETE";
    }
}
