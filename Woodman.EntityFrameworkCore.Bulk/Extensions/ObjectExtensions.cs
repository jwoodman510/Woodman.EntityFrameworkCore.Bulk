using System.Reflection;

namespace Woodman.EntityFrameworkCore.Bulk.Extensions
{
    internal static class ObjectExtensions
    {
        internal static T Get<T>(this object obj, FieldInfo fieldInfo) where T : class
        {
            return (T)fieldInfo.GetValue(obj);
        }

        internal static T Get<T>(this object obj, PropertyInfo propertyInfo) where T : class
        {
            return (T)propertyInfo.GetValue(obj);
        }
    }
}
