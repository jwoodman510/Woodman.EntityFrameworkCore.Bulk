using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.EntityFrameworkCore
{
    internal abstract class BulkExecutor<TEntity>
    {
        protected DbContext DbContext { get; }

        protected IEntityType EntityType { get; }

        protected List<IProperty> Properties { get; }

        protected PrimaryKey PrimaryKey { get; }

        protected BulkExecutor(DbContext dbContext)
        {
            DbContext = dbContext;

            EntityType = dbContext.Model.FindEntityType(typeof(TEntity));

            Properties = EntityType.GetProperties()?.ToList();

            PrimaryKey = new PrimaryKey(EntityType);
        }

        protected object GetPrimaryKey(TEntity entity)
        {
            if (PrimaryKey.IsCompositeKey)
            {
                return PrimaryKey.Keys.Select(k => k.Property.GetValue(entity)).ToArray();
            }
            else
            {
                return PrimaryKey.Primary.Property.GetValue(entity);
            }
        }

        protected object[] GetCompositeKey(TEntity entity)
        {
            if (!PrimaryKey.IsCompositeKey)
            {
                return null;
            }

            return PrimaryKey.Keys.Select(k => k.Property.GetValue(entity)).ToArray();
        }

        protected void ValidateCompositeKeys(List<object[]> keys)
        {
            if(!PrimaryKey.IsCompositeKey || keys == null || keys.Count == 0)
            {
                return;
            }

            var numExpected = PrimaryKey.Keys.Count;

            for(var i = 0; i < keys.Count; i++)
            {
                var key = keys[i];

                if(key == null || key.Length != numExpected)
                {
                    throw new ArgumentException($"Invalid Composite Key (index:{i}). Expected length of {numExpected}.");
                }

                for(var j = 0; j < key.Length; j++)
                {
                    var expected = PrimaryKey.Keys[j];
                    var actual = key[j];

                    if(actual == null)
                    {
                        throw new ArgumentException($"Invalid Composite Key (index:{i}:{j}). Value cannot be null.");
                    }

                    var actualType = actual.GetType();

                    if(actualType != expected.Property.PropertyType)
                    {
                        throw new ArgumentException($"Invalid Composite Key (index:{i}:{j}). " +
                            $"Expected type {expected.Property.PropertyType.Name} for Property {expected.Property.Name} but found type: {actualType.Name}.");
                    }
                }
            }
        }

        protected void SetPrimaryKey(TEntity entity, object keyVal)
        {
            if (PrimaryKey.IsCompositeKey)
            {
                var keyVals = keyVal as object[];

                if(keyVals == null)
                {
                    return;
                }

                for(var i = 0; i < keyVals.Length; i++)
                {
                    PrimaryKey.Keys[i].Property.SetValue(entity, keyVals[i]);
                }
            }
            else
            {
                PrimaryKey.Primary.Property.SetValue(entity, keyVal);
            }
        }

        protected bool PrimaryKeyEquals(TEntity entity1, TEntity entity2)
        {
            if (PrimaryKey.IsCompositeKey)
            {
                var pk1 = GetCompositeKey(entity1);
                var pk2 = GetCompositeKey(entity2);

                return Enumerable.SequenceEqual(pk1, pk2);
            }
            else
            {
                var keyVal1 = GetPrimaryKey(entity1);
                var keyVal2 = GetPrimaryKey(entity2);

                return keyVal1 == null || keyVal2 == null
                        ? keyVal1 == null && keyVal2 == null
                        : keyVal1.Equals(keyVal2);
            }
        }

        protected bool IsPrimaryKeyUnset(TEntity entity)
        {
            if (PrimaryKey.IsCompositeKey)
            {
                for(var i = 0; i < PrimaryKey.Keys.Count; i++)
                {
                    var key = PrimaryKey.Keys[i];
                    var keyVal = key.Property.GetValue(entity);

                    var isDefault = key.DefaultValue == null
                        ? keyVal == null
                        : key.DefaultValue.Equals(keyVal);

                    if (isDefault)
                    {
                        return true;
                    }
                }

                return false;
            }
            else
            {
                var val = GetPrimaryKey(entity);

                return val == null
                    ? PrimaryKey.Primary.DefaultValue == null
                    : val.Equals(PrimaryKey.Primary.DefaultValue);
            }           
        }
    }
}
