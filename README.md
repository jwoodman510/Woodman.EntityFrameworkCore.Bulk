# Woodman.EntityFrameworkCore.Bulk #

## Bulk Operations ##

### The following bulk operations are supported for SqlServer, NgpSql, and InMemory Providers: ###

#### Bulk Join ####
````
using(var dbContext = _dbContextProvider.GetDbContext(c => new DbContext(c)))
{
   var ids = new int[] { 123, 1234, 12345 };
	
   var queryable = dbContext.BulkJoin(ids);
	
   var entities = await queryable.Entity.ToListAsync();
}
````

#### Bulk Remove ####
````
using(var dbContext = _dbContextProvider.GetDbContext(c => new DbContext(c)))
{
   var ids = new int[] { 123, 1234, 12345 };
	
   int numDeleted = await dbContext.Entity.BulkRemoveAsync(ids);
}
````

#### Bulk Add ####
````
using(var dbContext = _dbContextProvider.GetDbContext(c => new DbContext(c)))
{
   var entities = new List<Entity>();

   entites.Add(new Entity { Name = "E1" };
   entites.Add(new Entity { Name = "E2" };
   
   object[] insertedIds = await dbContext.Entity.BulkAddAsync(entities);
}
````

#### Bulk Update ####
````
using(var dbContext = _dbContextProvider.GetDbContext(c => new DbContext(c)))
{
   // update a set of records using a constructor expression

   await dbContext.Entity.BulkUpdateAsync(() => new Entity
   {
      UpdatedDate = DateTime.UtcNow
   });
   
   // optionally pass in a set of ids to update records using the corresponding id
	
   var ids = new int[] { 123, 1234, 12345 };
	
   await dbContext.Entity.BulkUpdateAsync(ids, id => new Entity
   {
      UpdatedDate = id === 123 ? DateTime.UtcNow : DateTime.UtcNow.AddDays(1)
   });
}
````

#### Bulk Merge ####
````
   var entities = new List<Entity>();

   entites.Add(new Entity { Id = 1, Name = "E1" };
   entites.Add(new Entity { Name = "E2" };
   
   BulkMergeResult result = await dbContext.Entity.BulkMergeAsync(entities);
   
   int numRowsAffected = result.NumRowsAffected;

   object[] insertedIds = result.InsertedIds;
````