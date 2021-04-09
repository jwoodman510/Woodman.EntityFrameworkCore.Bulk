using System;
using System.Collections.Generic;

namespace Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.Sql
{
    public partial class EfCoreTestChild
    {
        public int Id { get; set; }
        public int EfCoreTestId { get; set; }
        public string Name { get; set; }
        public DateTime CreatedDate { get; set; }
        public DateTime ModifiedDate { get; set; }

        public EfCoreTest EfCoreTest { get; set; }
    }
}
