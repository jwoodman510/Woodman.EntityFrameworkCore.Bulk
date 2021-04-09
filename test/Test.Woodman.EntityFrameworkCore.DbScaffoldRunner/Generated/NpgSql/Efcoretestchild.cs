using System;
using System.Collections.Generic;

namespace Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.NpgSql
{
    public partial class Efcoretestchild
    {
        public int Id { get; set; }
        public int Efcoretestid { get; set; }
        public string Name { get; set; }
        public DateTime Createddate { get; set; }
        public DateTime Modifieddate { get; set; }

        public Efcoretest Efcoretest { get; set; }
    }
}
