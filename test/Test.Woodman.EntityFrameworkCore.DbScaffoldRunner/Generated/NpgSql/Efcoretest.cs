using System;
using System.Collections.Generic;

namespace Woodman.EntityFrameworkCore.DbScaffoldRunner.Generated.NpgSql
{
    public partial class Efcoretest
    {
        public Efcoretest()
        {
            Efcoretestchild = new HashSet<Efcoretestchild>();
        }

        public int Id { get; set; }
        public string Name { get; set; }
        public DateTime Createddate { get; set; }
        public DateTime Modifieddate { get; set; }

        public ICollection<Efcoretestchild> Efcoretestchild { get; set; }
    }
}
