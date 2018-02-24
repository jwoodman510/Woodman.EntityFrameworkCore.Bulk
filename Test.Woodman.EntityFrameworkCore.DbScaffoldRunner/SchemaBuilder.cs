using Microsoft.Extensions.Configuration;
using Npgsql;
using System.Data.SqlClient;
using System.IO;

namespace Test.Woodman.EntityFrameworkCore.DbScaffoldRunner
{
    public class SchemaBuilder
    {
        public void BuildSql(IConfiguration configuration)
        {
            foreach(var file in configuration.GetSection("scripts:sql").Get<string[]>())
            {
                var filePath = Path.Combine(Directory.GetCurrentDirectory(), file);
                var sqlScript = File.ReadAllText(filePath);
                var connString = configuration["connectionStrings:sql"];

                using (var cmd = new SqlCommand(sqlScript, new SqlConnection(connString)))
                {
                    if (cmd.Connection.State != System.Data.ConnectionState.Open)
                    {
                        cmd.Connection.Open();
                    }

                    cmd.ExecuteNonQuery();
                }
            }            
        }

        public void BuildNpgSql(IConfiguration configuration)
        {
            foreach (var file in configuration.GetSection("scripts:npgsql").Get<string[]>())
            {
                var filePath = Path.Combine(Directory.GetCurrentDirectory(), file);
                var sqlScript = File.ReadAllText(filePath);
                var connString = configuration["connectionStrings:npgsql"];

                using (var cmd = new NpgsqlCommand(sqlScript, new NpgsqlConnection(connString)))
                {
                    if (cmd.Connection.State != System.Data.ConnectionState.Open)
                    {
                        cmd.Connection.Open();
                    }

                    cmd.ExecuteNonQuery();
                }
            }
        }
    }
}
