using Microsoft.Extensions.Configuration;
using System;
using System.Diagnostics;
using System.IO;

namespace Test.Woodman.EntityFrameworkCore.DbScaffoldRunner
{
    public class Program
    {
        public static void Main(string[] args)
        {
            IConfiguration config = null;
            var rebuildSchema = false;

            Try(() =>
            {
                config = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                    .Build();

                rebuildSchema = bool.Parse(config["rebuildSchema"]);
            }, "Load Config");

            if (rebuildSchema)
            {
                Try(() =>
                {
                    new SchemaBuilder().BuildSql(config);
                }, "Build SQL Schema");

                Try(() =>
                {
                    new SchemaBuilder().BuildNpgSql(config);
                }, "Build NpgSql Schema");
            }
            else
            {
                Console.WriteLine("AppSettings.RebuildSchema set to false.");
            }

            Try(() =>
            {
                var path = Path.Combine(Directory.GetCurrentDirectory(), config["commands:sql"]);

                RunBatchFile(path);

            }, "SQL Scaffold");

            Try(() =>
            {
                var path = Path.Combine(Directory.GetCurrentDirectory(), config["commands:npgsql"]);

                RunBatchFile(path);
            }, "NpgSql Scaffold");
        }

        private static void RunBatchFile(string path)
        {
            var processInfo = new ProcessStartInfo("cmd.exe", "/c " + path)
            {
                CreateNoWindow = true,
                UseShellExecute = false,
                RedirectStandardError = true,
                RedirectStandardOutput = true
            };

            var process = Process.Start(processInfo);

            process.WaitForExit();

            string error = process.StandardError.ReadToEnd();

            if (!string.IsNullOrEmpty(error))
            {
                throw new Exception(error);
            }
        }

        private static void Try(Action action, string actionName)
        {
            try
            {
                Console.WriteLine($"starting {actionName}");

                action();

                Console.WriteLine($"{actionName} complete");
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"{actionName} failed");
                Console.WriteLine(ex);
                Console.ReadLine();
                throw;
            }
        }
    }
}
