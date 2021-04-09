using Microsoft.Extensions.Configuration;
using System;
using System.Diagnostics;
using System.IO;

namespace Woodman.EntityFrameworkCore.DbScaffoldRunner
{
    public class Program
    {
        public static void Main(string[] args)
        {
            IConfiguration config = null;
            var runScripts = false;
            var runCommands = false;

            Try(() =>
            {
                config = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                    .Build();

                runScripts = bool.Parse(config["runScripts"]);
                runCommands = bool.Parse(config["runCommands"]);
            }, "Load Config");

            if (runScripts)
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
                Console.WriteLine($"{nameof(runScripts)} set to false.");
            }

            if (runCommands)
            {
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
            else
            {
                Console.WriteLine($"{nameof(runCommands)} set to false.");
            }
        }

        private static void RunBatchFile(string path)
        {
            using (var process = Process.Start(new ProcessStartInfo("cmd.exe", "/c " + path)
            {
                CreateNoWindow = true,
                UseShellExecute = false,
                RedirectStandardError = true,
                RedirectStandardOutput = true
            }))
            {
                process.WaitForExit();

                string error = process.StandardError.ReadToEnd();

                if (!string.IsNullOrEmpty(error))
                {
                    throw new Exception(error);
                }
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
