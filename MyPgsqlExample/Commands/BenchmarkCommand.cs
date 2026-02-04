namespace MyPgsqlExample.Commands;

using BenchmarkDotNet.Running;

using MyPgsqlExample.Benchmarks;

using Smart.CommandLine.Hosting;

[Command("benchmark", "Benchmark")]
public sealed class BenchmarkCommand : ICommandHandler
{
    // TODO
    private const string ConnectionString = "Host=mysql-server;Port=5432;Database=test;Username=test;Password=test";

    public ValueTask ExecuteAsync(CommandContext context)
    {
        Environment.SetEnvironmentVariable(PostgresBenchmark.ConnectionStringVariable, ConnectionString);

        BenchmarkRunner.Run<PostgresBenchmark>();

        return ValueTask.CompletedTask;
    }
}
