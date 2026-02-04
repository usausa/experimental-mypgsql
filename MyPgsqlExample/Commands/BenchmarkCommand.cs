namespace MyPgsqlExample.Commands;

using BenchmarkDotNet.Running;

using MyPgsqlExample.Benchmarks;

using Smart.CommandLine.Hosting;

[Command("benchmark", "Benchmark")]
public sealed class BenchmarkCommand : BaseCommand, ICommandHandler
{
    public ValueTask ExecuteAsync(CommandContext context)
    {
        Environment.SetEnvironmentVariable(PostgresBenchmark.ConnectionStringVariable, ConnectionString);

        BenchmarkRunner.Run<PostgresBenchmark>();

        return ValueTask.CompletedTask;
    }
}
