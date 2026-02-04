using MyPgsqlExample.Commands;

using Smart.CommandLine.Hosting;

var builder = CommandHost.CreateBuilder(args);
builder.ConfigureCommands(commands =>
{
    commands.ConfigureRootCommand(root =>
    {
        root.WithDescription("Example");
    });

    commands.AddCommand<SetupCommand>();
    commands.AddCommand<ExampleCommand>();
    commands.AddCommand<BenchmarkCommand>();
});

var host = builder.Build();
return await host.RunAsync();
