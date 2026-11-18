using System.CommandLine.Help;

using MyPgsqlExample.Commands;

using Smart.CommandLine.Hosting;

var builder = CommandHost.CreateBuilder(args);
builder.ConfigureCommands(commands =>
{
    commands.ConfigureRootCommand(root =>
    {
        root.WithDescription("Example");
        root.Configure(static command =>
        {
            var help = command.Options.OfType<HelpOption>().First();
            help.Aliases.Remove("-h");
            help.Aliases.Remove("/h");
        });
    });

    commands.AddCommand<SetupCommand>();
    commands.AddCommand<ExampleCommand>();
    commands.AddCommand<BenchmarkCommand>();
});

var host = builder.Build();
return await host.RunAsync();
