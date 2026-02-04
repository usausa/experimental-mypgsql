namespace MyPgsqlExample.Commands;

using Smart.CommandLine.Hosting;

[Command("setup", "Database setup")]
public sealed class SetupCommand : BaseCommand, ICommandHandler
{
    public ValueTask ExecuteAsync(CommandContext context)
    {
        // TODO

        return ValueTask.CompletedTask;
    }
}
