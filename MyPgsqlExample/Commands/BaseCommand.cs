namespace MyPgsqlExample.Commands;

using Smart.CommandLine.Hosting;

public abstract class BaseCommand
{
    [Option<string>("--host", "-h", Description = "Host", DefaultValue = "postgres")]
    public string Host { get; set; } = default!;

    [Option<int>("--port", "-p", Description = "Port", DefaultValue = 5432)]
    public int Port { get; set; }

    [Option<string>("--database", "-d", Description = "Database", DefaultValue = "test")]
    public string Database { get; set; } = default!;

    [Option<string>("--username", "-u", Description = "Username", DefaultValue = "test")]
    public string Username { get; set; } = default!;

    [Option<string>("--password", "-p", Description = "Password", DefaultValue = "test")]
    public string Password { get; set; } = default!;

    protected string ConnectionString =>
        $"Host={Host};Port={Port};Database={Database};Username={Username};Password={Password}";
}
