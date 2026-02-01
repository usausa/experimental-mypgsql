namespace MyPgsql;

using System.Globalization;

public sealed class PgConnectionStringBuilder
{
    public string Host { get; set; } = "localhost";

    public int Port { get; set; } = 5432;

    public string Database { get; set; } = string.Empty;

    public string Username { get; set; } = string.Empty;

    public string Password { get; set; } = string.Empty;

    public PgConnectionStringBuilder()
    {
    }

    public PgConnectionStringBuilder(string connectionString)
    {
        Parse(connectionString);
    }

    private void Parse(string connectionString)
    {
        foreach (var part in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var idx = part.IndexOf('=', StringComparison.Ordinal);
            if (idx <= 0)
            {
                continue;
            }

#pragma warning disable CA1308
            var key = part[..idx].Trim().ToLowerInvariant();
#pragma warning restore CA1308
            var value = part[(idx + 1)..].Trim();

            switch (key)
            {
                case "host" or "server":
                    Host = value;
                    break;
                case "port":
                    Port = Int32.Parse(value, CultureInfo.InvariantCulture);
                    break;
                case "database" or "db":
                    Database = value;
                    break;
                case "username" or "user" or "uid":
                    Username = value;
                    break;
                case "password" or "pwd":
                    Password = value;
                    break;
            }
        }
    }

    public override string ToString()
    {
        return $"Host={Host};Port={Port};Database={Database};Username={Username};Password={Password}";
    }
}
