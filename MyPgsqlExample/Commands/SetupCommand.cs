namespace MyPgsqlExample.Commands;

using System.Diagnostics;

using Mofucat.DataBridge;

using Npgsql;

using NpgsqlBulkHelper;

using Smart.CommandLine.Hosting;

[Command("setup", "Database setup")]
public sealed class SetupCommand : BaseCommand, ICommandHandler
{
    public async ValueTask ExecuteAsync(CommandContext context)
    {
        await using var con = new NpgsqlConnection(ConnectionString);
        await con.OpenAsync();

        await ExecuteNonQueryAsync(con, "DROP TABLE IF EXISTS data");
        await ExecuteNonQueryAsync(con, "DROP TABLE IF EXISTS users");

#pragma warning disable SA1118
        await ExecuteNonQueryAsync(
            con,
            """
            CREATE TABLE data (
                id        INTEGER      NOT NULL CONSTRAINT data_pk PRIMARY KEY,
                name      VARCHAR(50)  NOT NULL,
                option    VARCHAR(100),
                flag      BOOLEAN      NOT NULL,
                create_at TIMESTAMP    NOT NULL
            )
            """);
        await ExecuteNonQueryAsync(
            con,
            """
            CREATE TABLE users
            (
                id         INTEGER       NOT NULL PRIMARY KEY,
                name       VARCHAR(100)  NOT NULL,
                email      VARCHAR(100)  NOT NULL,
                created_at TIMESTAMP     NOT NULL
            )
            """);
#pragma warning restore SA1118

        var bulkCopy = new NpgsqlBulkCopy(con)
        {
            DestinationTableName = "data"
        };

        using var source = new ObjectDataReader<Data>(Enumerable.Range(1, 100000).Select(static x => new Data
        {
            Id = x,
            Name = $"Name-{x}",
            Option = x % 3 == 0 ? null : "Options",
            Flag = x % 2 == 0,
            CreatedAt = DateTime.Now
        }));

        var watch = Stopwatch.StartNew();
        var inserted = await bulkCopy.WriteToServerAsync(source);
        var elapsed = watch.Elapsed;

        Console.WriteLine($"Inserted: rows=[{inserted}], elapsed=[{elapsed.TotalMilliseconds}]");
    }

#pragma warning disable CA2100
    private static async ValueTask ExecuteNonQueryAsync(NpgsqlConnection con, string sql)
    {
        await using var cmd = new NpgsqlCommand(sql, con);
        await cmd.ExecuteNonQueryAsync();
    }
#pragma warning restore CA2100

    public class Data
    {
        public int Id { get; set; }

        public string Name { get; set; } = default!;

        public string? Option { get; set; }

        public bool Flag { get; set; }

        public DateTime CreatedAt { get; set; }
    }
}
