namespace MyPgsqlExample.Benchmarks;

using BenchmarkDotNet.Attributes;

using Npgsql;

using MyPgsql;

[MemoryDiagnoser]
#pragma warning disable CA1001
#pragma warning disable CA1707
#pragma warning disable CA1849
public class PostgresBenchmark
{
    public const string ConnectionStringVariable = "BENCH_CONNECTION_STRING";

    private NpgsqlConnection npgsqlConnection = default!;
    private PgConnection myPgsqlConnection = default!;

    private int insertId;

    [GlobalSetup]
    public async Task Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable) ??
                           throw new InvalidOperationException("Environment variable is not set");

        npgsqlConnection = new NpgsqlConnection(connectionString);
        await npgsqlConnection.OpenAsync();

        myPgsqlConnection = new PgConnection(connectionString);
        await myPgsqlConnection.OpenAsync();

        insertId = 100000;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await npgsqlConnection.DisposeAsync();
        await myPgsqlConnection.DisposeAsync();
    }

    [Benchmark(Description = "Npgsql: SELECT all from data")]
    public async Task<int> Npgsql_SelectAllData()
    {
        await using var cmd = new NpgsqlCommand("SELECT id, name, option, flag, create_at FROM data", npgsqlConnection);
        await using var reader = await cmd.ExecuteReaderAsync();

        var count = 0;
        while (await reader.ReadAsync())
        {
            _ = reader.GetInt32(0);
            _ = reader.GetString(1);
            _ = reader.IsDBNull(2) ? null : reader.GetString(2);
            _ = reader.GetBoolean(3);
            _ = reader.GetDateTime(4);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "MyPgsql: SELECT all from data")]
    public async Task<int> MyPgsql_SelectAllData()
    {
        await using var cmd = myPgsqlConnection.CreateCommand();
        cmd.CommandText = "SELECT id, name, option, flag, create_at FROM data";
        await using var reader = await cmd.ExecuteReaderAsync();

        var count = 0;
        while (await reader.ReadAsync())
        {
            _ = reader.GetInt32(0);
            _ = reader.GetString(1);
            _ = reader.IsDBNull(2) ? null : reader.GetString(2);
            _ = reader.GetBoolean(3);
            _ = reader.GetDateTime(4);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "Npgsql: INSERT and DELETE user")]
    public async Task Npgsql_InsertDeleteUser()
    {
        var id = Interlocked.Increment(ref insertId);

        // INSERT
        await using (var cmd = new NpgsqlCommand("INSERT INTO users (id, name, email, created_at) VALUES (@id, @name, @email, @created_at)", npgsqlConnection))
        {
            cmd.Parameters.AddWithValue("@id", id);
            cmd.Parameters.AddWithValue("@name", "Benchmark User");
            cmd.Parameters.AddWithValue("@email", "benchmark@example.com");
            cmd.Parameters.AddWithValue("@created_at", DateTime.UtcNow);
            await cmd.ExecuteNonQueryAsync();
        }

        // DELETE
        await using (var cmd = new NpgsqlCommand("DELETE FROM users WHERE id = @id", npgsqlConnection))
        {
            cmd.Parameters.AddWithValue("@id", id);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    [Benchmark(Description = "MyPgsql: INSERT and DELETE user")]
    public async Task MyPgsql_InsertDeleteUser()
    {
        var id = Interlocked.Increment(ref insertId);

        // INSERT
        await using (var cmd = new PgCommand("INSERT INTO users (id, name, email, created_at) VALUES (@id, @name, @email, @created_at)", myPgsqlConnection))
        {
            cmd.Parameters.AddWithValue("@id", id);
            cmd.Parameters.AddWithValue("@name", "Benchmark User");
            cmd.Parameters.AddWithValue("@email", "benchmark@example.com");
            cmd.Parameters.AddWithValue("@created_at", DateTime.UtcNow);
            await cmd.ExecuteNonQueryAsync();
        }

        // DELETE
        await using (var cmd = new PgCommand("DELETE FROM users WHERE id = @id", myPgsqlConnection))
        {
            cmd.Parameters.AddWithValue("@id", id);
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
