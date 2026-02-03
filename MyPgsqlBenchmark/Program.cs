namespace MyPgsqlBenchmark;

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;

using MyPgsql;

using Npgsql;

internal static class Program
{
    public static void Main()
    {
        BenchmarkRunner.Run<PostgresBenchmarks>();
    }
}

[MemoryDiagnoser]
#pragma warning disable CA1001
#pragma warning disable CA1707
#pragma warning disable CA1849
public class PostgresBenchmarks
{
    private const string ConnectionString = "Host=mysql-server;Port=5432;Database=test;Username=test;Password=test";

    private NpgsqlConnection npgsqlConnection = null!;
    private PgConnection myPgsqlConnection = null!;
    //private int insertId;

    [GlobalSetup]
    public async Task Setup()
    {
        npgsqlConnection = new NpgsqlConnection(ConnectionString);
        await npgsqlConnection.OpenAsync();

        myPgsqlConnection = new PgConnection(ConnectionString);
        await myPgsqlConnection.OpenAsync();

        //insertId = 100000;
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

    //[Benchmark(Description = "Npgsql: INSERT and DELETE user")]
    //public async Task Npgsql_InsertDeleteUser()
    //{
    //    var id = Interlocked.Increment(ref _insertId);

    //    // INSERT
    //    await using (var cmd = new NpgsqlCommand(
    //        "INSERT INTO users (id, name, email, created_at) VALUES (@id, @name, @email, @created_at)",
    //        npgsqlConnection))
    //    {
    //        cmd.Parameters.AddWithValue("@id", id);
    //        cmd.Parameters.AddWithValue("@name", "Benchmark User");
    //        cmd.Parameters.AddWithValue("@email", "benchmark@example.com");
    //        cmd.Parameters.AddWithValue("@created_at", DateTime.UtcNow);
    //        await cmd.ExecuteNonQueryAsync();
    //    }

    //    // DELETE
    //    await using (var cmd = new NpgsqlCommand("DELETE FROM users WHERE id = @id", npgsqlConnection))
    //    {
    //        cmd.Parameters.AddWithValue("@id", id);
    //        await cmd.ExecuteNonQueryAsync();
    //    }
    //}
}
#pragma warning restore CA1849
#pragma warning restore CA1707
#pragma warning restore CA1001
