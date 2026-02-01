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
public class PostgresBenchmarks
{
    private const string ConnectionString = "Host=mysql-server;Port=5432;Database=test;Username=test;Password=test";

    private NpgsqlConnection _npgsqlConnection = null!;
    private PgConnection _myPgsqlConnection = null!;
    //private int _insertId;

    [GlobalSetup]
    public async Task Setup()
    {
        // Npgsql接続
        _npgsqlConnection = new NpgsqlConnection(ConnectionString);
        await _npgsqlConnection.OpenAsync();

        // MyPgsql接続 (バイナリプロトコル版)
        _myPgsqlConnection = new PgConnection(ConnectionString);
        await _myPgsqlConnection.OpenAsync();

        //_insertId = 100000;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _npgsqlConnection.DisposeAsync();
        await _myPgsqlConnection.DisposeAsync();
    }

    [Benchmark(Description = "Npgsql: SELECT all from data")]
    public async Task<int> Npgsql_SelectAllData()
    {
        await using var cmd = new NpgsqlCommand("SELECT id, name, option, flag, create_at FROM data", _npgsqlConnection);
        //await using var cmd = new NpgsqlCommand("SELECT * FROM device", _npgsqlConnection);
        await using var reader = await cmd.ExecuteReaderAsync();

        var count = 0;
        while (await reader.ReadAsync())
        {
            var id = reader.GetInt32(0);
            var name = reader.GetString(1);
            var option = reader.IsDBNull(2) ? null : reader.GetString(2);
            var flag = reader.GetBoolean(3);
            var createAt = reader.GetDateTime(4);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "MyPgsql: SELECT all from data")]
    public async Task<int> MyPgsql_SelectAllData()
    {
        await using var cmd = _myPgsqlConnection.CreateCommand();
        cmd.CommandText = "SELECT id, name, option, flag, create_at FROM data";
        await using var reader = await cmd.ExecuteReaderAsync();

        var count = 0;
        while (await reader.ReadAsync())
        {
            var id = reader.GetInt32(0);
            var name = reader.GetString(1);
            var option = reader.IsDBNull(2) ? null : reader.GetString(2);
            var flag = reader.GetBoolean(3);
            var createAt = reader.GetDateTime(4);
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
    //        _npgsqlConnection))
    //    {
    //        cmd.Parameters.AddWithValue("@id", id);
    //        cmd.Parameters.AddWithValue("@name", "Benchmark User");
    //        cmd.Parameters.AddWithValue("@email", "benchmark@example.com");
    //        cmd.Parameters.AddWithValue("@created_at", DateTime.UtcNow);
    //        await cmd.ExecuteNonQueryAsync();
    //    }

    //    // DELETE
    //    await using (var cmd = new NpgsqlCommand("DELETE FROM users WHERE id = @id", _npgsqlConnection))
    //    {
    //        cmd.Parameters.AddWithValue("@id", id);
    //        await cmd.ExecuteNonQueryAsync();
    //    }
    //}
}
