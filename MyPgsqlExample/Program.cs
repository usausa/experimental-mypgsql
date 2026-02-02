namespace MyPgsqlExample;

using System.Data;

using MyPgsql;

internal static class Program
{
    public static async Task Main()
    {
        const string connectionString = "Host=mysql-server;Port=5432;Database=test;Username=test;Password=test";

#pragma warning disable CA1031
        try
        {
            await using var connection = new PgConnection(connectionString);
            await connection.OpenAsync();
            Console.WriteLine("接続成功！\n");

            // === 1. INSERT ===
            Console.WriteLine("=== INSERT ===");
            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO users (id, name, email, created_at) VALUES (@id, @name, @email, @created_at)";
                cmd.Parameters.Add(new PgParameter("@id", DbType.Int32) { Value = 2001 });
                cmd.Parameters.Add(new PgParameter("@name", DbType.String) { Value = "ADO.NET User" });
                cmd.Parameters.Add(new PgParameter("@email", DbType.String) { Value = "adonet@example.com" });
                cmd.Parameters.Add(new PgParameter("@created_at", DbType.DateTime) { Value = DateTime.Now });

                var inserted = await cmd.ExecuteNonQueryAsync();
                Console.WriteLine($"挿入: {inserted} 行\n");
            }

            // === 2. SELECT (単一値) ===
            Console.WriteLine("=== SELECT (ExecuteScalar) ===");
            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SELECT COUNT(*) FROM users";
                var count = await cmd.ExecuteScalarAsync();
                Console.WriteLine($"ユーザー数: {count}\n");
            }

            // === 3. SELECT (DataReader) ===
            Console.WriteLine("=== SELECT (DataReader) ===");
            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SELECT id, name, email, created_at FROM users WHERE id = @id";
                cmd.Parameters.Add(new PgParameter("@id", DbType.Int32) { Value = 2001 });

                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    Console.WriteLine($"ID: {reader.GetInt32(0)}, Name: {reader.GetString(1)}, Email: {reader.GetString(2)}, Created: {reader.GetDateTime(3)}\n");
                }
            }

            // === 4. UPDATE ===
            Console.WriteLine("=== UPDATE ===");
            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "UPDATE users SET name = @name, email = @email WHERE id = @id";
                cmd.Parameters.Add(new PgParameter("@id", DbType.Int32) { Value = 2001 });
                cmd.Parameters.Add(new PgParameter("@name", DbType.String) { Value = "Updated ADO.NET User" });
                cmd.Parameters.Add(new PgParameter("@email", DbType.String) { Value = "updated.adonet@example.com" });

                var updated = await cmd.ExecuteNonQueryAsync();
                Console.WriteLine($"更新: {updated} 行\n");
            }

            // === 5. Transaction Demo ===
            Console.WriteLine("=== TRANSACTION ===");
            await using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    await using (var cmd = connection.CreateCommand())
                    {
                        cmd.Transaction = transaction;
                        cmd.CommandText = "INSERT INTO users (id, name, email, created_at) VALUES (@id, @name, @email, @created_at)";
                        cmd.Parameters.Add(new PgParameter("@id", DbType.Int32) { Value = 2002 });
                        cmd.Parameters.Add(new PgParameter("@name", DbType.String) { Value = "Transaction User" });
                        cmd.Parameters.Add(new PgParameter("@email", DbType.String) { Value = "tx@example.com" });
                        cmd.Parameters.Add(new PgParameter("@created_at", DbType.DateTime) { Value = DateTime.Now });
                        await cmd.ExecuteNonQueryAsync();
                    }

                    await transaction.CommitAsync();
                    Console.WriteLine("トランザクションコミット成功\n");
                }
                catch
                {
                    await transaction.RollbackAsync();
                    Console.WriteLine("トランザクションロールバック\n");
                    throw;
                }
            }

            // === 6. SELECT (全件確認) ===
            Console.WriteLine("=== SELECT (全件確認) ===");
            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SELECT id, name, email FROM users ORDER BY id";
                await using var reader = await cmd.ExecuteReaderAsync();

                Console.WriteLine($"{"ID",-10} {"Name",-25} {"Email",-30}");
                Console.WriteLine(new string('-', 65));

                while (await reader.ReadAsync())
                {
                    Console.WriteLine($"{reader.GetInt32(0),-10} {reader.GetString(1),-25} {reader.GetString(2),-30}");
                }
                Console.WriteLine();
            }

            // === 7. GetValue/GetValues/GetDataTypeName/GetFieldType テスト ===
            Console.WriteLine("=== GetValue/GetValues/GetDataTypeName/GetFieldType テスト ===");
            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SELECT id, name, email, created_at FROM users WHERE id = @id";
                cmd.Parameters.Add(new PgParameter("@id", DbType.Int32) { Value = 2001 });

                await using var reader = await cmd.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                {
                    // カラム情報の確認
                    Console.WriteLine("カラム情報:");
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        Console.WriteLine($"  [{i}] {reader.GetName(i)}: DataTypeName={reader.GetDataTypeName(i)}, FieldType={reader.GetFieldType(i).Name}");
                    }
                    Console.WriteLine();

                    // GetValue で型を確認
                    Console.WriteLine("GetValue による取得:");
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        var value = reader.GetValue(i);
                        Console.WriteLine($"  [{i}] {reader.GetName(i)}: Value={value}, Type={value.GetType().Name}");
                    }
                    Console.WriteLine();

                    // GetValues で一括取得
                    Console.WriteLine("GetValues による一括取得:");
                    var values = new object[reader.FieldCount];
                    reader.GetValues(values);
                    for (var i = 0; i < values.Length; i++)
                    {
                        Console.WriteLine($"  [{i}] {reader.GetName(i)}: Value={values[i]}, Type={values[i].GetType().Name}");
                    }
                    Console.WriteLine();
                }
            }

            // === 8. DELETE (クリーンアップ) ===
            Console.WriteLine("=== DELETE (クリーンアップ) ===");
            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "DELETE FROM users WHERE id IN (@id1, @id2)";
                cmd.Parameters.Add(new PgParameter("@id1", DbType.Int32) { Value = 2001 });
                cmd.Parameters.Add(new PgParameter("@id2", DbType.Int32) { Value = 2002 });

                var deleted = await cmd.ExecuteNonQueryAsync();
                Console.WriteLine($"削除: {deleted} 行\n");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"エラー: {ex.Message}");
            Console.WriteLine(ex.StackTrace);
        }
#pragma warning restore CA1031
    }
}
