namespace MyPgsql;

using System.Data;
using System.Data.Common;

public sealed class PgTransaction : DbTransaction
{
    private readonly PgConnection _connection;
    private readonly IsolationLevel _isolationLevel;
    private bool _completed;

    internal PgTransaction(PgConnection connection, IsolationLevel isolationLevel)
    {
        _connection = connection;
        _isolationLevel = isolationLevel;
    }

    public new PgConnection Connection => _connection;
    protected override DbConnection DbConnection => _connection;
    public override IsolationLevel IsolationLevel => _isolationLevel;

    public override void Commit()
    {
        CommitAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        if (_completed)
            throw new InvalidOperationException("トランザクションは既に完了しています");

        await _connection.Protocol.ExecuteSimpleQueryAsync("COMMIT", cancellationToken);
        _completed = true;
        _connection.ClearTransaction();
    }

    public override void Rollback()
    {
        RollbackAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        if (_completed)
            throw new InvalidOperationException("トランザクションは既に完了しています");

        await _connection.Protocol.ExecuteSimpleQueryAsync("ROLLBACK", cancellationToken);
        _completed = true;
        _connection.ClearTransaction();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_completed)
        {
            try
            {
                Rollback();
            }
            catch
            {
                // 無視
            }
        }
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        if (!_completed)
        {
            try
            {
                await RollbackAsync();
            }
            catch
            {
                // 無視
            }
        }
        GC.SuppressFinalize(this);
    }
}
