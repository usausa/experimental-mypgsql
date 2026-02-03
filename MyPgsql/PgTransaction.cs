namespace MyPgsql;

using System.Data;
using System.Data.Common;

public sealed class PgTransaction : DbTransaction
{
    private bool completed;

    public new PgConnection Connection { get; }

    protected override DbConnection DbConnection => Connection;

    public override IsolationLevel IsolationLevel { get; }

    internal PgTransaction(PgConnection connection, IsolationLevel isolationLevel)
    {
        Connection = connection;
        IsolationLevel = isolationLevel;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !completed)
        {
#pragma warning disable CA1031
            try
            {
                Rollback();
            }
            catch
            {
                // Ignore
            }
#pragma warning restore CA1031
        }
        base.Dispose(disposing);
    }

    public override void Commit()
    {
        CommitAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        if (completed)
        {
            throw new InvalidOperationException("Transaction already completed.");
        }

        await Connection.Protocol.ExecuteSimpleQueryAsync("COMMIT", cancellationToken).ConfigureAwait(false);
        completed = true;
        Connection.ClearTransaction();
    }

    public override void Rollback()
    {
        RollbackAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        if (completed)
        {
            throw new InvalidOperationException("Transaction already completed.");
        }

        await Connection.Protocol.ExecuteSimpleQueryAsync("ROLLBACK", cancellationToken).ConfigureAwait(false);
        completed = true;
        Connection.ClearTransaction();
    }
}
