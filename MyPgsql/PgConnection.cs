namespace MyPgsql;

using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

public sealed class PgConnection : DbConnection
{
    private PgConnectionStringBuilder connectionStringBuilder = new();

    // ReSharper disable once ReplaceWithFieldKeyword
    private string connectionString = string.Empty;

    private ConnectionState state = ConnectionState.Closed;

    private PgProtocolHandler? protocol;

#pragma warning disable CA2213
    private PgTransaction? currentTransaction;
#pragma warning restore CA2213

    [AllowNull]
    public override string ConnectionString
    {
        get => connectionString;
        set
        {
            connectionString = value ?? string.Empty;
            connectionStringBuilder = new PgConnectionStringBuilder(connectionString);
        }
    }

    public override string Database => connectionStringBuilder.Database;

    public override string DataSource => $"{connectionStringBuilder.Host}:{connectionStringBuilder.Port}";

    public override string ServerVersion => "PostgreSQL";

    public override ConnectionState State => state;

    internal PgProtocolHandler Protocol => protocol ?? throw new InvalidOperationException("Connection is not open.");

    internal PgTransaction? CurrentTransaction => currentTransaction;

    public PgConnection()
    {
    }

    public PgConnection(string connectionString)
    {
        ConnectionString = connectionString;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close();
        }
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        await CloseAsync().ConfigureAwait(false);
        await base.DisposeAsync().ConfigureAwait(false);
    }

    public override void Open()
    {
        OpenAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        if (state == ConnectionState.Open)
        {
            return;
        }

        state = ConnectionState.Connecting;
        try
        {
            protocol = new PgProtocolHandler();
            await protocol.ConnectAsync(
                connectionStringBuilder.Host,
                connectionStringBuilder.Port,
                connectionStringBuilder.Database,
                connectionStringBuilder.Username,
                connectionStringBuilder.Password,
                cancellationToken).ConfigureAwait(false);

            state = ConnectionState.Open;
        }
        catch
        {
            state = ConnectionState.Closed;
#pragma warning disable CA1849
            // ReSharper disable once MethodHasAsyncOverload
            protocol?.Dispose();
#pragma warning restore CA1849
            protocol = null;
            throw;
        }
    }

    public override void Close()
    {
        CloseAsync().GetAwaiter().GetResult();
    }

    public override async Task CloseAsync()
    {
        if (state == ConnectionState.Closed)
        {
            return;
        }

        if (protocol is not null)
        {
            await protocol.DisposeAsync().ConfigureAwait(false);
            protocol = null;
        }

        currentTransaction = null;
        state = ConnectionState.Closed;
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        return BeginTransactionAsync(isolationLevel, CancellationToken.None).GetAwaiter().GetResult();
    }

    public new Task<PgTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        return BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
    }

    public new async Task<PgTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
    {
        if (state != ConnectionState.Open)
        {
            throw new InvalidOperationException("Connection is not open.");
        }

        if (currentTransaction is not null)
        {
            throw new InvalidOperationException("Transaction is already started.");
        }

        var isolationLevelSql = isolationLevel switch
        {
            IsolationLevel.ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel.ReadCommitted => "READ COMMITTED",
            IsolationLevel.RepeatableRead => "REPEATABLE READ",
            IsolationLevel.Serializable => "SERIALIZABLE",
            _ => "READ COMMITTED"
        };

        await Protocol.ExecuteSimpleQueryAsync($"BEGIN ISOLATION LEVEL {isolationLevelSql}", cancellationToken).ConfigureAwait(false);
        currentTransaction = new PgTransaction(this, isolationLevel);
        return currentTransaction;
    }

    internal void ClearTransaction()
    {
        currentTransaction = null;
    }

    public new PgCommand CreateCommand()
    {
        return new PgCommand { Connection = this };
    }

    protected override DbCommand CreateDbCommand()
    {
        return CreateCommand();
    }

    public override void ChangeDatabase(string databaseName)
    {
        throw new NotSupportedException("ChangeDatabase is not supported.");
    }
}
