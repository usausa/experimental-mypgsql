namespace MyPgsql;

using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

public sealed class PgConnection : DbConnection
{
    private PgConnectionStringBuilder _connectionStringBuilder = new();
    private string _connectionString = "";
    private ConnectionState _state = ConnectionState.Closed;
    private PgProtocolHandler? _protocol;
    private PgTransaction? _currentTransaction;

    public PgConnection()
    {
    }

    public PgConnection(string connectionString)
    {
        ConnectionString = connectionString;
    }

    [AllowNull]
    public override string ConnectionString
    {
        get => _connectionString;
        set
        {
            _connectionString = value ?? string.Empty;
            _connectionStringBuilder = new PgConnectionStringBuilder(_connectionString);
        }
    }

    public override string Database => _connectionStringBuilder.Database;
    public override string DataSource => $"{_connectionStringBuilder.Host}:{_connectionStringBuilder.Port}";
    public override string ServerVersion => "PostgreSQL";
    public override ConnectionState State => _state;

    internal PgProtocolHandler Protocol => _protocol ?? throw new InvalidOperationException("接続が開かれていません");
    internal PgTransaction? CurrentTransaction => _currentTransaction;

    public override void Open()
    {
        OpenAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        if (_state == ConnectionState.Open)
        {
            return;
        }

        _state = ConnectionState.Connecting;
        try
        {
            _protocol = new PgProtocolHandler();
            await _protocol.ConnectAsync(
                _connectionStringBuilder.Host,
                _connectionStringBuilder.Port,
                _connectionStringBuilder.Database,
                _connectionStringBuilder.Username,
                _connectionStringBuilder.Password,
                cancellationToken).ConfigureAwait(false);

            _state = ConnectionState.Open;
        }
        catch
        {
            _state = ConnectionState.Closed;
#pragma warning disable CA1849
            // ReSharper disable once MethodHasAsyncOverload
            _protocol?.Dispose();
#pragma warning restore CA1849
            _protocol = null;
            throw;
        }
    }

    public override void Close()
    {
        CloseAsync().GetAwaiter().GetResult();
    }

    public override async Task CloseAsync()
    {
        if (_state == ConnectionState.Closed)
        {
            return;
        }

        if (_protocol is not null)
        {
            await _protocol.DisposeAsync().ConfigureAwait(false);
            _protocol = null;
        }

        _currentTransaction = null;
        _state = ConnectionState.Closed;
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        return BeginTransactionAsync(isolationLevel, CancellationToken.None).GetAwaiter().GetResult();
    }

    public new async Task<PgTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        return await BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
    }

    public async Task<PgTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
    {
        if (_state != ConnectionState.Open)
            throw new InvalidOperationException("接続が開かれていません");

        if (_currentTransaction is not null)
            throw new InvalidOperationException("既にトランザクションが開始されています");

        var isolationLevelSql = isolationLevel switch
        {
            IsolationLevel.ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel.ReadCommitted => "READ COMMITTED",
            IsolationLevel.RepeatableRead => "REPEATABLE READ",
            IsolationLevel.Serializable => "SERIALIZABLE",
            _ => "READ COMMITTED"
        };

        await Protocol.ExecuteSimpleQueryAsync($"BEGIN ISOLATION LEVEL {isolationLevelSql}", cancellationToken);
        _currentTransaction = new PgTransaction(this, isolationLevel);
        return _currentTransaction;
    }

    internal void ClearTransaction()
    {
        _currentTransaction = null;
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
        throw new NotSupportedException("データベースの変更はサポートされていません");
    }

    public override async ValueTask DisposeAsync()
    {
        await CloseAsync().ConfigureAwait(false);
        await base.DisposeAsync().ConfigureAwait(false);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close();
        }
        base.Dispose(disposing);
    }
}
