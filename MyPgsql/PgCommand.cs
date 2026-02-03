namespace MyPgsql;

using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

#pragma warning disable CA2100
public sealed class PgCommand : DbCommand
{
    [AllowNull]
    public override string CommandText { get; set; } = string.Empty;

    public override int CommandTimeout { get; set; } = 30;

    public override CommandType CommandType { get; set; } = CommandType.Text;

    public override bool DesignTimeVisible { get; set; }

    public override UpdateRowSource UpdatedRowSource { get; set; }

    public new PgConnection? Connection { get; set; }

    protected override DbConnection? DbConnection
    {
        get => Connection;
        set => Connection = value as PgConnection;
    }

    public new PgTransaction? Transaction { get; set; }

    protected override DbTransaction? DbTransaction
    {
        get => Transaction;
        set => Transaction = value as PgTransaction;
    }

    public new PgParameterCollection Parameters { get; } = new();

    protected override DbParameterCollection DbParameterCollection => Parameters;

    public PgCommand()
    {
    }

    public PgCommand(string commandText)
    {
        CommandText = commandText;
    }

    public PgCommand(string commandText, PgConnection connection)
    {
        CommandText = commandText;
        Connection = connection;
    }

    public override void Cancel()
    {
    }

    public override int ExecuteNonQuery()
    {
        return ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        ValidateCommand();

        if (Parameters.Count == 0)
        {
            return Connection!.Protocol.ExecuteNonQueryAsync(CommandText, cancellationToken);
        }
        return Connection!.Protocol.ExecuteNonQueryWithParametersAsync(CommandText, Parameters.GetParametersInternal(), cancellationToken);
    }

    public override object? ExecuteScalar()
    {
        return ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
#pragma warning disable CA2007
        await using var reader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false);
#pragma warning restore CA2007
        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
#pragma warning disable CA1849
            return reader.IsDBNull(0) ? null : reader.GetValue(0);
#pragma warning restore CA1849
        }
        return null;
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        return ExecuteDbDataReaderAsync(behavior, CancellationToken.None).GetAwaiter().GetResult();
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        ValidateCommand();

        if (Parameters.Count == 0)
        {
            await Connection!.Protocol.SendExtendedQueryAsync(CommandText, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await Connection!.Protocol.SendExtendedQueryWithParametersAsync(CommandText, Parameters.GetParametersInternal(), cancellationToken).ConfigureAwait(false);
        }

        return new PgDataReader(Connection!.Protocol, Connection, behavior, cancellationToken);
    }

    public override void Prepare()
    {
    }

    protected override DbParameter CreateDbParameter()
    {
        return new PgParameter();
    }

    private void ValidateCommand()
    {
        if (Connection is null)
        {
            throw new InvalidOperationException("Connection is not set");
        }
        if (Connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException("Connection is not open");
        }
        if (String.IsNullOrEmpty(CommandText))
        {
            throw new InvalidOperationException("CommandText is not set");
        }
    }
}
#pragma warning restore CA2100
