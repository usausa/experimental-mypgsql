namespace MyPgsql;

using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

public sealed class PgParameter : DbParameter
{
    //--------------------------------------------------------------------------------
    // Properties
    //--------------------------------------------------------------------------------

    public override DbType DbType { get; set; } = DbType.String;

    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;

    public override bool IsNullable { get; set; } = true;

    [AllowNull]
    public override string ParameterName { get; set; } = string.Empty;

    public override int Size { get; set; }

    [AllowNull]
    public override string SourceColumn { get; set; } = string.Empty;

    public override bool SourceColumnNullMapping { get; set; }

    public override object? Value { get; set; }

    //--------------------------------------------------------------------------------
    // Constructors
    //--------------------------------------------------------------------------------

    public PgParameter()
    {
    }

    public PgParameter(string parameterName, DbType dbType)
    {
        ParameterName = parameterName;
        DbType = dbType;
    }

    public PgParameter(string parameterName, object? value)
    {
        ParameterName = parameterName;
        Value = value;
        DbType = InferDbType(value);
    }

    //--------------------------------------------------------------------------------
    // Helpers
    //--------------------------------------------------------------------------------

    private static DbType InferDbType(object? value)
    {
        return value switch
        {
            null => DbType.Object,
            DBNull => DbType.Object,
            short => DbType.Int16,
            int => DbType.Int32,
            long => DbType.Int64,
            float => DbType.Single,
            double => DbType.Double,
            decimal => DbType.Decimal,
            bool => DbType.Boolean,
            DateTime => DbType.DateTime,
            DateTimeOffset => DbType.DateTimeOffset,
            DateOnly => DbType.Date,
            Guid => DbType.Guid,
            byte[] => DbType.Binary,
            string => DbType.String,
            _ => DbType.String
        };
    }

    //--------------------------------------------------------------------------------
    // Overrides
    //--------------------------------------------------------------------------------

    public override void ResetDbType()
    {
        DbType = DbType.String;
    }
}
