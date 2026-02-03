namespace MyPgsql;

using System.Collections.Frozen;

internal static class PgTypes
{
    // PostgreSQL OID constants
    public const int OidBool = 16;
    public const int OidBytea = 17;
    public const int OidInt8 = 20;
    public const int OidInt2 = 21;
    public const int OidInt4 = 23;
    public const int OidText = 25;
    public const int OidOid = 26;
    public const int OidFloat4 = 700;
    public const int OidFloat8 = 701;
    public const int OidVarchar = 1043;
    public const int OidChar = 1042;
    public const int OidDate = 1082;
    public const int OidTimestamp = 1114;
    public const int OidTimestampTz = 1184;
    public const int OidNumeric = 1700;
    public const int OidUuid = 2950;

    // PostgreSQL format codes
    public const short FormatText = 0;
    public const short FormatBinary = 1;

    /// <summary>
    /// PostgreSQL epoch (2000-01-01 00:00:00 UTC).
    /// </summary>
    public static readonly DateTime PostgresEpoch = new(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    // OID to Type mapping
    public static readonly FrozenDictionary<int, Type> OidToTypeMap = new Dictionary<int, Type>
    {
        [OidBool] = typeof(bool),
        [OidBytea] = typeof(byte[]),
        [OidInt8] = typeof(long),
        [OidInt2] = typeof(short),
        [OidInt4] = typeof(int),
        [OidText] = typeof(string),
        [OidOid] = typeof(int),
        [OidFloat4] = typeof(float),
        [OidFloat8] = typeof(double),
        [OidVarchar] = typeof(string),
        [OidChar] = typeof(string),
        [OidDate] = typeof(DateTime),
        [OidTimestamp] = typeof(DateTime),
        [OidTimestampTz] = typeof(DateTime),
        [OidNumeric] = typeof(decimal),
        [OidUuid] = typeof(Guid)
    }.ToFrozenDictionary();

    // OID to TypeName mapping
    public static readonly FrozenDictionary<int, string> OidToTypeNameMap = new Dictionary<int, string>
    {
        [OidBool] = "boolean",
        [OidBytea] = "bytea",
        [OidInt8] = "bigint",
        [OidInt2] = "smallint",
        [OidInt4] = "integer",
        [OidText] = "text",
        [OidOid] = "oid",
        [OidFloat4] = "real",
        [OidFloat8] = "double precision",
        [OidVarchar] = "character varying",
        [OidChar] = "character",
        [OidDate] = "date",
        [OidTimestamp] = "timestamp without time zone",
        [OidTimestampTz] = "timestamp with time zone",
        [OidNumeric] = "numeric",
        [OidUuid] = "uuid"
    }.ToFrozenDictionary();
}
