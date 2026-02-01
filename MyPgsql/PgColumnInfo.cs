namespace MyPgsql;

internal readonly record struct PgColumnInfo(string Name, int TypeOid, short FormatCode);
