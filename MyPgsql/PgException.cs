namespace MyPgsql;

public sealed class PgException(string message) : Exception(message);
