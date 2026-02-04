namespace MyPgsql;

using System.Collections;
using System.Data;
using System.Data.Common;

#pragma warning disable CA1010
public sealed class PgParameterCollection : DbParameterCollection
{
    private readonly List<PgParameter> parameters = [];

    //--------------------------------------------------------------------------------
    // Properties
    //--------------------------------------------------------------------------------

    public override object SyncRoot { get; } = new();

    public override int Count => parameters.Count;

    //--------------------------------------------------------------------------------
    // Collection methods
    //--------------------------------------------------------------------------------

    public override int Add(object value)
    {
        parameters.Add((PgParameter)value);
        return parameters.Count - 1;
    }

    public PgParameter Add(PgParameter parameter)
    {
        parameters.Add(parameter);
        return parameter;
    }

    public override void AddRange(Array values)
    {
        foreach (PgParameter param in values)
        {
            parameters.Add(param);
        }
    }

    public override void Clear()
    {
        parameters.Clear();
    }

    public override bool Contains(object value)
    {
        return parameters.Contains((PgParameter)value);
    }

    public override bool Contains(string value)
    {
        return parameters.Exists(x => x.ParameterName == value);
    }

    public override void CopyTo(Array array, int index)
    {
        ((ICollection)parameters).CopyTo(array, index);
    }

    public override IEnumerator GetEnumerator()
    {
        return parameters.GetEnumerator();
    }

    public override int IndexOf(object value)
    {
        return parameters.IndexOf((PgParameter)value);
    }

    public override int IndexOf(string parameterName)
    {
        return parameters.FindIndex(x => x.ParameterName == parameterName);
    }

    public override void Insert(int index, object value)
    {
        parameters.Insert(index, (PgParameter)value);
    }

    public override void Remove(object value)
    {
        parameters.Remove((PgParameter)value);
    }

    public override void RemoveAt(int index)
    {
        parameters.RemoveAt(index);
    }

    public override void RemoveAt(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index >= 0)
        {
            parameters.RemoveAt(index);
        }
    }

    //--------------------------------------------------------------------------------
    // Parameter accessors
    //--------------------------------------------------------------------------------

    protected override DbParameter GetParameter(int index)
    {
        return parameters[index];
    }

    protected override DbParameter GetParameter(string parameterName)
    {
        return parameters.Find(x => x.ParameterName == parameterName) ?? throw new ArgumentException($"Parameter '{parameterName}' not found");
    }

    protected override void SetParameter(int index, DbParameter value)
    {
        parameters[index] = (PgParameter)value;
    }

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        var index = IndexOf(parameterName);
        if (index >= 0)
        {
            parameters[index] = (PgParameter)value;
        }
        else
        {
            parameters.Add((PgParameter)value);
        }
    }

    //--------------------------------------------------------------------------------
    // Helpers
    //--------------------------------------------------------------------------------

    public PgParameter AddWithValue(string parameterName, object value)
        => Add(new PgParameter(parameterName, value));

    public PgParameter AddWithValue(string parameterName, DbType parameterType, object value)
        => Add(new PgParameter(parameterName, parameterType) { Value = value });

    public PgParameter AddWithValue(string parameterName, DbType parameterType, int size, object value)
        => Add(new PgParameter(parameterName, parameterType) { Value = value, Size = size });

    public PgParameter AddWithValue(string parameterName, DbType parameterType, int size, string? sourceColumn, object value)
        => Add(new PgParameter(parameterName, parameterType) { Value = value, Size = size, SourceColumn = sourceColumn });

    public PgParameter AddWithValue(object value)
        => Add(new PgParameter { Value = value });

    public PgParameter AddWithValue(DbType parameterType, object value)
        => Add(new PgParameter { DbType = parameterType, Value = value });

    public PgParameter Add(string parameterName, DbType parameterType)
        => Add(new PgParameter(parameterName, parameterType));

    public PgParameter Add(string parameterName, DbType parameterType, int size)
        => Add(new PgParameter(parameterName, parameterType) { Size = size });

    public PgParameter Add(string parameterName, DbType parameterType, int size, string sourceColumn)
        => Add(new PgParameter(parameterName, parameterType) { Size = size, SourceColumn = sourceColumn });

    //--------------------------------------------------------------------------------
    // Internal Methods
    //--------------------------------------------------------------------------------

    internal IReadOnlyList<PgParameter> GetParametersInternal() => parameters;
}
#pragma warning restore CA1010
