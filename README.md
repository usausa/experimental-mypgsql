# Experimental Project

AI-generated PostgreSQL protocol client implementation.

# Benchmark

```
BenchmarkDotNet v0.15.8, Windows 11 (10.0.26200.7705/25H2/2025Update/HudsonValley2)
AMD Ryzen 9 5900X 3.70GHz, 1 CPU, 24 logical and 12 physical cores
.NET SDK 10.0.102
  [Host]     : .NET 10.0.2 (10.0.2, 10.0.225.61305), X64 RyuJIT x86-64-v3
  DefaultJob : .NET 10.0.2 (10.0.2, 10.0.225.61305), X64 RyuJIT x86-64-v3
```
| Method                          | Mean     | Error   | StdDev  | Allocated |
|-------------------------------- |---------:|--------:|--------:|----------:|
| &#39;Npgsql: SELECT all from data&#39;  | 461.0 ms | 0.80 ms | 0.63 ms |   7.23 MB |
| &#39;MyPgsql: SELECT all from data&#39; | 460.9 ms | 0.20 ms | 0.18 ms |    7.2 MB |
