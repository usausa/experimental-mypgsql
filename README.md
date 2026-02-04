# Experimental Project

AI-generated PostgreSQL protocol client implementation.

# Benchmark

```
BenchmarkDotNet v0.15.8, Windows 11 (10.0.26200.7705/25H2/2025Update/HudsonValley2)
AMD Ryzen 9 5900X 3.70GHz, 1 CPU, 24 logical and 12 physical cores
.NET SDK 10.0.102
  [Host]    : .NET 10.0.2 (10.0.2, 10.0.225.61305), X64 RyuJIT x86-64-v3
  MediumRun : .NET 10.0.2 (10.0.2, 10.0.225.61305), X64 RyuJIT x86-64-v3

Job=MediumRun  IterationCount=15  LaunchCount=2  
WarmupCount=10  
```
| Method                            | Mean       | Error     | StdDev    | Min         | Max        | P90        | Allocated  |
|---------------------------------- |-----------:|----------:|----------:|------------:|-----------:|-----------:|-----------:|
| &#39;Npgsql: SELECT all from data&#39;    | 462.620 ms | 0.2967 ms | 0.4159 ms | 462.1837 ms | 464.103 ms | 463.019 ms |  7348.7 KB |
| &#39;MyPgsql: SELECT all from data&#39;   | 462.097 ms | 1.5093 ms | 1.9625 ms | 460.5118 ms | 467.152 ms | 464.983 ms | 7389.68 KB |
| &#39;Npgsql: INSERT and DELETE user&#39;  |   1.077 ms | 0.0825 ms | 0.1183 ms |   0.9766 ms |   1.360 ms |   1.316 ms |    4.66 KB |
| &#39;MyPgsql: INSERT and DELETE user&#39; |   1.075 ms | 0.0524 ms | 0.0768 ms |   0.9662 ms |   1.285 ms |   1.183 ms |    5.55 KB |
