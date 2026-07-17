# PollyRabbitMQ

[![NuGet](https://img.shields.io/nuget/v/PollyRabbitMQ.svg)](https://www.nuget.org/packages/PollyRabbitMQ)
[![NuGet Downloads](https://img.shields.io/nuget/dt/PollyRabbitMQ.svg)](https://www.nuget.org/packages/PollyRabbitMQ)
[![CI](https://github.com/Swevo/PollyRabbitMQ/actions/workflows/build.yml/badge.svg)](https://github.com/Swevo/PollyRabbitMQ/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Polly v8 resilience for RabbitMQ.Client v7+** — retry, circuit-breaker, and timeout for `IChannel` operations, with a built-in `RabbitMqTransientErrors` predicate covering the most common RabbitMQ transient exceptions. Includes automatic channel recreation between retries so `AlreadyClosedException` never kills your publisher.

```csharp
// Before — crashes on broker restart
await channel.BasicPublishAsync("orders", "order.placed", body: payload);

// After — retry + channel recreation + circuit breaker
var resilient = await connection.CreateResilientChannelAsync(options =>
{
    options.MaxRetries = 3;
    options.RecreateChannelOnFailure = true; // rebuild channel between retries ✔
});

await resilient.BasicPublishAsync("orders", "order.placed", body: payload);
```

---

## Installation

```bash
dotnet add package PollyRabbitMQ
```

Targets **net6.0**, **net8.0**, and **net9.0**.
Dependencies: `Polly.Core 8.*`, `RabbitMQ.Client 7.*`, `Microsoft.Extensions.DependencyInjection.Abstractions 8.*`

---

## RabbitMqTransientErrors — the key feature

`PollyRabbitMQ` ships `RabbitMqTransientErrors.IsTransient` — a pre-built `PredicateBuilder` covering the four most common transient RabbitMQ exceptions:

```csharp
new RetryStrategyOptions
{
    MaxRetryAttempts = 3,
    ShouldHandle = RabbitMqTransientErrors.IsTransient, // built-in ✔
}
```

### Covered exceptions

| Exception | When it occurs |
|-----------|----------------|
| `AlreadyClosedException` | Channel or connection closed by the broker (most common) |
| `OperationInterruptedException` | Operation interrupted mid-flight (e.g. during broker restart) |
| `BrokerUnreachableException` | Cannot reach the broker — all endpoints tried |
| `ConnectFailureException` | TCP connect to a broker endpoint failed |

> **The critical one:** `AlreadyClosedException` is thrown when the broker closes a channel while your app is publishing. Without retry, this crashes your publisher. With `PollyRabbitMQ` and `RecreateChannelOnFailure = true`, the channel is rebuilt transparently before each retry.

---

## Quick start

### Option 1 — Options-based (recommended)

```csharp
using PollyRabbitMQ;

var factory = new ConnectionFactory { HostName = "localhost" };
await using var connection = await factory.CreateConnectionAsync();

await using var resilient = await connection.CreateResilientChannelAsync(options =>
{
    options.MaxRetries              = 3;
    options.BaseDelay               = TimeSpan.FromMilliseconds(500);
    options.MaxDelay                = TimeSpan.FromSeconds(30);
    options.OperationTimeout        = TimeSpan.FromSeconds(15);
    options.RecreateChannelOnFailure = true;
    options.CircuitBreakerFailureRatio      = 0.5;
    options.CircuitBreakerMinimumThroughput = 10;
    options.CircuitBreakerBreakDuration     = TimeSpan.FromSeconds(5);
});

// Publish
await resilient.BasicPublishAsync("orders", "order.placed", body: payload);

// Consume single message
var message = await resilient.BasicGetAsync("order-queue", autoAck: false);
if (message is not null)
{
    // Process...
    await resilient.BasicAckAsync(message.DeliveryTag);
}

// Topology
await resilient.QueueDeclareAsync("order-queue", durable: true);
await resilient.ExchangeDeclareAsync("orders", ExchangeType.Direct, durable: true);
```

### Option 2 — Dependency injection

```csharp
// Program.cs
builder.Services.AddResilientRabbitMqChannel(
    configureFactory: factory =>
    {
        factory.HostName = "localhost";
        factory.UserName = "guest";
        factory.Password = "guest";
    },
    configureOptions: options =>
    {
        options.MaxRetries = 3;
        options.RecreateChannelOnFailure = true;
    });

// Publisher
public class OrderPublisher(ResilientChannel channel)
{
    public Task PublishAsync(ReadOnlyMemory<byte> payload, CancellationToken ct = default) =>
        channel.BasicPublishAsync("orders", "order.placed", payload, cancellationToken: ct);
}
```

---

## Supported operations

| Method | Description |
|--------|-------------|
| `BasicPublishAsync` | Publish a message to an exchange |
| `BasicGetAsync` | Poll a queue for one message |
| `BasicAckAsync` | Acknowledge a delivered message |
| `BasicNackAsync` | Negatively acknowledge (optionally requeue) |
| `QueueDeclareAsync` | Declare a queue |
| `ExchangeDeclareAsync` | Declare an exchange |

---

## How channel recreation works

When `RecreateChannelOnFailure = true` (default) and a transient error occurs, `ResilientChannel` will:

1. Detect the failure (catch `AlreadyClosedException` etc.)
2. Check if the current channel is closed
3. Create a **new** `IChannel` from the same connection before the next retry attempt
4. Transparently continue — your code doesn't change

This is critical for long-running publishers that survive broker restarts.

---

## Pipeline order

```
[Timeout] → [Retry + channel recreation] → [Circuit Breaker] → [RabbitMQ broker]
```

---

## Related Packages

| Package | Downloads | Description |
|---|---|---|
| [PollyHealthChecks](https://www.nuget.org/packages/PollyHealthChecks) | [![Downloads](https://img.shields.io/nuget/dt/PollyHealthChecks.svg)](https://www.nuget.org/packages/PollyHealthChecks) | ASP.NET Core health checks for Polly v8 circuit breakers — expose circuit-breaker state (Closed, HalfOpen, Open, Isolated) as /health endpoint responses |
| [PollyBackoff](https://www.nuget.org/packages/PollyBackoff) | [![Downloads](https://img.shields.io/nuget/dt/PollyBackoff.svg)](https://www.nuget.org/packages/PollyBackoff) | Backoff delay strategies for Polly v8 resilience pipelines |
| [PollyGrpc](https://www.nuget.org/packages/PollyGrpc) | [![Downloads](https://img.shields.io/nuget/dt/PollyGrpc.svg)](https://www.nuget.org/packages/PollyGrpc) | Polly v8 resilience interceptor for gRPC |
| [PollyEFCore](https://www.nuget.org/packages/PollyEFCore) | [![Downloads](https://img.shields.io/nuget/dt/PollyEFCore.svg)](https://www.nuget.org/packages/PollyEFCore) | Polly v8 resilience pipelines for Entity Framework Core — wrap every EF Core query and SaveChanges with retry, timeout and circuit-breaker via a single AddPollyResilience() call |
| [PollyMailKit](https://www.nuget.org/packages/PollyMailKit) | [![Downloads](https://img.shields.io/nuget/dt/PollyMailKit.svg)](https://www.nuget.org/packages/PollyMailKit) | Polly v8 resilience pipelines for MailKit — retry, timeout, and circuit-breaker for SmtpClient.SendAsync and any MailKit SMTP operation |
| [PollyMassTransit](https://www.nuget.org/packages/PollyMassTransit) | [![Downloads](https://img.shields.io/nuget/dt/PollyMassTransit.svg)](https://www.nuget.org/packages/PollyMassTransit) | Polly v8 resilience pipelines for MassTransit — retry, timeout, and circuit-breaker for IBus.Publish and ISendEndpointProvider.Send |
| [PollyNpgsql](https://www.nuget.org/packages/PollyNpgsql) | [![Downloads](https://img.shields.io/nuget/dt/PollyNpgsql.svg)](https://www.nuget.org/packages/PollyNpgsql) | Polly v8 resilience pipelines for Npgsql (PostgreSQL) — retry, timeout, and circuit-breaker for NpgsqlConnection queries and commands, plus a built-in PostgresTransientErrors predicate covering all common PostgreSQL transient SQLSTATE codes |
| [PollyOpenAI](https://www.nuget.org/packages/PollyOpenAI) | [![Downloads](https://img.shields.io/nuget/dt/PollyOpenAI.svg)](https://www.nuget.org/packages/PollyOpenAI) | Polly v8 resilience for OpenAI and Azure OpenAI API calls |
| [PollyAzureEventHub](https://www.nuget.org/packages/PollyAzureEventHub) | [![Downloads](https://img.shields.io/nuget/dt/PollyAzureEventHub.svg)](https://www.nuget.org/packages/PollyAzureEventHub) | Polly v8 resilience pipelines for Azure Event Hubs — retry, timeout, and circuit-breaker for EventHubProducerClient and EventHubConsumerClient |
| [PollyElasticsearch](https://www.nuget.org/packages/PollyElasticsearch) | [![Downloads](https://img.shields.io/nuget/dt/PollyElasticsearch.svg)](https://www.nuget.org/packages/PollyElasticsearch) | Polly v8 resilience pipelines for Elastic.Clients.Elasticsearch 8+ — retry, timeout, and circuit-breaker for any Elasticsearch operation, plus a built-in ElasticTransientErrors predicate covering rate limiting (429), service unavailability (503), gateway timeouts (504), and connection failures |
| [PollyHangfire](https://www.nuget.org/packages/PollyHangfire) | [![Downloads](https://img.shields.io/nuget/dt/PollyHangfire.svg)](https://www.nuget.org/packages/PollyHangfire) | Polly v8 resilience pipelines for Hangfire — retry, timeout, and circuit-breaker for IBackgroundJobClient.Enqueue and Schedule |
| [PollyCosmosDb](https://www.nuget.org/packages/PollyCosmosDb) | [![Downloads](https://img.shields.io/nuget/dt/PollyCosmosDb.svg)](https://www.nuget.org/packages/PollyCosmosDb) | Polly v8 resilience pipelines for Azure Cosmos DB — retry, timeout, and circuit-breaker for Container operations, plus a built-in CosmosTransientErrors predicate covering rate limiting (429), timeouts (408), partition failovers (410), and service unavailability (503) |
| [PollySendGrid](https://www.nuget.org/packages/PollySendGrid) | [![Downloads](https://img.shields.io/nuget/dt/PollySendGrid.svg)](https://www.nuget.org/packages/PollySendGrid) | Polly v8 resilience pipelines for SendGrid — retry, timeout, and circuit-breaker for ISendGridClient.SendEmailAsync |
| [PollyMongo](https://www.nuget.org/packages/PollyMongo) | [![Downloads](https://img.shields.io/nuget/dt/PollyMongo.svg)](https://www.nuget.org/packages/PollyMongo) | Polly v8 resilience pipelines for MongoDB.Driver — wrap Find, InsertOne, UpdateOne, DeleteOne and other IMongoCollection calls with retry, timeout, circuit-breaker, and more using a single ResilientMongoCollection decorator |
| [PollyDapper](https://www.nuget.org/packages/PollyDapper) | [![Downloads](https://img.shields.io/nuget/dt/PollyDapper.svg)](https://www.nuget.org/packages/PollyDapper) | Polly v8 resilience pipelines for Dapper — wrap QueryAsync, ExecuteAsync, and other Dapper calls with retry, timeout, circuit-breaker, and more using a single ResilientDbConnection decorator |
| [PollyMediatR](https://www.nuget.org/packages/PollyMediatR) | [![Downloads](https://img.shields.io/nuget/dt/PollyMediatR.svg)](https://www.nuget.org/packages/PollyMediatR) | Polly v8 resilience pipelines for MediatR — add retry, timeout, circuit-breaker, rate-limiting, hedging, and chaos engineering to any MediatR request handler with a single line of DI registration |
| [PollySqlClient](https://www.nuget.org/packages/PollySqlClient) | [![Downloads](https://img.shields.io/nuget/dt/PollySqlClient.svg)](https://www.nuget.org/packages/PollySqlClient) | Polly v8 resilience pipelines for Microsoft.Data.SqlClient (SQL Server and Azure SQL) — retry, timeout, and circuit-breaker for SqlConnection queries and commands, plus a built-in SqlServerTransientErrors predicate covering all common SQL Server and Azure SQL transient error numbers |
| [PollyAzureKeyVault](https://www.nuget.org/packages/PollyAzureKeyVault) | [![Downloads](https://img.shields.io/nuget/dt/PollyAzureKeyVault.svg)](https://www.nuget.org/packages/PollyAzureKeyVault) | Polly v8 resilience pipelines for Azure Key Vault — retry, timeout, and circuit-breaker for SecretClient, KeyClient, and CertificateClient |
| [PollyAzureQueueStorage](https://www.nuget.org/packages/PollyAzureQueueStorage) | [![Downloads](https://img.shields.io/nuget/dt/PollyAzureQueueStorage.svg)](https://www.nuget.org/packages/PollyAzureQueueStorage) | Polly v8 resilience pipelines for Azure Queue Storage — retry, timeout, and circuit-breaker for Azure.Storage.Queues QueueClient |
| [PollyRedis](https://www.nuget.org/packages/PollyRedis) | [![Downloads](https://img.shields.io/nuget/dt/PollyRedis.svg)](https://www.nuget.org/packages/PollyRedis) | Polly v8 resilience for StackExchange.Redis |
| [PollyAzureServiceBus](https://www.nuget.org/packages/PollyAzureServiceBus) | [![Downloads](https://img.shields.io/nuget/dt/PollyAzureServiceBus.svg)](https://www.nuget.org/packages/PollyAzureServiceBus) | Polly v8 resilience for Azure Service Bus — retry, circuit breaker, and timeout for sending and receiving messages |
| [PollyAzureTableStorage](https://www.nuget.org/packages/PollyAzureTableStorage) | [![Downloads](https://img.shields.io/nuget/dt/PollyAzureTableStorage.svg)](https://www.nuget.org/packages/PollyAzureTableStorage) | Polly v8 resilience pipelines for Azure Table Storage — retry, timeout, and circuit-breaker for Azure.Data.Tables TableClient |

## 💼 Need .NET consulting?

The author of this package is available for consulting on **Polly v8 resilience**, **Azure cloud architecture**, and **clean .NET design**.

**[→ solidqualitysolutions.com](https://www.solidqualitysolutions.com/)** · **[LinkedIn](https://www.linkedin.com/in/justbannister/)**
## License

MIT
