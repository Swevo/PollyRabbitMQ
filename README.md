# PollyRabbitMQ

[![NuGet](https://img.shields.io/nuget/v/PollyRabbitMQ.svg)](https://www.nuget.org/packages/PollyRabbitMQ)
[![Build Status](https://github.com/Swevo/PollyRabbitMQ/actions/workflows/build.yml/badge.svg)](https://github.com/Swevo/PollyRabbitMQ/actions)

**PollyRabbitMQ** wraps RabbitMQ.Client v7 channels with a Polly v8 resilience pipeline — retry, circuit breaker, and timeout — so your messaging code handles transient broker failures transparently.

## Features

- 🔁 **Retry** with exponential back-off + jitter on transient RabbitMQ exceptions
- ⚡ **Circuit Breaker** to stop hammering a degraded broker
- ⏱️ **Timeout** per operation
- 🔄 **Optional channel recreation** between retries when the broker closes the channel
- 🧩 **Works with any `IChannel`** from RabbitMQ.Client 7+
- 💉 **Dependency Injection** via `IServiceCollection` extensions

## Installation

```bash
dotnet add package PollyRabbitMQ
```

## Quick Start

```csharp
using PollyRabbitMQ;
using RabbitMQ.Client;

// Create a connection the normal way
var factory = new ConnectionFactory { HostName = "localhost" };
await using var connection = await factory.CreateConnectionAsync();

// Wrap a channel with Polly resilience
var resilientChannel = await connection.CreateResilientChannelAsync();

// Declare a queue
await resilientChannel.QueueDeclareAsync("my-queue");

// Publish a message — retried automatically on transient failures
var body = Encoding.UTF8.GetBytes("Hello, World!");
await resilientChannel.BasicPublishAsync("", "my-queue", body);

// Consume a message
var result = await resilientChannel.BasicGetAsync("my-queue");
if (result is not null)
{
    Console.WriteLine(Encoding.UTF8.GetString(result.Body.Span));
    await resilientChannel.BasicAckAsync(result.DeliveryTag);
}
```

## Dependency Injection

```csharp
builder.Services.AddResilientRabbitMqChannel(connection, options =>
{
    options.MaxRetries = 5;
    options.BaseDelay = TimeSpan.FromMilliseconds(200);
    options.MaxDelay = TimeSpan.FromSeconds(30);
    options.OperationTimeout = TimeSpan.FromSeconds(10);
    options.CircuitBreakerFailureRatio = 0.5;
    options.CircuitBreakerMinimumThroughput = 5;
    options.CircuitBreakerBreakDuration = TimeSpan.FromSeconds(30);
    options.RecreateChannelOnFailure = true;
});

// Inject ResilientChannel into your service
public class MyConsumer(ResilientChannel channel) { ... }
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `MaxRetries` | `3` | Number of retries (0 = no retries) |
| `BaseDelay` | `200 ms` | Starting delay for exponential back-off |
| `MaxDelay` | `30 s` | Maximum delay between retries |
| `OperationTimeout` | `10 s` | Timeout per individual operation |
| `CircuitBreakerFailureRatio` | `0.5` | Failure ratio to open the circuit |
| `CircuitBreakerMinimumThroughput` | `5` | Min calls before CB evaluates |
| `CircuitBreakerSamplingDuration` | `30 s` | Sliding window for CB evaluation |
| `CircuitBreakerBreakDuration` | `30 s` | How long the circuit stays open |
| `RecreateChannelOnFailure` | `false` | Recreate the channel between retries |

## Transient Exceptions

The following RabbitMQ exceptions are treated as transient and trigger retry:

- `AlreadyClosedException` — channel closed by broker
- `OperationInterruptedException` — operation interrupted
- `BrokerUnreachableException` — broker temporarily unreachable
- `ConnectFailureException` — initial connection failed

Non-transient exceptions propagate immediately without retry.

## Channel Recreation

When `RecreateChannelOnFailure = true` and a channel factory is supplied, the channel is recreated between retries:

```csharp
var channel = await connection.CreateResilientChannelAsync(
    options: new PollyRabbitMqOptions { RecreateChannelOnFailure = true },
    channelFactory: ct => connection.CreateChannelAsync(cancellationToken: ct));
```

## Related Packages

| Package | Purpose |
|---------|---------|
| [PollyBackoff](https://github.com/Swevo/PollyBackoff) | Polly v8 advanced back-off strategies |
| [PollyChaos](https://github.com/Swevo/PollyChaos) | Chaos engineering for Polly v8 |
| [PollyMediatR](https://github.com/Swevo/PollyMediatR) | Polly resilience for MediatR handlers |
| [PollyEFCore](https://github.com/Swevo/PollyEFCore) | Polly resilience for Entity Framework Core |
| [PollyHealthChecks](https://github.com/Swevo/PollyHealthChecks) | ASP.NET Core health checks for Polly policies |
| [PollyOpenAI](https://github.com/Swevo/PollyOpenAI) | Polly resilience for OpenAI / Azure OpenAI |
| [PollyRedis](https://github.com/Swevo/PollyRedis) | Polly resilience for StackExchange.Redis |
| [PollySignalR](https://github.com/Swevo/PollySignalR) | Polly resilience for SignalR clients |
| [PollyGrpc](https://github.com/Swevo/PollyGrpc) | Polly resilience for gRPC channels |
| [PollyKafka](https://github.com/Swevo/PollyKafka) | Polly resilience for Confluent Kafka |
| [PollyAzureServiceBus](https://github.com/Swevo/PollyAzureServiceBus) | Polly resilience for Azure Service Bus |
| [Polly.Contrib.Caching](https://github.com/Swevo/Polly.Contrib.Caching) | Polly v8 caching policy |
| [Polly.Contrib.Bulkhead](https://github.com/Swevo/Polly.Contrib.Bulkhead) | Polly v8 bulkhead policy |
| [Polly.Contrib.OpenTelemetry](https://github.com/Swevo/Polly-Contrib-OpenTelemetry) | OpenTelemetry instrumentation for Polly |

## License

MIT
