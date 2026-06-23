namespace PollyRabbitMQ;

/// <summary>
/// Internal helper that constructs the Polly v8 resilience pipeline.
/// </summary>
internal static class PipelineBuilder
{
    public static ResiliencePipeline Build(
        PollyRabbitMqOptions options,
        Func<CancellationToken, ValueTask>? onRetry = null)
    {
        var predicate = new PredicateBuilder().Handle<TransientRabbitMqException>();
        var builder = new ResiliencePipelineBuilder();

        if (options.MaxRetries >= 1)
        {
            builder.AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = predicate,
                MaxRetryAttempts = options.MaxRetries,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                Delay = options.BaseDelay,
                MaxDelay = options.MaxDelay,
                OnRetry = onRetry is null
                    ? null
                    : args => onRetry(args.Context.CancellationToken),
            });
        }

        builder
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                ShouldHandle = predicate,
                FailureRatio = options.CircuitBreakerFailureRatio,
                MinimumThroughput = options.CircuitBreakerMinimumThroughput,
                SamplingDuration = options.CircuitBreakerSamplingDuration,
                BreakDuration = options.CircuitBreakerBreakDuration,
            })
            .AddTimeout(new TimeoutStrategyOptions
            {
                Timeout = options.OperationTimeout,
            });

        return builder.Build();
    }

    public static ResiliencePipeline<T> BuildTyped<T>(
        PollyRabbitMqOptions options,
        Func<CancellationToken, ValueTask>? onRetry = null)
    {
        var predicate = new PredicateBuilder<T>().Handle<TransientRabbitMqException>();
        var builder = new ResiliencePipelineBuilder<T>();

        if (options.MaxRetries >= 1)
        {
            builder.AddRetry(new RetryStrategyOptions<T>
            {
                ShouldHandle = predicate,
                MaxRetryAttempts = options.MaxRetries,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                Delay = options.BaseDelay,
                MaxDelay = options.MaxDelay,
                OnRetry = onRetry is null
                    ? null
                    : args => onRetry(args.Context.CancellationToken),
            });
        }

        builder
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions<T>
            {
                ShouldHandle = predicate,
                FailureRatio = options.CircuitBreakerFailureRatio,
                MinimumThroughput = options.CircuitBreakerMinimumThroughput,
                SamplingDuration = options.CircuitBreakerSamplingDuration,
                BreakDuration = options.CircuitBreakerBreakDuration,
            })
            .AddTimeout(new TimeoutStrategyOptions
            {
                Timeout = options.OperationTimeout,
            });

        return builder.Build();
    }
}
