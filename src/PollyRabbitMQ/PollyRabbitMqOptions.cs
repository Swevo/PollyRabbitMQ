namespace PollyRabbitMQ;

/// <summary>
/// Configuration options for Polly resilience applied to RabbitMQ channel operations.
/// </summary>
public sealed class PollyRabbitMqOptions
{
    // ── Retry ─────────────────────────────────────────────────────────────

    /// <summary>Number of retry attempts. Set to 0 to disable retries.</summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>Base delay for exponential back-off with jitter.</summary>
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>Maximum delay cap for exponential back-off.</summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(30);

    // ── Circuit breaker ───────────────────────────────────────────────────

    /// <summary>Fraction of failures (0–1) required to open the circuit.</summary>
    public double CircuitBreakerFailureRatio { get; set; } = 0.5;

    /// <summary>Minimum calls within the sampling window before the circuit can open.</summary>
    public int CircuitBreakerMinimumThroughput { get; set; } = 10;

    /// <summary>Sliding window over which failures are measured.</summary>
    public TimeSpan CircuitBreakerSamplingDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How long the circuit stays open before moving to half-open.</summary>
    public TimeSpan CircuitBreakerBreakDuration { get; set; } = TimeSpan.FromSeconds(5);

    // ── Timeout ───────────────────────────────────────────────────────────

    /// <summary>Maximum time allowed per channel operation.</summary>
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(15);

    // ── Channel recreation ────────────────────────────────────────────────

    /// <summary>
    /// When <c>true</c> (default), the <see cref="ResilientChannel"/> will call the
    /// channel factory to create a fresh <see cref="IChannel"/> after a transient
    /// failure before retrying. This handles the case where the channel was closed
    /// by the broker.
    /// </summary>
    public bool RecreateChannelOnFailure { get; set; } = true;
}
