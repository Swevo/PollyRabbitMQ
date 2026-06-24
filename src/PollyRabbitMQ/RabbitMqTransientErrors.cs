namespace PollyRabbitMQ;

/// <summary>
/// Pre-built Polly <see cref="PredicateBuilder"/> for common RabbitMQ transient errors.
/// Covers channel/connection closures, broker unreachable, interrupted operations, and TCP connect failures.
/// </summary>
public static class RabbitMqTransientErrors
{
    /// <summary>
    /// A <see cref="PredicateBuilder"/> that handles the four most common RabbitMQ transient exceptions.
    /// Assign to <c>ShouldHandle</c> on any Polly retry or circuit-breaker strategy.
    /// </summary>
    /// <remarks>
    /// Covered exceptions:
    /// <list type="bullet">
    ///   <item><see cref="AlreadyClosedException"/> — channel or connection closed by the broker</item>
    ///   <item><see cref="OperationInterruptedException"/> — operation interrupted mid-flight (e.g. broker restart)</item>
    ///   <item><see cref="BrokerUnreachableException"/> — cannot reach the broker (all endpoints tried)</item>
    ///   <item><see cref="ConnectFailureException"/> — TCP connect to a broker endpoint failed</item>
    /// </list>
    /// </remarks>
    public static readonly PredicateBuilder IsTransient =
        (PredicateBuilder)new PredicateBuilder()
            .Handle<AlreadyClosedException>()
            .Handle<OperationInterruptedException>()
            .Handle<BrokerUnreachableException>()
            .Handle<ConnectFailureException>();
}
