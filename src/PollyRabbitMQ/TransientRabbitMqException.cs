namespace PollyRabbitMQ;

/// <summary>
/// Thrown when a transient RabbitMQ exception is caught, allowing Polly to identify
/// retriable channel errors without coupling the pipeline predicate to RabbitMQ internals.
/// </summary>
public sealed class TransientRabbitMqException : Exception
{
    /// <summary>The original RabbitMQ exception.</summary>
    public Exception RabbitMqException { get; }

    /// <inheritdoc />
    public TransientRabbitMqException(Exception inner)
        : base(inner.Message, inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        RabbitMqException = inner;
    }
}
