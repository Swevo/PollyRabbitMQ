namespace PollyRabbitMQ;

/// <summary>
/// Determines whether a RabbitMQ exception should be treated as transient (retried).
/// </summary>
internal static class TransientExceptionPolicy
{
    /// <summary>
    /// Returns <c>true</c> for RabbitMQ exceptions that are likely transient:
    /// channel/connection closures, broker unreachable, interrupted operations.
    /// </summary>
    public static bool IsTransient(Exception ex) => ex switch
    {
        AlreadyClosedException          => true,  // channel/connection closed by broker
        OperationInterruptedException   => true,  // operation interrupted mid-flight
        BrokerUnreachableException      => true,  // can't connect to broker
        ConnectFailureException         => true,  // TCP connect failed
        _ => false,
    };
}
