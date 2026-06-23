namespace PollyRabbitMQ.Tests;

internal static class TestFactory
{
    public static PollyRabbitMqOptions FastOptions(Action<PollyRabbitMqOptions>? configure = null)
    {
        var opts = new PollyRabbitMqOptions
        {
            BaseDelay = TimeSpan.Zero,
            MaxDelay = TimeSpan.Zero,
            OperationTimeout = TimeSpan.FromSeconds(10),
            CircuitBreakerMinimumThroughput = 100,
            CircuitBreakerSamplingDuration = TimeSpan.FromSeconds(10),
            CircuitBreakerBreakDuration = TimeSpan.FromSeconds(1),
            RecreateChannelOnFailure = false,
        };
        configure?.Invoke(opts);
        return opts;
    }

    public static AlreadyClosedException MakeClosedException()
        => new(new ShutdownEventArgs(ShutdownInitiator.Peer, 0, "channel closed"));

    public static OperationInterruptedException MakeInterruptedException()
        => new(new ShutdownEventArgs(ShutdownInitiator.Peer, 0, "interrupted"));

    public static ReadOnlyMemory<byte> Body(string content = "hello")
        => System.Text.Encoding.UTF8.GetBytes(content);
}
