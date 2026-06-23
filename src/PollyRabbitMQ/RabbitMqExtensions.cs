namespace PollyRabbitMQ;

/// <summary>
/// Extension methods on <see cref="IConnection"/> and <see cref="IServiceCollection"/>
/// to create resilient RabbitMQ channels.
/// </summary>
public static class RabbitMqExtensions
{
    /// <summary>
    /// Creates a <see cref="ResilientChannel"/> wrapping a new channel from this connection.
    /// The channel factory is wired up automatically for recreation on transient failures.
    /// </summary>
    public static async Task<ResilientChannel> CreateResilientChannelAsync(
        this IConnection connection,
        Action<PollyRabbitMqOptions>? configure = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var options = new PollyRabbitMqOptions();
        configure?.Invoke(options);

        var channel = await connection.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: false, publisherConfirmationTrackingEnabled: false),
            cancellationToken).ConfigureAwait(false);

        Func<CancellationToken, Task<IChannel>> factory = ct =>
            connection.CreateChannelAsync(
                new CreateChannelOptions(publisherConfirmationsEnabled: false, publisherConfirmationTrackingEnabled: false),
                ct);

        return new ResilientChannel(channel, options, factory);
    }

    // ── DI registration ──────────────────────────────────────────────────

    /// <summary>
    /// Registers a <see cref="ResilientChannel"/> as a singleton using a
    /// <see cref="ConnectionFactory"/> built from the supplied configure delegate.
    /// </summary>
    public static IServiceCollection AddResilientRabbitMqChannel(
        this IServiceCollection services,
        Action<ConnectionFactory> configureFactory,
        Action<PollyRabbitMqOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configureFactory);

        return services.AddSingleton(sp =>
        {
            var factory = new ConnectionFactory();
            configureFactory(factory);
            var connection = factory.CreateConnectionAsync().GetAwaiter().GetResult();
            return connection.CreateResilientChannelAsync(configureOptions).GetAwaiter().GetResult();
        });
    }
}
