namespace PollyRabbitMQ;

/// <summary>
/// A resilient RabbitMQ channel that wraps <see cref="IChannel"/> with a Polly v8
/// pipeline: retry → circuit breaker → timeout.
/// </summary>
/// <remarks>
/// When <see cref="PollyRabbitMqOptions.RecreateChannelOnFailure"/> is <c>true</c>
/// and a channel factory is supplied, the channel is recreated between retries so
/// that transient broker-side closures are handled transparently.
/// </remarks>
public sealed class ResilientChannel : IAsyncDisposable
{
    private IChannel _channel;
    private readonly Func<CancellationToken, Task<IChannel>>? _channelFactory;
    private readonly PollyRabbitMqOptions _options;
    private readonly ResiliencePipeline _pipeline;
    private readonly ResiliencePipeline<BasicGetResult?> _getPipeline;
    private readonly ResiliencePipeline<QueueDeclareOk> _declarePipeline;
    private readonly SemaphoreSlim _lock = new(1, 1);

    /// <summary>
    /// Initialises a resilient channel wrapping the given <see cref="IChannel"/>.
    /// </summary>
    /// <param name="channel">The underlying RabbitMQ channel.</param>
    /// <param name="options">Resilience options.</param>
    /// <param name="channelFactory">
    /// Optional factory for recreating the channel after transient failures.
    /// Required when <see cref="PollyRabbitMqOptions.RecreateChannelOnFailure"/> is <c>true</c>.
    /// </param>
    public ResilientChannel(
        IChannel channel,
        PollyRabbitMqOptions options,
        Func<CancellationToken, Task<IChannel>>? channelFactory = null)
    {
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentNullException.ThrowIfNull(options);
        _channel = channel;
        _options = options;
        _channelFactory = channelFactory;
        _pipeline = PipelineBuilder.Build(options, OnRetryAsync);
        _getPipeline = PipelineBuilder.BuildTyped<BasicGetResult?>(options, OnRetryAsync);
        _declarePipeline = PipelineBuilder.BuildTyped<QueueDeclareOk>(options, OnRetryAsync);
    }

    // ── Publish ───────────────────────────────────────────────────────────

    /// <summary>
    /// Publishes a message with Polly resilience applied.
    /// Uses the simple non-generic extension method overload for ergonomic usage.
    /// </summary>
    public async ValueTask BasicPublishAsync(
        string exchange,
        string routingKey,
        ReadOnlyMemory<byte> body,
        bool mandatory = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(exchange);
        ArgumentNullException.ThrowIfNull(routingKey);

        await _pipeline.ExecuteAsync(async ct =>
        {
            try
            {
                await _channel.BasicPublishAsync<BasicProperties>(
                        exchange, routingKey, mandatory, new BasicProperties(), body, ct)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (TransientExceptionPolicy.IsTransient(ex))
            {
                throw new TransientRabbitMqException(ex);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    // ── Get ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Gets a single message from the queue with Polly resilience applied.
    /// Returns <c>null</c> if no message is available.
    /// </summary>
    public async Task<BasicGetResult?> BasicGetAsync(
        string queue,
        bool autoAck = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queue);

        return await _getPipeline.ExecuteAsync(async ct =>
        {
            try
            {
                return await _channel.BasicGetAsync(queue, autoAck, ct)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (TransientExceptionPolicy.IsTransient(ex))
            {
                throw new TransientRabbitMqException(ex);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    // ── Topology ──────────────────────────────────────────────────────────

    /// <summary>Declares a queue with Polly resilience applied.</summary>
    public async Task<QueueDeclareOk> QueueDeclareAsync(
        string queue,
        bool durable = true,
        bool exclusive = false,
        bool autoDelete = false,
        IDictionary<string, object>? arguments = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queue);

        return await _declarePipeline.ExecuteAsync(async ct =>
        {
            try
            {
                return await _channel.QueueDeclareAsync(
                    queue, durable, exclusive, autoDelete, arguments, false, false, ct)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (TransientExceptionPolicy.IsTransient(ex))
            {
                throw new TransientRabbitMqException(ex);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Declares an exchange with Polly resilience applied.</summary>
    public async Task ExchangeDeclareAsync(
        string exchange,
        string type,
        bool durable = true,
        bool autoDelete = false,
        IDictionary<string, object>? arguments = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(exchange);
        ArgumentNullException.ThrowIfNull(type);

        await _pipeline.ExecuteAsync(async ct =>
        {
            try
            {
                await _channel.ExchangeDeclareAsync(
                    exchange, type, durable, autoDelete, arguments, false, false, ct)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (TransientExceptionPolicy.IsTransient(ex))
            {
                throw new TransientRabbitMqException(ex);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    // ── Acknowledge ───────────────────────────────────────────────────────

    /// <summary>Acknowledges a message with Polly resilience applied.</summary>
    public async Task BasicAckAsync(
        ulong deliveryTag,
        bool multiple = false,
        CancellationToken cancellationToken = default)
    {
        await _pipeline.ExecuteAsync(async ct =>
        {
            try
            {
                await _channel.BasicAckAsync(deliveryTag, multiple, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (TransientExceptionPolicy.IsTransient(ex))
            {
                throw new TransientRabbitMqException(ex);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Negatively acknowledges a message with Polly resilience applied.</summary>
    public async Task BasicNackAsync(
        ulong deliveryTag,
        bool multiple = false,
        bool requeue = true,
        CancellationToken cancellationToken = default)
    {
        await _pipeline.ExecuteAsync(async ct =>
        {
            try
            {
                await _channel.BasicNackAsync(deliveryTag, multiple, requeue, ct)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (TransientExceptionPolicy.IsTransient(ex))
            {
                throw new TransientRabbitMqException(ex);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Gets the underlying <see cref="IChannel"/> for advanced use-cases.</summary>
    public IChannel InnerChannel => _channel;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _lock.Dispose();
        await _channel.DisposeAsync().ConfigureAwait(false);
    }

    // ── Channel recreation on retry ───────────────────────────────────────

    private async ValueTask OnRetryAsync(CancellationToken ct)
    {
        if (!_options.RecreateChannelOnFailure || _channelFactory is null) return;

        await _lock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_channel.IsOpen) return;
            var old = _channel;
            _channel = await _channelFactory(ct).ConfigureAwait(false);
            await old.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _lock.Release();
        }
    }
}
