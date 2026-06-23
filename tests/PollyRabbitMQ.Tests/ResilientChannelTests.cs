namespace PollyRabbitMQ.Tests;

public class ResilientChannelTests
{
    // helper to set up the generic BasicPublishAsync<BasicProperties>
    private static void SetupPublish(IChannel inner, Func<CallInfo, ValueTask> handler) =>
        inner.BasicPublishAsync<BasicProperties>(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<bool>(),
            Arg.Any<BasicProperties>(), Arg.Any<ReadOnlyMemory<byte>>(),
            Arg.Any<CancellationToken>())
            .Returns(handler);

    // ── Success path ──────────────────────────────────────────────────────

    [Fact]
    public async Task BasicPublishAsync_Success_CompletesWithoutException()
    {
        var inner = Substitute.For<IChannel>();
        SetupPublish(inner, _ => ValueTask.CompletedTask);

        var channel = new ResilientChannel(inner, TestFactory.FastOptions());
        var act = async () => await channel.BasicPublishAsync("", "my-queue", TestFactory.Body());
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task BasicGetAsync_Success_ReturnsNull_WhenNoMessage()
    {
        var inner = Substitute.For<IChannel>();
        inner.BasicGetAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
             .Returns((BasicGetResult?)null);

        var channel = new ResilientChannel(inner, TestFactory.FastOptions());
        var result = await channel.BasicGetAsync("my-queue");

        result.Should().BeNull();
    }

    // ── Retry on transient errors ──────────────────────────────────────────

    [Fact]
    public async Task BasicPublishAsync_AlreadyClosed_Retries()
    {
        int calls = 0;
        var inner = Substitute.For<IChannel>();
        SetupPublish(inner, _ =>
        {
            calls++;
            if (calls < 2) throw TestFactory.MakeClosedException();
            return ValueTask.CompletedTask;
        });

        var channel = new ResilientChannel(inner,
            TestFactory.FastOptions(o => { o.MaxRetries = 3; o.CircuitBreakerMinimumThroughput = 100; }));

        await channel.BasicPublishAsync("", "my-queue", TestFactory.Body());
        calls.Should().Be(2);
    }

    [Fact]
    public async Task BasicPublishAsync_OperationInterrupted_Retries()
    {
        int calls = 0;
        var inner = Substitute.For<IChannel>();
        SetupPublish(inner, _ =>
        {
            calls++;
            if (calls < 2) throw TestFactory.MakeInterruptedException();
            return ValueTask.CompletedTask;
        });

        var channel = new ResilientChannel(inner,
            TestFactory.FastOptions(o => { o.MaxRetries = 3; o.CircuitBreakerMinimumThroughput = 100; }));

        await channel.BasicPublishAsync("", "my-queue", TestFactory.Body());
        calls.Should().Be(2);
    }

    [Fact]
    public async Task BasicGetAsync_TransientError_Retries()
    {
        int calls = 0;
        var inner = Substitute.For<IChannel>();
        inner.BasicGetAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
             .Returns(_ =>
             {
                 calls++;
                 if (calls < 2) throw TestFactory.MakeClosedException();
                 return Task.FromResult<BasicGetResult?>(null);
             });

        var channel = new ResilientChannel(inner,
            TestFactory.FastOptions(o => { o.MaxRetries = 3; o.CircuitBreakerMinimumThroughput = 100; }));

        await channel.BasicGetAsync("my-queue");
        calls.Should().Be(2);
    }

    [Fact]
    public async Task BasicPublishAsync_NonTransientError_NotRetried()
    {
        int calls = 0;
        var inner = Substitute.For<IChannel>();
        SetupPublish(inner, _ =>
        {
            calls++;
            throw new InvalidOperationException("not transient");
        });

        var channel = new ResilientChannel(inner,
            TestFactory.FastOptions(o => { o.MaxRetries = 3; o.CircuitBreakerMinimumThroughput = 100; }));

        Func<Task> act = () => channel.BasicPublishAsync("", "my-queue", TestFactory.Body()).AsTask();
        await act.Should().ThrowAsync<InvalidOperationException>();
        calls.Should().Be(1);
    }

    [Fact]
    public async Task BasicPublishAsync_ExhaustsRetries_ThrowsTransientException()
    {
        var inner = Substitute.For<IChannel>();
        SetupPublish(inner, _ => throw TestFactory.MakeClosedException());

        var channel = new ResilientChannel(inner,
            TestFactory.FastOptions(o => { o.MaxRetries = 2; o.CircuitBreakerMinimumThroughput = 100; }));

        Func<Task> act = () => channel.BasicPublishAsync("", "my-queue", TestFactory.Body()).AsTask();
        await act.Should().ThrowAsync<TransientRabbitMqException>()
            .Where(e => e.RabbitMqException is AlreadyClosedException);
    }

    // ── Circuit breaker ───────────────────────────────────────────────────

    [Fact]
    public async Task CircuitBreaker_OpensAfterThreshold()
    {
        var inner = Substitute.For<IChannel>();
        SetupPublish(inner, _ => throw TestFactory.MakeClosedException());

        var channel = new ResilientChannel(inner, TestFactory.FastOptions(o =>
        {
            o.MaxRetries = 0;
            o.CircuitBreakerMinimumThroughput = 3;
            o.CircuitBreakerFailureRatio = 0.5;
            o.CircuitBreakerSamplingDuration = TimeSpan.FromSeconds(10);
            o.CircuitBreakerBreakDuration = TimeSpan.FromSeconds(10);
        }));

        var exceptions = new List<Exception>();
        for (int i = 0; i < 10; i++)
        {
            try { await channel.BasicPublishAsync("", "my-queue", TestFactory.Body()); }
            catch (Exception ex) { exceptions.Add(ex); }
        }

        exceptions.Should().Contain(e => e is BrokenCircuitException);
    }

    // ── Timeout ───────────────────────────────────────────────────────────

    [Fact]
    public async Task BasicPublishAsync_Timeout_ThrowsTimeoutRejectedException()
    {
        var inner = Substitute.For<IChannel>();
        SetupPublish(inner, info =>
        {
            var ct = info.Arg<CancellationToken>();
            Task.Delay(TimeSpan.FromSeconds(5), ct).GetAwaiter().GetResult();
            return ValueTask.CompletedTask;
        });

        var channel = new ResilientChannel(inner, new PollyRabbitMqOptions
        {
            MaxRetries = 0,
            OperationTimeout = TimeSpan.FromMilliseconds(50),
            CircuitBreakerMinimumThroughput = 100,
        });

        Func<Task> act = () => channel.BasicPublishAsync("", "my-queue", TestFactory.Body()).AsTask();
        await act.Should().ThrowAsync<TimeoutRejectedException>();
    }

    // ── Null guards ───────────────────────────────────────────────────────

    [Fact]
    public void Constructor_NullChannel_Throws()
    {
        Action act = () => new ResilientChannel(null!, new PollyRabbitMqOptions());
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_NullOptions_Throws()
    {
        var inner = Substitute.For<IChannel>();
        Action act = () => new ResilientChannel(inner, null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public async Task BasicPublishAsync_NullExchange_Throws()
    {
        var inner = Substitute.For<IChannel>();
        var channel = new ResilientChannel(inner, TestFactory.FastOptions());
        Func<Task> act = () => channel.BasicPublishAsync(null!, "my-queue", TestFactory.Body()).AsTask();
        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    // ── TransientRabbitMqException properties ─────────────────────────────

    [Fact]
    public async Task TransientException_HasCorrectProperties()
    {
        var inner = Substitute.For<IChannel>();
        SetupPublish(inner, _ => throw TestFactory.MakeClosedException());

        var channel = new ResilientChannel(inner,
            TestFactory.FastOptions(o => { o.MaxRetries = 0; o.CircuitBreakerMinimumThroughput = 100; }));

        Func<Task> act = () => channel.BasicPublishAsync("", "my-queue", TestFactory.Body()).AsTask();
        var ex = await act.Should().ThrowAsync<TransientRabbitMqException>();
        ex.Which.RabbitMqException.Should().BeOfType<AlreadyClosedException>();
    }

    // ── QueueDeclare / BasicAck ───────────────────────────────────────────

    [Fact]
    public async Task BasicAckAsync_Success_Completes()
    {
        var inner = Substitute.For<IChannel>();
        inner.BasicAckAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
             .Returns(ValueTask.CompletedTask);

        var channel = new ResilientChannel(inner, TestFactory.FastOptions());
        await channel.BasicAckAsync(1UL);

        await inner.Received(1).BasicAckAsync(1UL, false, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task BasicNackAsync_Success_Completes()
    {
        var inner = Substitute.For<IChannel>();
        inner.BasicNackAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
             .Returns(ValueTask.CompletedTask);

        var channel = new ResilientChannel(inner, TestFactory.FastOptions());
        await channel.BasicNackAsync(1UL);

        await inner.Received(1).BasicNackAsync(1UL, false, true, Arg.Any<CancellationToken>());
    }

    // ── InnerChannel ─────────────────────────────────────────────────────

    [Fact]
    public void InnerChannel_ReturnsWrappedChannel()
    {
        var inner = Substitute.For<IChannel>();
        var channel = new ResilientChannel(inner, TestFactory.FastOptions());
        channel.InnerChannel.Should().BeSameAs(inner);
    }
}
