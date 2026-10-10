using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChErrorsTests
{
    private static readonly ChRedactor R = ChRedactor.None;

    [Theory]
    [InlineData(159)] [InlineData(209)] [InlineData(210)] [InlineData(242)] [InlineData(252)] [InlineData(202)] [InlineData(999)]
    public void Transient_exception_codes(int code) => Assert.True(ChErrors.IsTransientCode(code));

    [Theory]
    [InlineData(60)] [InlineData(62)] [InlineData(516)] [InlineData(47)]
    public void Permanent_exception_codes(int code) => Assert.False(ChErrors.IsTransientCode(code));

    [Fact]
    public void Exception_code_parses_from_the_body_text() =>
        Assert.Equal(60, ChErrors.ParseExceptionCode("Code: 60. DB::Exception: Unknown table expression identifier 'x'. (UNKNOWN_TABLE) (version 25.8.1.1)"));

    [Fact]
    public void Missing_code_parses_as_null() => Assert.Null(ChErrors.ParseExceptionCode("<html>bad gateway</html>"));

    [Fact]
    public void Auth_failure_is_permanent_0401()
    {
        var ex = ChErrors.FromServer(500, 516, "Code: 516. DB::Exception: pz: Authentication failed", null, R, "reading 'x'", ChCodes.Read_QueryFailed);
        Assert.False(ex.IsTransient);
        Assert.StartsWith("clickhouse: PZCH0401: reading 'x': ", ex.Message);
    }

    [Fact]
    public void Too_many_parts_is_transient_0402_with_retry_after()
    {
        var ex = ChErrors.FromServer(500, 252, "Code: 252. DB::Exception: Too many parts", TimeSpan.FromSeconds(3), R, "ctx", ChCodes.Write_CommitFailed);
        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(3), ex.RetryAfter);
        Assert.Contains("PZCH0402", ex.Message);
    }

    [Fact]
    public void Http_503_without_a_code_is_transient() =>
        Assert.True(ChErrors.FromServer(503, null, "upstream unavailable", null, R, "ctx", ChCodes.Read_QueryFailed).IsTransient);

    [Fact]
    public void Other_server_errors_use_the_callers_fail_code()
    {
        var ex = ChErrors.FromServer(404, 60, "Code: 60. DB::Exception: Unknown table", null, R, "reading 'x'", ChCodes.Read_QueryFailed);
        Assert.False(ex.IsTransient);
        Assert.StartsWith("clickhouse: PZCH0203: reading 'x': ClickHouse error 60: Code: 60. DB::Exception: Unknown table", ex.Message);
    }

    [Fact]
    public void Summarize_keeps_the_first_line_and_caps_length()
    {
        var body = "Code: 62. DB::Exception: Syntax error\nStack trace:\n0. foo";
        Assert.Equal("Code: 62. DB::Exception: Syntax error", ChErrors.Summarize(body, R));
        Assert.Equal(500, ChErrors.Summarize(new string('x', 2000), R).Length);
    }

    [Fact]
    public void Wrap_refuses_cancellation() =>
        Assert.Throws<InvalidOperationException>(() => ChErrors.Wrap(new OperationCanceledException(), R, "ctx"));

    [Fact]
    public void Wrap_makes_transport_failures_transient() =>
        Assert.True(ChErrors.Wrap(new HttpRequestException("connection refused"), R, "ctx").IsTransient);

    [Fact]
    public void Untrusted_certificate_is_permanent_with_a_tls_hint()
    {
        var ex = ChErrors.Wrap(new HttpRequestException("ssl", new System.Security.Authentication.AuthenticationException("remote certificate is invalid")), R, "ctx");
        Assert.False(ex.IsTransient);
        Assert.Contains("PZCH0401", ex.Message);
        Assert.Contains("tls_ca_file", ex.Message);
    }

    [Fact]
    public void Tls_handshake_aborted_by_a_reset_stays_transient()
    {
        var inner = new System.Security.Authentication.AuthenticationException("handshake failed",
            new IOException("reset", new System.Net.Sockets.SocketException(104)));
        var ex = ChErrors.Wrap(new HttpRequestException("ssl", inner), R, "ctx");
        Assert.True(ex.IsTransient);
        Assert.Contains("PZCH0402", ex.Message);
    }

    [Fact]
    public void A_secret_straddling_the_length_cap_leaves_no_prefix()
    {
        var password = "Zq9" + new string('k', 20) + "W7";
        var redactor = new ChRedactor([password]);
        var body = new string('x', 490) + password + " trailing";
        var ex = ChErrors.FromServer(500, 60, body, null, redactor, "ctx", ChCodes.Read_QueryFailed);
        Assert.DoesNotContain("Zq9", ex.Message, StringComparison.Ordinal);
        Assert.Contains(ChRedactor.Mask, ex.Message, StringComparison.Ordinal);
    }
}
