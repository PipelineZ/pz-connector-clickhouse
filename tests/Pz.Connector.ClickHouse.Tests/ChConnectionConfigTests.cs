using Pz.Connectors.Abstractions;

namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChConnectionConfigTests
{
    private static (ChConnectionConfig? Config, List<string> Errors) Parse(Dictionary<string, object?> values)
    {
        var errors = new List<string>();
        return (ChConnectionConfig.Parse(new ConnectorConfig(values), errors), errors);
    }

    [Fact]
    public void Minimal_config_takes_defaults()
    {
        var (cfg, errors) = Parse(new() { ["url"] = "http://localhost:8123" });
        Assert.Empty(errors);
        Assert.Equal(new Uri("http://localhost:8123/"), cfg!.Url);
        Assert.Equal("default", cfg.User);
        Assert.Null(cfg.Password);
        Assert.Equal("default", cfg.Database);
        Assert.Null(cfg.Cluster);
        Assert.Empty(cfg.Settings);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("ftp://h:21")]
    [InlineData("https://h:8443/path")]
    [InlineData("https://h:8443/?x=1")]
    [InlineData("https://user:pw@h:8443")]
    [InlineData("not a url")]
    public void Bad_url_is_0101(string? url)
    {
        var (cfg, errors) = Parse(new() { ["url"] = url });
        Assert.Null(cfg);
        Assert.Contains(errors, e => e.Contains("PZCH0101"));
    }

    [Fact]
    public void Unknown_key_is_0107_and_errors_aggregate()
    {
        var (_, errors) = Parse(new() { ["url"] = "ftp://x", ["passwrd"] = "x" });
        Assert.Contains(errors, e => e.Contains("PZCH0101"));
        Assert.Contains(errors, e => e.Contains("PZCH0107") && e.Contains("'passwrd'"));
    }

    [Fact]
    public void Password_is_registered_with_the_redactor()
    {
        var (cfg, _) = Parse(new() { ["url"] = "https://h:8443", ["password"] = "hunter22" });
        Assert.Equal("x ***", cfg!.Redactor.Redact("x hunter22"));
    }

    [Fact]
    public void Settings_render_scalars_and_integral_doubles()
    {
        var (cfg, errors) = Parse(new()
        {
            ["url"] = "http://h:8123",
            ["settings"] = new Dictionary<string, object?> { ["max_execution_time"] = 600.0, ["ratio"] = 0.5, ["flag"] = true, ["s"] = "x" },
        });
        Assert.Empty(errors);
        Assert.Equal("600", cfg!.Settings["max_execution_time"]);
        Assert.Equal("0.5", cfg.Settings["ratio"]);
        Assert.Equal("1", cfg.Settings["flag"]);
        Assert.Equal("x", cfg.Settings["s"]);
    }

    [Fact]
    public void Non_scalar_setting_is_0107()
    {
        var (_, errors) = Parse(new() { ["url"] = "http://h:8123", ["settings"] = new Dictionary<string, object?> { ["x"] = new List<object?>() } });
        Assert.Contains(errors, e => e.Contains("PZCH0107") && e.Contains("'settings.x'"));
    }

    [Fact]
    public void Relative_ca_file_anchors_on_base_dir()
    {
        var dir = Directory.CreateTempSubdirectory().FullName;
        File.WriteAllText(Path.Combine(dir, "ca.pem"), "x");
        var (cfg, errors) = Parse(new() { ["url"] = "https://h:8443", ["tls_ca_file"] = "ca.pem", ["base_dir"] = dir });
        Assert.Empty(errors);
        Assert.Equal(Path.Combine(dir, "ca.pem"), cfg!.CaFile);
    }

    [Fact]
    public void Missing_ca_file_is_0102()
    {
        var (_, errors) = Parse(new() { ["url"] = "https://h:8443", ["tls_ca_file"] = "/nope/ca.pem" });
        Assert.Contains(errors, e => e.Contains("PZCH0102"));
    }

    [Fact]
    public void Ca_file_on_plain_http_is_0107()
    {
        var dir = Directory.CreateTempSubdirectory().FullName;
        File.WriteAllText(Path.Combine(dir, "ca.pem"), "x");
        var (_, errors) = Parse(new() { ["url"] = "http://h:8123", ["tls_ca_file"] = Path.Combine(dir, "ca.pem") });
        Assert.Contains(errors, e => e.Contains("PZCH0107") && e.Contains("https"));
    }

    [Fact]
    public async Task ValidateAsync_reports_errors_without_network()
    {
        var result = await new ChConnector().ValidateAsync(new ConnectorConfig(new Dictionary<string, object?> { ["url"] = "nope" }), default);
        Assert.False(result.IsValid);
    }
}
