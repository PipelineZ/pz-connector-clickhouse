namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChRedactorTests
{
    [Fact]
    public void Registered_secret_is_masked_longest_first()
    {
        var r = new ChRedactor(["abc", "abcdef"]);
        Assert.Equal("x *** y ***", r.Redact("x abcdef y abc"));
    }

    [Theory]
    [InlineData("X-ClickHouse-Key: hunter22 rest", "X-ClickHouse-Key: *** rest")]
    [InlineData("Authorization: Basic dXNlcjpwYXNz", "Authorization: ***")]
    [InlineData("url ?password=hunter22&x=1", "url ?password=***&x=1")]
    [InlineData("http://user:hunter22@host:8123/", "http://user:***@host:8123/")]
    public void Credential_shapes_are_masked_even_when_unregistered(string input, string expected) =>
        Assert.Equal(expected, ChRedactor.None.Redact(input));

    [Fact]
    public void Short_secrets_are_not_matched() =>
        Assert.Equal("a b", new ChRedactor(["a"]).Redact("a b"));
}
