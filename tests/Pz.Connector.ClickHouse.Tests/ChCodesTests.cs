namespace Pz.Connector.ClickHouse.Tests;

public sealed class ChCodesTests
{
    [Fact]
    public void Every_code_is_unique_and_well_formed()
    {
        Assert.Equal(ChCodes.All.Count, ChCodes.All.Distinct().Count());
        Assert.All(ChCodes.All, c => Assert.Matches(@"\APZCH0[1-4]\d\d\z", c));
    }

    [Fact]
    public void Message_prefixes_after_redaction()
    {
        var redactor = new ChRedactor(["clickhouse: s3cret"]);
        Assert.Equal("clickhouse: PZCH0401: denied for ***", ChCodes.Message(ChCodes.Remote_Unauthorized, redactor, "denied for clickhouse: s3cret"));
    }
}
