using Pz.Connectors.TestKit;

namespace Pz.Connector.ClickHouse.Tests;

/// <summary>The TestKit's credential shapes through this connector's redactor, seeded with the same
/// synthetic secret the suite embeds, exactly as a real config seeds it with the password.</summary>
public sealed class ChRedactionContract : ErrorRedactionContractTests
{
    protected override string RedactErrorText(string thirdPartyMessage) =>
        new ChRedactor(["pz-testkit-secret-value"]).Redact(thirdPartyMessage);
}
