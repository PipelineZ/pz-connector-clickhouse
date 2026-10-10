using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace Pz.Connector.ClickHouse.Tests;

[CollectionDefinition("clickhouse-tls")]
public sealed class ClickHouseTlsCollection : ICollectionFixture<ClickHouseTlsFixture>;

/// <summary>A server listening on HTTPS 8443 with a certificate signed by a CA generated here, per
/// run. The CA is written to a temp PEM file for <c>tls_ca_file</c>; the OS trust store never knows
/// it, so a connection without <c>tls_ca_file</c> must fail.</summary>
public sealed class ClickHouseTlsFixture : IAsyncLifetime
{
    private IContainer? _container;

    public string Url { get; private set; } = "";
    public string CaFile { get; private set; } = "";

    public async Task InitializeAsync()
    {
        if (!DockerFacts.IsAvailable)
        {
            return;
        }

        using var caKey = RSA.Create(2048);
        var caReq = new CertificateRequest("CN=pz-test-ca", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caReq.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        caReq.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        using var ca = caReq.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));

        using var serverKey = RSA.Create(2048);
        var serverReq = new CertificateRequest("CN=localhost", serverKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName("localhost");
        san.AddIpAddress(IPAddress.Loopback);
        serverReq.CertificateExtensions.Add(san.Build());
        serverReq.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1")], false));
        using var server = serverReq.Create(ca, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(29), [1, 2, 3, 4]);

        var dir = Directory.CreateTempSubdirectory("pz-ch-tls").FullName;
        CaFile = Path.Combine(dir, "ca.pem");
        await File.WriteAllTextAsync(CaFile, ca.ExportCertificatePem()).ConfigureAwait(false);

        const string TlsXml = """
            <clickhouse>
              <https_port>8443</https_port>
              <openSSL><server>
                <certificateFile>/etc/clickhouse-server/certs/server.crt</certificateFile>
                <privateKeyFile>/etc/clickhouse-server/certs/server.key</privateKeyFile>
                <verificationMode>none</verificationMode>
                <loadDefaultCAFile>false</loadDefaultCAFile>
              </server></openSSL>
            </clickhouse>
            """;

        const uint ReadableByServer = 0x1A4; // 0644

        _container = new ContainerBuilder(ClickHouseFixture.Image)
            .WithEnvironment("CLICKHOUSE_USER", ClickHouseFixture.User).WithEnvironment("CLICKHOUSE_PASSWORD", ClickHouseFixture.Password)
            .WithResourceMapping(Encoding.UTF8.GetBytes(TlsXml), "/etc/clickhouse-server/config.d/pz-tls.xml", ReadableByServer)
            .WithResourceMapping(Encoding.UTF8.GetBytes(server.ExportCertificatePem()), "/etc/clickhouse-server/certs/server.crt", ReadableByServer)
            .WithResourceMapping(Encoding.UTF8.GetBytes(serverKey.ExportPkcs8PrivateKeyPem()), "/etc/clickhouse-server/certs/server.key", ReadableByServer)
            .WithPortBinding(8443, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(8443))
            .Build();
        await _container.StartAsync().ConfigureAwait(false);
        Url = $"https://localhost:{_container.GetMappedPublicPort(8443)}";
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }

    public Dictionary<string, object?> ConnectionConfig(bool withCa)
    {
        var values = new Dictionary<string, object?> { ["url"] = Url, ["user"] = ClickHouseFixture.User, ["password"] = ClickHouseFixture.Password };
        if (withCa) values["tls_ca_file"] = CaFile;
        return values;
    }
}
