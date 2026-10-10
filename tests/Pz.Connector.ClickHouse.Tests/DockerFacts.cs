using System.Diagnostics;

namespace Pz.Connector.ClickHouse.Tests;

/// <summary>Docker probe shared by every server-backed fact. A collection fixture must not throw
/// SkipException (xunit reports that as a failed fixture, not a skip), so fixtures consult
/// <see cref="IsAvailable"/> and no-op, and each test class constructor calls
/// <see cref="SkipUnlessDocker"/> where the skip registers as a skip. "Available" means the daemon
/// runs Linux containers: a Windows runner's daemon in Windows-container mode counts as absent.</summary>
internal static class DockerFacts
{
    private static readonly Lazy<bool> DockerAvailable = new(() =>
    {
        try
        {
            var psi = new ProcessStartInfo("docker", "info --format {{.OSType}}") { RedirectStandardOutput = true, RedirectStandardError = true };
            using var process = Process.Start(psi);
            if (process is null || !process.WaitForExit(10_000) || process.ExitCode != 0)
            {
                return false;
            }

            return process.StandardOutput.ReadToEnd().Trim() == "linux";
        }
        catch
        {
            return false;
        }
    });

    public static bool IsAvailable => DockerAvailable.Value;

    public static void SkipUnlessDocker() => Skip.IfNot(DockerAvailable.Value, "docker (linux containers) is not available");
}
