namespace Pz.Connector.ClickHouse;

/// <summary>PZCH error codes. Every user-facing <c>PzConnectorException</c> message has the shape
/// <c>clickhouse: PZCH####: &lt;redacted text&gt;</c>. 01xx connection config, 02xx read, 03xx write,
/// 04xx transport/remote.</summary>
internal static class ChCodes
{
    public const string Config_UrlInvalid = "PZCH0101";
    public const string Config_CaFileUnreadable = "PZCH0102";
    public const string Config_UnknownCluster = "PZCH0103";
    public const string Config_EntityInvalid = "PZCH0104";
    public const string Config_ClusterOnCloud = "PZCH0105";
    public const string Config_ReplicaMacroNotUnique = "PZCH0106";
    public const string Config_Invalid = "PZCH0107";

    public const string Read_EntityAndQuery = "PZCH0201";
    public const string Read_UnsupportedCursorType = "PZCH0202";
    public const string Read_QueryFailed = "PZCH0203";
    public const string Read_UnsupportedType = "PZCH0204";
    public const string Read_BadDatasetOption = "PZCH0205";

    public const string Write_BadWriteMode = "PZCH0301";
    public const string Write_NotReplacing = "PZCH0302";
    public const string Write_UnsupportedArrowType = "PZCH0303";
    public const string Write_TargetColumnMissing = "PZCH0304";
    public const string Write_CommitFailed = "PZCH0305";
    public const string Write_NoExchange = "PZCH0306";
    public const string Write_BadOutputOption = "PZCH0307";
    public const string Write_MergeKeys = "PZCH0308";

    public const string Remote_Unauthorized = "PZCH0401";
    public const string Remote_Transient = "PZCH0402";

    /// <summary>Every code above, kept in sync by hand: reflection over the consts would need trimmer
    /// annotations to survive Native AOT, for a list that changes only when a code is added.</summary>
    internal static readonly IReadOnlyList<string> All =
    [
        Config_UrlInvalid, Config_CaFileUnreadable, Config_UnknownCluster, Config_EntityInvalid, Config_ClusterOnCloud,
        Config_ReplicaMacroNotUnique, Config_Invalid,
        Read_EntityAndQuery, Read_UnsupportedCursorType, Read_QueryFailed, Read_UnsupportedType, Read_BadDatasetOption,
        Write_BadWriteMode, Write_NotReplacing, Write_UnsupportedArrowType, Write_TargetColumnMissing, Write_CommitFailed,
        Write_NoExchange, Write_BadOutputOption, Write_MergeKeys,
        Remote_Unauthorized, Remote_Transient,
    ];

    /// <summary>Redaction runs before the prefix goes on, so a secret that happens to contain
    /// "clickhouse:" cannot shred the one part of the message that is ours.</summary>
    public static string Message(string code, ChRedactor redactor, string text) => $"clickhouse: {code}: {redactor.Redact(text)}";
}
