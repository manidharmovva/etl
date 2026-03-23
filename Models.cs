namespace EtlFanOutFunction;

/// <summary>
/// Represents one logical batch/table to extract-transform-load.
/// The orchestrator fans out one activity per batch.
/// </summary>
public class EtlBatchInfo
{
    /// <summary>Source table or query identifier on the on-prem database.</summary>
    public string SourceTable { get; set; } = string.Empty;

    /// <summary>Destination table on Azure SQL.</summary>
    public string DestinationTable { get; set; } = string.Empty;

    /// <summary>Optional SQL WHERE clause to scope the extract (e.g. date range).</summary>
    public string? Filter { get; set; }
}

/// <summary>
/// Result returned by each activity after processing a batch.
/// </summary>
public class EtlBatchResult
{
    public string SourceTable { get; set; } = string.Empty;
    public string DestinationTable { get; set; } = string.Empty;
    public int RowsExtracted { get; set; }
    public int RowsLoaded { get; set; }
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
}
