using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.DurableTask;
using Microsoft.Extensions.Logging;

namespace EtlFanOutFunction;

public class EtlActivities
{
    // -----------------------------------------------------------------------
    //  Activity 1 – Discover tables/batches from the on-prem database
    // -----------------------------------------------------------------------
    [Function(nameof(DiscoverTables))]
    public async Task<List<EtlBatchInfo>> DiscoverTables(
        [ActivityTrigger] object? input,
        FunctionContext context)
    {
        var logger = context.GetLogger(nameof(DiscoverTables));
        logger.LogInformation("Discovering source tables from on-prem SQL Server...");

        var batches = new List<EtlBatchInfo>();

        await using var conn = await DbConnectionHelper.ConnectOnPremAsync(logger);

        // Query the INFORMATION_SCHEMA for user tables.
        // Adjust the query/filter to match your schema.
        const string sql = @"
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME";

        await using var cmd = new SqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            var schema = reader.GetString(0);
            var table = reader.GetString(1);
            batches.Add(new EtlBatchInfo
            {
                SourceTable = $"[{schema}].[{table}]",
                DestinationTable = $"[{schema}].[{table}]"
            });
        }

        logger.LogInformation("Discovered {Count} tables to process.", batches.Count);
        return batches;
    }

    // -----------------------------------------------------------------------
    //  Activity 2 – Extract, Transform, and Load a single batch
    // -----------------------------------------------------------------------
    [Function(nameof(ProcessBatch))]
    public async Task<EtlBatchResult> ProcessBatch(
        [ActivityTrigger] EtlBatchInfo batch,
        FunctionContext context)
    {
        var logger = context.GetLogger(nameof(ProcessBatch));
        logger.LogInformation("Processing batch: {Source} → {Dest}",
            batch.SourceTable, batch.DestinationTable);

        var result = new EtlBatchResult
        {
            SourceTable = batch.SourceTable,
            DestinationTable = batch.DestinationTable
        };

        try
        {
            // ---- EXTRACT from on-prem ----
            logger.LogInformation("Extracting data from on-prem table {Table}...", batch.SourceTable);

            await using var srcConn = await DbConnectionHelper.ConnectOnPremAsync(logger);

            var filter = string.IsNullOrWhiteSpace(batch.Filter) ? "" : $" WHERE {batch.Filter}";
            var selectSql = $"SELECT * FROM {batch.SourceTable}{filter}";

            await using var cmd = new SqlCommand(selectSql, srcConn);
            cmd.CommandTimeout = 300; // 5 min for large tables
            await using var reader = await cmd.ExecuteReaderAsync();

            // Materialize into a DataTable for bulk copy (transform step can be added here)
            var dataTable = new System.Data.DataTable();
            dataTable.Load(reader);
            result.RowsExtracted = dataTable.Rows.Count;

            logger.LogInformation("Extracted {Rows} rows from {Table}.",
                result.RowsExtracted, batch.SourceTable);

            // ---- TRANSFORM (placeholder) ----
            // Add any column mappings, data cleansing, type conversions here.
            // Example: dataTable.Columns["OldName"].ColumnName = "NewName";

            // ---- LOAD into Azure SQL ----
            logger.LogInformation("Loading data into Azure SQL table {Table}...", batch.DestinationTable);

            await using var destConn = await DbConnectionHelper.ConnectAzureSqlAsync(logger);

            using var bulkCopy = new SqlBulkCopy(destConn)
            {
                DestinationTableName = batch.DestinationTable,
                BatchSize = 5000,
                BulkCopyTimeout = 600
            };

            // Map columns by ordinal (assumes matching schema).
            foreach (System.Data.DataColumn col in dataTable.Columns)
            {
                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
            }

            await bulkCopy.WriteToServerAsync(dataTable);
            result.RowsLoaded = dataTable.Rows.Count;

            logger.LogInformation("Loaded {Rows} rows into Azure SQL table {Table}.",
                result.RowsLoaded, batch.DestinationTable);

            result.Success = true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "FAILED processing batch {Source} → {Dest}.",
                batch.SourceTable, batch.DestinationTable);
            result.Success = false;
            result.ErrorMessage = ex.Message;
        }

        return result;
    }
}
