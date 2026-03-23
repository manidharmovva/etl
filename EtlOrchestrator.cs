using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;

namespace EtlFanOutFunction;

public class EtlOrchestrator
{
    /// <summary>
    /// HTTP-triggered starter – kicks off the orchestration.
    /// POST /api/start-etl  (body: optional JSON array of EtlBatchInfo)
    /// </summary>
    [Function(nameof(StartEtl))]
    public async Task<HttpResponseData> StartEtl(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "start-etl")] HttpRequestData req,
        [DurableClient] DurableTaskClient client,
        FunctionContext context)
    {
        var logger = context.GetLogger(nameof(StartEtl));

        // Accept an optional list of batches from the request body.
        // If none provided, the orchestrator will discover tables itself.
        List<EtlBatchInfo>? batches = null;
        try
        {
            batches = await req.ReadFromJsonAsync<List<EtlBatchInfo>>();
        }
        catch
        {
            // Body is empty or not valid JSON – orchestrator will use defaults.
        }

        var instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
            nameof(RunEtlOrchestrator), batches);

        logger.LogInformation("Started ETL orchestration. Instance ID: {InstanceId}", instanceId);

        return await client.CreateCheckStatusResponseAsync(req, instanceId);
    }

    /// <summary>
    /// Durable orchestrator – fan-out / fan-in.
    /// 1. Calls "DiscoverTables" activity to get batch list (if not supplied).
    /// 2. Fans out: one "ProcessBatch" activity per table/batch.
    /// 3. Fans in:  collects all results and produces a summary.
    /// </summary>
    [Function(nameof(RunEtlOrchestrator))]
    public async Task<List<EtlBatchResult>> RunEtlOrchestrator(
        [OrchestrationTrigger] TaskOrchestrationContext context)
    {
        var logger = context.CreateReplaySafeLogger(nameof(RunEtlOrchestrator));

        // --- Step 1: Determine which batches to process -----------------------
        var batches = context.GetInput<List<EtlBatchInfo>>();
        if (batches is null || batches.Count == 0)
        {
            logger.LogInformation("No batches supplied – discovering tables from on-prem DB.");
            batches = await context.CallActivityAsync<List<EtlBatchInfo>>(
                nameof(EtlActivities.DiscoverTables), string.Empty);
        }

        logger.LogInformation("Fanning out {Count} ETL batches.", batches.Count);

        // --- Step 2: Fan-out – launch all batch activities in parallel ----------
        var tasks = new List<Task<EtlBatchResult>>();
        foreach (var batch in batches)
        {
            tasks.Add(context.CallActivityAsync<EtlBatchResult>(
                nameof(EtlActivities.ProcessBatch), batch));
        }

        // --- Step 3: Fan-in – wait for every activity to complete ---------------
        var results = (await Task.WhenAll(tasks)).ToList();

        // --- Summary -----------------------------------------------------------
        var successCount = results.Count(r => r.Success);
        var failCount = results.Count(r => !r.Success);
        var totalRows = results.Where(r => r.Success).Sum(r => r.RowsLoaded);

        logger.LogInformation(
            "ETL complete. Batches succeeded: {Success}, failed: {Failed}, total rows loaded: {Rows}",
            successCount, failCount, totalRows);

        return results;
    }
}
