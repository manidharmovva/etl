using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace EtlFanOutFunction;

/// <summary>
/// Provides helpers to connect to on-prem SQL Server and Azure SQL,
/// with explicit logging of connection success / failure.
/// </summary>
public static class DbConnectionHelper
{
    /// <summary>
    /// Opens a connection to the on-premises SQL Server and logs the result.
    /// </summary>
    public static async Task<SqlConnection> ConnectOnPremAsync(ILogger logger)
    {
        var connectionString = Environment.GetEnvironmentVariable("ONPREM_SQL_CONNECTION_STRING")
            ?? throw new InvalidOperationException("ONPREM_SQL_CONNECTION_STRING is not configured.");

        var connection = new SqlConnection(connectionString);
        try
        {
            await connection.OpenAsync();
            logger.LogInformation("SUCCESS: Connected to On-Prem SQL Server – {Server}/{Database}",
                connection.DataSource, connection.Database);
            return connection;
        }
        catch (SqlException ex)
        {
            logger.LogError(ex, "FAILURE: Could not connect to On-Prem SQL Server. " +
                "Check ONPREM_SQL_CONNECTION_STRING and network/firewall settings.");
            await connection.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// Opens a connection to Azure SQL Database and logs the result.
    /// </summary>
    public static async Task<SqlConnection> ConnectAzureSqlAsync(ILogger logger)
    {
        var connectionString = Environment.GetEnvironmentVariable("AZURE_SQL_CONNECTION_STRING")
            ?? throw new InvalidOperationException("AZURE_SQL_CONNECTION_STRING is not configured.");

        var connection = new SqlConnection(connectionString);
        try
        {
            await connection.OpenAsync();
            logger.LogInformation("SUCCESS: Connected to Azure SQL Database – {Server}/{Database}",
                connection.DataSource, connection.Database);
            return connection;
        }
        catch (SqlException ex)
        {
            logger.LogError(ex, "FAILURE: Could not connect to Azure SQL Database. " +
                "Check AZURE_SQL_CONNECTION_STRING and firewall rules.");
            await connection.DisposeAsync();
            throw;
        }
    }
}
