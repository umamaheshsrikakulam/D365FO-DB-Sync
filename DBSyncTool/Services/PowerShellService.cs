using System.Management.Automation;

namespace DBSyncTool.Services
{
    public class PowerShellService
    {
        private readonly Action<string> _logger;
        private readonly Action<string> _rawLogger;

        public PowerShellService(Action<string> logger, Action<string> rawLogger)
        {
            _logger = logger;
            _rawLogger = rawLogger;
        }

        public async Task<(bool Success, string? Error)> ExecuteScriptAsync(
            string scriptPath, string backupFilePath, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(scriptPath))
            {
                return (false, "Script path is empty.");
            }

            if (!File.Exists(scriptPath))
            {
                return (false, $"Script file not found: {scriptPath}");
            }

            _logger($"[PowerShell] Executing: {scriptPath}");
            _logger($"[PowerShell] Parameter -BackupFilePath: {backupFilePath}");

            try
            {
                using var ps = PowerShell.Create();

                // Set execution policy to Bypass for this session
                ps.AddCommand("Set-ExecutionPolicy")
                  .AddParameter("ExecutionPolicy", "Bypass")
                  .AddParameter("Scope", "Process")
                  .AddParameter("Force");
                ps.Invoke();
                ps.Commands.Clear();

                ps.AddCommand(scriptPath);
                ps.AddParameter("BackupFilePath", backupFilePath);

                // Subscribe to output streams — script output goes raw (no prefix/timestamp)
                ps.Streams.Information.DataAdded += (sender, e) =>
                {
                    if (sender is PSDataCollection<InformationRecord> records && e.Index < records.Count)
                    {
                        _rawLogger($"{records[e.Index].MessageData}");
                    }
                };

                ps.Streams.Error.DataAdded += (sender, e) =>
                {
                    if (sender is PSDataCollection<ErrorRecord> records && e.Index < records.Count)
                    {
                        _rawLogger($"ERROR: {records[e.Index]}");
                    }
                };

                ps.Streams.Warning.DataAdded += (sender, e) =>
                {
                    if (sender is PSDataCollection<WarningRecord> records && e.Index < records.Count)
                    {
                        _rawLogger($"WARNING: {records[e.Index]}");
                    }
                };

                ps.Streams.Verbose.DataAdded += (sender, e) =>
                {
                    if (sender is PSDataCollection<VerboseRecord> records && e.Index < records.Count)
                    {
                        _rawLogger($"VERBOSE: {records[e.Index]}");
                    }
                };

                await Task.Run(() => ps.Invoke(), cancellationToken);

                if (ps.HadErrors)
                {
                    var errors = string.Join(Environment.NewLine,
                        ps.Streams.Error.Select(e => e.ToString()));
                    return (false, $"Script completed with errors:\n{errors}");
                }

                return (true, null);
            }
            catch (OperationCanceledException)
            {
                _logger("[PowerShell] Script execution cancelled.");
                return (false, "Script execution was cancelled.");
            }
            catch (Exception ex)
            {
                string errorMsg = $"Script execution failed: {ex.Message}";
                _logger($"[PowerShell] ERROR: {errorMsg}");
                return (false, errorMsg);
            }
        }
    }
}
