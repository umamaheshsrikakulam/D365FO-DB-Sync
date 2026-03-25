using System.Collections.Concurrent;
using DBSyncTool.Models;

namespace DBSyncTool.Helpers
{
    /// <summary>
    /// Manages MaxRecId storage for fallback mode optimization (tables without SysRowVersion).
    /// Enables treating RecVersion=1 records as UNCHANGED when RecId <= StoredMaxRecId.
    /// </summary>
    public class MaxRecIdManager
    {
        private ConcurrentDictionary<string, long> _maxRecIds = new(StringComparer.OrdinalIgnoreCase);

        public void LoadFromConfig(AppConfiguration config)
        {
            _maxRecIds = new ConcurrentDictionary<string, long>(ParseMaxRecIdText(config.MaxTransferredRecIds), StringComparer.OrdinalIgnoreCase);
        }

        public void SaveToConfig(AppConfiguration config)
        {
            config.MaxTransferredRecIds = FormatMaxRecIdText(_maxRecIds);
        }

        public long? GetMaxRecId(string tableName)
        {
            return _maxRecIds.TryGetValue(tableName.ToUpper(), out var maxRecId) ? maxRecId : null;
        }

        public void SetMaxRecId(string tableName, long maxRecId)
        {
            _maxRecIds[tableName.ToUpper()] = maxRecId;
        }

        public void ClearTable(string tableName)
        {
            _maxRecIds.TryRemove(tableName.ToUpper(), out _);
        }

        public void ClearAll()
        {
            _maxRecIds.Clear();
        }

        private Dictionary<string, long> ParseMaxRecIdText(string text)
        {
            var result = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            if (string.IsNullOrWhiteSpace(text)) return result;

            foreach (var line in text.Split('\n', '\r'))
            {
                var trimmed = line.Trim();
                if (string.IsNullOrEmpty(trimmed)) continue;

                var parts = trimmed.Split(',');
                if (parts.Length == 2)
                {
                    var tableName = parts[0].Trim();
                    if (long.TryParse(parts[1].Trim(), out var maxRecId))
                    {
                        result[tableName] = maxRecId;
                    }
                }
            }
            return result;
        }

        private string FormatMaxRecIdText(ConcurrentDictionary<string, long> maxRecIds)
        {
            var lines = maxRecIds
                .OrderBy(kvp => kvp.Key)
                .Select(kvp => $"{kvp.Key},{kvp.Value}");
            return string.Join("\r\n", lines);
        }
    }
}
