using System;
using System.Collections.Generic;
using System.Linq;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser.Cache
{
    /// <summary>
    /// In-memory cache for standalone mode. Lightweight, no external dependencies.
    /// Not crash-safe -- reprocess files on restart.
    /// </summary>
    public class InMemoryCacheStore : ICacheStore
    {
        private readonly Dictionary<string, List<ProcessedLeg>> _cache
            = new Dictionary<string, List<ProcessedLeg>>(StringComparer.OrdinalIgnoreCase);

        private readonly object _lock = new object();

        public void StorePendingLeg(string gid, ProcessedLeg leg)
        {
            if (string.IsNullOrEmpty(gid)) return;

            lock (_lock)
            {
                List<ProcessedLeg> legs;
                if (!_cache.TryGetValue(gid, out legs))
                {
                    legs = new List<ProcessedLeg>();
                    _cache[gid] = legs;
                }

                legs.Add(leg);
            }
        }

        public List<ProcessedLeg> GetPendingLegs(string gid)
        {
            if (string.IsNullOrEmpty(gid)) return new List<ProcessedLeg>();

            lock (_lock)
            {
                List<ProcessedLeg> legs;
                if (_cache.TryGetValue(gid, out legs))
                {
                    return legs
                        .OrderBy(l => l.InLegConnectTime ?? "")
                        .ThenBy(l => l.SourceLine)
                        .ToList();
                }
                return new List<ProcessedLeg>();
            }
        }

        public void RemovePendingLeg(string gid, string inLegConnectTime)
        {
            if (string.IsNullOrEmpty(gid)) return;

            lock (_lock)
            {
                List<ProcessedLeg> legs;
                if (_cache.TryGetValue(gid, out legs))
                {
                    legs.RemoveAll(l => l.InLegConnectTime == inLegConnectTime);
                    if (legs.Count == 0) _cache.Remove(gid);
                }
            }
        }

        public void RemoveCall(string gid)
        {
            if (string.IsNullOrEmpty(gid)) return;

            lock (_lock)
            {
                _cache.Remove(gid);
            }
        }

        public IEnumerable<string> GetAllGids()
        {
            lock (_lock)
            {
                return _cache.Keys.ToList();
            }
        }

        public void EvictStaleEntries(TimeSpan maxAge)
        {
            var cutoff = DateTime.UtcNow - maxAge;
            var cutoffStr = cutoff.ToString("yyyy-MM-ddTHH:mm:ss");

            lock (_lock)
            {
                var staleGids = _cache
                    .Where(kvp => kvp.Value.All(l =>
                        string.Compare(l.InLegConnectTime ?? "", cutoffStr, StringComparison.Ordinal) < 0))
                    .Select(kvp => kvp.Key)
                    .ToList();

                foreach (var gid in staleGids)
                {
                    _cache.Remove(gid);
                }
            }
        }

        public int Count
        {
            get
            {
                lock (_lock)
                {
                    return _cache.Values.Sum(l => l.Count);
                }
            }
        }
    }
}


