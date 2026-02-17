using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser.Processing
{
    /// <summary>
    /// Manages incomplete calls across sessions.
    /// Persists calls that haven"t completed, reloads them on next run.
    /// </summary>
    public class PendingCallsManager
    {
        private readonly IProcessorLogger _log;
        private readonly string _pendingFolder;
        private readonly int _retentionHours;
        private readonly Dictionary<string, PendingCall> _pendingCalls;

        public PendingCallsManager(IProcessorLogger log, string orphanFolder, int retentionHours)
        {
            _log = log;
            _retentionHours = retentionHours;
            _pendingFolder = string.IsNullOrEmpty(orphanFolder) 
                ? null 
                : Path.Combine(orphanFolder, "pending");
            _pendingCalls = new Dictionary<string, PendingCall>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Load pending calls from previous sessions.
        /// Calls older than retention period are moved to orphan output.
        /// </summary>
        public void LoadPendingCalls(Action<ProcessedCall> orphanCallback)
        {
            if (_pendingFolder == null || !Directory.Exists(_pendingFolder))
                return;

            var cutoff = DateTime.UtcNow.AddHours(-_retentionHours);
            var files = Directory.GetFiles(_pendingFolder, "pending_*.json");
            int loaded = 0, abandoned = 0;

            foreach (var file in files)
            {
                try
                {
                    var json = File.ReadAllText(file);
                    var calls = JsonSerializer.Deserialize<List<PendingCall>>(json);
                    if (calls == null) continue;

                    foreach (var pending in calls)
                    {
                        if (pending.FirstSeen < cutoff)
                        {
                            // Too old - abandon to orphan file
                            if (pending.Call != null)
                            {
                                orphanCallback?.Invoke(pending.Call);
                                abandoned++;
                            }
                        }
                        else
                        {
                            // Still within retention - reload for matching
                            _pendingCalls[pending.ThreadId] = pending;
                            loaded++;
                        }
                    }

                    // Delete the file after processing
                    File.Delete(file);
                }
                catch (Exception ex)
                {
                    _log.Error($"Failed to load pending file {file}", ex);
                }
            }

            if (loaded > 0 || abandoned > 0)
                _log.Info($"Pending calls: loaded {{{loaded}}}, abandoned {{{abandoned}}}");
        }

        /// <summary>
        /// Check if a ThreadId has pending legs from previous session.
        /// </summary>
        public bool HasPending(string threadId) => _pendingCalls.ContainsKey(threadId);

        /// <summary>
        /// Get pending call for a ThreadId.
        /// </summary>
        public PendingCall GetPending(string threadId)
        {
            PendingCall pending;
            _pendingCalls.TryGetValue(threadId, out pending);
            return pending;
        }

        /// <summary>
        /// Mark a pending call as recovered (matched with new CDRs).
        /// </summary>
        public void MarkRecovered(string threadId)
        {
            PendingCall pending;
            if (_pendingCalls.TryGetValue(threadId, out pending))
            {
                var age = DateTime.UtcNow - pending.FirstSeen;
                _log.Info($"Orphan recovered: ThreadId {{{threadId}}} completed after {{{age.TotalHours:F1}}}h");
                _pendingCalls.Remove(threadId);
            }
        }

        /// <summary>
        /// Add a new incomplete call to pending.
        /// </summary>
        public void AddPending(string threadId, ProcessedCall call, List<ProcessedLeg> legs)
        {
            _pendingCalls[threadId] = new PendingCall
            {
                ThreadId = threadId,
                FirstSeen = DateTime.UtcNow,
                Call = call,
                Legs = legs
            };
        }

        /// <summary>
        /// Persist all pending calls for next session.
        /// </summary>
        public void SavePendingCalls()
        {
            if (_pendingFolder == null || _pendingCalls.Count == 0)
                return;

            if (!Directory.Exists(_pendingFolder))
                Directory.CreateDirectory(_pendingFolder);

            var today = DateTime.UtcNow.ToString("yyyyMMdd");
            var filePath = Path.Combine(_pendingFolder, $"pending_{today}.json");

            
            var json = JsonSerializer.Serialize(_pendingCalls.Values.ToList());
            File.WriteAllText(filePath, json);

            _log.Info($"Saved {{{_pendingCalls.Count}}} pending calls to {{{filePath}}}");
        }

        public int Count => _pendingCalls.Count;
    }

    /// <summary>
    /// A call awaiting completion from previous session.
    /// </summary>
    public class PendingCall
    {
        public string ThreadId { get; set; }
        public DateTime FirstSeen { get; set; }
        public ProcessedCall Call { get; set; }
        public List<ProcessedLeg> Legs { get; set; }
    }
}





