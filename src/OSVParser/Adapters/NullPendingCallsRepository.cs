using System;
using System.Collections.Generic;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser
{
    /// <summary>
    /// No-op pending calls repository (standalone/testing fallback).
    /// </summary>
    public class NullPendingCallsRepository : IPendingCallsRepository
    {
        public IList<PendingCallRecord> Load(int instanceId) => new List<PendingCallRecord>();
        public void Sync(int instanceId, IList<string> deletedIds, IList<PendingCallRecord> upsertCalls) { }
        public long GetCurrentRecordSeq() => 0;
        public long IncrementRecordSeq() => 0;
    }
}
