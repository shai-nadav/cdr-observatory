using System;
using System.Collections.Generic;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser.Cache
{
    /// <summary>
    /// Cache for pending CDR legs awaiting GID matching.
    /// Implementations: InMemoryCacheStore (standalone), SqlServerCacheStore (v2 integrated).
    /// </summary>
    public interface ICacheStore
    {
        /// <summary>Store a processed leg pending further matching</summary>
        void StorePendingLeg(string gid, ProcessedLeg leg);

        /// <summary>Get all pending legs for a GID, ordered by InLegConnectTime</summary>
        List<ProcessedLeg> GetPendingLegs(string gid);

        /// <summary>Remove a specific leg from cache (after final output)</summary>
        void RemovePendingLeg(string gid, string inLegConnectTime);

        /// <summary>Remove all legs for a GID (after call is fully assembled)</summary>
        void RemoveCall(string gid);

        /// <summary>Get all GIDs currently in cache</summary>
        IEnumerable<string> GetAllGids();

        /// <summary>Evict entries older than maxAge</summary>
        void EvictStaleEntries(TimeSpan maxAge);

        /// <summary>Total legs currently in cache</summary>
        int Count { get; }
    }
}

