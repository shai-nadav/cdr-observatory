using System.Collections.Generic;

namespace Pipeline.Components.OSVParser.Models
{
    /// <summary>
    /// Complete result from processing a batch of CDR files.
    /// </summary>
    public class ProcessingResult
    {
        public List<ProcessedCall> Calls { get; set; } = new List<ProcessedCall>();

        /// <summary>Raw CDR records that were processed (for optional debug output).</summary>
        public List<RawCdrRecord> RawRecords { get; set; } = new List<RawCdrRecord>();

        // Stats
        public int TotalRecordsProcessed { get; set; }
        public int TotalFilesProcessed { get; set; }
        public int TotalCallsIdentified { get; set; }
        public int TotalLegsCreated { get; set; }
        public int OrphanedLegs { get; set; }
        public List<string> Warnings { get; set; } = new List<string>();
        public List<string> Errors { get; set; } = new List<string>();

        // Performance
        public long ProcessingTimeMs { get; set; }
        public double RecordsPerSecond => ProcessingTimeMs > 0
            ? TotalRecordsProcessed / (ProcessingTimeMs / 1000.0)
            : 0;
        
        /// <summary>True if processing was aborted before completion.</summary>
        public bool WasAborted { get; set; }
    }
}

