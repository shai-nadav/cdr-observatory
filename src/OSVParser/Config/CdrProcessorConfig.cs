using System.Collections.Generic;

namespace Pipeline.Components.OSVParser.Config
{
    /// <summary>
    /// Configuration for a CDR processor instance. Fully instance-scoped - no statics.
    /// </summary>
    public class CdrProcessorConfig
    {
        /// <summary>Input folder containing CDR CSV files from PBX</summary>
        public string InputFolder { get; set; }

        /// <summary>Output folder for processed JSON files</summary>
        public string OutputFolder { get; set; }

        /// <summary>
        /// Extension ranges that define internal numbers.
        /// Supports individual numbers and ranges: "1000-4999", "5000", "34944068000-34944068999"
        /// VM numbers should be included here - they are considered internal.
        /// </summary>
        public List<string> ExtensionRanges { get; set; } = new List<string>();

        /// <summary>
        /// Path to external file containing extension ranges (one per line or comma-separated).
        /// If set, ranges are loaded from this file instead of ExtensionRanges property.
        /// Supports comments with # and empty lines.
        /// </summary>
        public string ExtensionRangesFile { get; set; }

        /// <summary>
        /// Path to file where discovered extension ranges are saved.
        /// After processing, any auto-detected extensions are appended here.
        /// Can be same as ExtensionRangesFile to build up ranges over time.
        /// </summary>
        public string DiscoveredExtensionsFile { get; set; }

        /// <summary>Instance identifier for logging and multi-instance isolation</summary>
        public string InstanceId { get; set; } = "default";

        /// <summary>Log file path (Serilog file sink)</summary>
        public string LogPath { get; set; }


        /// <summary>Cache TTL - how long to keep orphaned GIDs before eviction</summary>
        public int CacheTtlMinutes { get; set; } = 1440; // 24 hours default

        /// <summary>File pattern to match CDR files in input folder</summary>
        public string FilePattern { get; set; } = "*.csv";

        /// <summary>
        /// Archive folder for processed CDR files.
        /// After successful processing, files are moved here.
        /// If null/empty, files remain in InputFolder.
        /// </summary>
        public string ArchiveFolder { get; set; }

        /// <summary>
        /// Voicemail pilot number (e.g. "20002").
        /// When a leg''s CalledParty equals this, it''s a VM leg.
        /// Used for Transfer From/To chain computation.
        /// </summary>
        public string VoicemailNumber { get; set; }
        /// <summary>
        /// Path to SipEndpoints XML file for PSTN gateway detection.
        /// Used for direction detection based on Ingress/Egress endpoints.
        /// When configured, direction is determined by whether endpoints are PSTN gateways.
        /// </summary>
        public string SipEndpointsFile { get; set; }

        /// <summary>\r\n        /// Routing-only extension numbers (CMS, pilot numbers, etc.).
        /// When these act as pure routers (0 duration), legs with DestinationExt
        /// matching are suppressed from output. Info is preserved in Transfer From/To
        /// fields of adjacent legs. Answered legs (dur>0) are kept as real call records.
        /// </summary>
        public List<string> RoutingNumbers { get; set; } = new List<string>();

        /// <summary>
        /// Hunt Group numbers for leg merging logic.
        /// When ForwardingParty is an HG number, it''s not considered "real" forwarding
        /// (used for attempt+answer leg merging). HG numbers don''t affect direction detection.
        /// </summary>
        public List<string> HuntGroupNumbers { get; set; } = new List<string>();

        // -------------------------------------------------------------------
        // Streaming / Memory Management
        // -------------------------------------------------------------------

        /// <summary>
        /// Maximum legs to hold in cache before evicting oldest calls.
        /// Default = 1000 (production). Set to 0 for unlimited (batch mode).
        /// Eviction outputs oldest calls when limit is exceeded.
        /// </summary>
        public int MaxCachedLegs { get; set; } = 1000;

        /// <summary>
        /// Flush mode: when to output remaining cached calls.
        /// - "batch" (default): flush only at end of all files (production mode)
        /// - "per-file": flush after each file (testing mode)
        /// </summary>
        public string FlushMode { get; set; } = "batch";

        /// <summary>
        /// Enable call completion detection: output calls as soon as direction is determined.
        /// Default = false (production). When false, calls only output via eviction
        /// or end-of-batch, ensuring T2T calls have correct direction.
        /// Set to true for lower latency (89% of calls output immediately, but T2T may be wrong).
        /// </summary>
        public bool EnableCallCompletionDetection { get; set; } = false;

        // -------------------------------------------------------------------
        // Debug / Traceability Output
        // -------------------------------------------------------------------

        /// <summary>
        /// Write decoded CDR files per input file.
        /// Output path: {inputFolder}\output\cdrsDecoded\{inputFileName}.csv
        /// Shows raw CDR fields used by processor with codes/flags decoded to text.
        /// Default = false (production). Set to true for debugging/validation.
        /// </summary>
        public bool WriteDecodedCdrs { get; set; } = false;

        // -------------------------------------------------------------------
        // Archival / Lifecycle Management
        // -------------------------------------------------------------------

        /// <summary>
        /// Folder for orphan CDR output (calls that aged out without completion).
        /// Orphans are written to daily files: orphans_YYYYMMDD.csv
        /// If null/empty, orphans go to regular output.
        /// </summary>
        public string OrphanFolder { get; set; }

        /// <summary>
        /// Days before archival files are zipped. Default = 30.
        /// Applies to: OrphanFolder, ArchiveFolder, and decoded CDRs folder.
        /// After zipping, original files are deleted.
        /// </summary>
        public int ArchiveZipDays { get; set; } = 30;

        /// <summary>
        /// Hours to retain incomplete calls before abandoning to orphan file.
        /// Incomplete calls are persisted between sessions and matched against new CDRs.
        /// Default = 24 hours.
        /// </summary>
        public int IncompleteRetentionHours { get; set; } = 24;
    }
}


