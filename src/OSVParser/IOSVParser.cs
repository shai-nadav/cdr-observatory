using Pipeline.Components.OSVParser.Processing;
using System.Collections.Generic;

namespace Pipeline.Components.OSVParser
{
    /// <summary>
    /// OSV Parser interface for TEM-CA integration.
    /// Wraps the CDR processor engine with DCFile-compatible lifecycle.
    /// </summary>
    public interface IOSVParser
    {
        /// <summary>
        /// Initialize the parser with runtime dependencies.
        /// Call once before Process().
        /// </summary>
        /// <param name="instanceId">Instance ID for parallel execution</param>
        /// <param name="instanceName">Instance name for logging</param>
        /// <param name="inputPath">Source directory for .bf files</param>
        /// <param name="abortHelper">Stop signal handler</param>
        /// <param name="logger">Logging interface</param>
        /// <param name="logName">Log category name</param>
        void Initialize(
            int instanceId,
            string instanceName,
            string inputPath,
            IAbortHelper abortHelper,
            IProcessorLogger logger,
            string logName);
        
        /// <summary>
        /// Initialize with full dependency injection (for TEM-CA adapters).
        /// </summary>
        void Initialize(
            int instanceId,
            string instanceName,
            string inputPath,
            IAbortHelper abortHelper,
            IProcessorLogger logger,
            string logName,
            ISettingsProvider settings,
            ISipEndpointsProvider sipEndpoints,
            IPendingCallsRepository pendingCalls,
            IProcessingTracer processingTracer);
        
        /// <summary>
        /// Process all .bf files in input directory.
        /// Outputs *_calls.csv files to Processed subfolder.
        /// Cleans work directory at start.
        /// Checks abort signal between files.
        /// </summary>
        /// <returns>true if completed successfully, false if aborted or failed</returns>
        bool Process();
        
        /// <summary>
        /// Clean work directory (call on startup/recovery).
        /// </summary>
        void CleanWorkDirectory();
        
        /// <summary>
        /// Archive maintenance - zip files older than today.
        /// Called automatically at end of Process(), can also call manually.
        /// </summary>
        void ArchiveEndOfDay();
        
        /// <summary>
        /// Get processing statistics from last run.
        /// </summary>
        OSVParserStats GetStats();

        /// <summary>
        /// Get the rewritten input file mask to be used by DCFile after preprocess.
        /// Returns null/empty when parser did not produce a rewritten input set.
        /// </summary>
        string GetPreprocessedInputFile();
    }
    
    /// <summary>
    /// Processing statistics.
    /// </summary>
    public class OSVParserStats
    {
        public int FilesProcessed { get; set; }
        public int RecordsProcessed { get; set; }
        public int CallsIdentified { get; set; }
        public int LegsCreated { get; set; }
        public int PendingCallsLoaded { get; set; }
        public int PendingCallsRemaining { get; set; }
        public int OrphansCreated { get; set; }
        public long ProcessingTimeMs { get; set; }
        public bool WasAborted { get; set; }
        public string PreprocessedInputFile { get; set; }
        public List<string> Errors { get; set; } = new List<string>();
    }
}

