using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Pipeline.Components.OSVParser;
using Pipeline.Components.OSVParser.Cache;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser.Processing
{
    /// <summary>
    /// OSV Parser implementation for TEM-CA integration.
    /// Wraps CdrProcessorEngine with DCFile-compatible lifecycle (Initialize + Process).
    /// Handles file I/O, recovery, and archival operations.
    /// </summary>
    public class OSVParserEngine : IOSVParser
    {
        // Initialization state
        private bool _initialized;
        private int _instanceId;
        private string _instanceName;
        private string _inputPath;
        private IAbortHelper _abortHelper;
        private IProcessorLogger _logger;
        private string _logName;
        
        // Dependencies (can be injected or defaults)
        private ISettingsProvider _settings;
        private ISipEndpointsProvider _sipEndpoints;
        private IPendingCallsRepository _pendingCalls;
        private IProcessingTracer _processingTracer;

        // Engine and stats
        private CdrProcessorEngine _engine;
        private OSVParserStats _lastStats;
        private string _preprocessedInputFile;
        
        // Directories (initialized from ISettingsProvider at process start)
        private string _workDirectory;
        private string _archiveDirectory;
        private string _processedDirectory;
        private string _decodedDirectory;
        private string _orphansDirectory;

        private string WorkDirectory => _workDirectory;
        private string ArchiveDirectory => _archiveDirectory;
        private string ProcessedDirectory => _processedDirectory;
        private string DecodedDirectory => _decodedDirectory;
        private string OrphansDirectory => _orphansDirectory;
        private string FailedSessionMarkerPath => Path.Combine(_inputPath, _instanceId.ToString(), "last_failed_session.txt");
        private string DecodedOutputFolder => Path.Combine(_inputPath, "output", "cdrsDecoded");
        /// <summary>
        /// Initialize with minimal dependencies (for standalone/testing).
        /// Uses default settings, SIP endpoints from file, and file-based pending calls.
        /// </summary>
        public void Initialize(
            int instanceId,
            string instanceName,
            string inputPath,
            IAbortHelper abortHelper,
            IProcessorLogger logger,
            string logName)
        {
            Initialize(instanceId, instanceName, inputPath, abortHelper, logger, logName,
                settings: null, sipEndpoints: null, pendingCalls: null);
        }
        
        /// <summary>
        /// Initialize with full dependency injection (for TEM-CA adapters).
        /// </summary>
        public void Initialize(
            int instanceId,
            string instanceName,
            string inputPath,
            IAbortHelper abortHelper,
            IProcessorLogger logger,
            string logName,
            ISettingsProvider settings,
            ISipEndpointsProvider sipEndpoints,
            IPendingCallsRepository pendingCalls,
            IProcessingTracer processingTracer = null)
        {
            if (_initialized)
                throw new InvalidOperationException("Already initialized. Call CleanWorkDirectory() to reset.");

            _processingTracer = processingTracer;
            _instanceId = instanceId;
            _instanceName = instanceName ?? $"OSVParser-{instanceId}";
            if (inputPath == null) throw new ArgumentNullException(nameof(inputPath));
            if (logger == null) throw new ArgumentNullException(nameof(logger));
            _inputPath = inputPath;
            _abortHelper = abortHelper ?? NullAbortHelper.Instance;
            _logger = logger;
            _logName = logName ?? "OSVParser";
            
            _settings = settings;
            _sipEndpoints = sipEndpoints;
            _pendingCalls = pendingCalls;
            
            _lastStats = new OSVParserStats();
            _preprocessedInputFile = null;
            _workDirectory = null;
            _archiveDirectory = null;
            _processedDirectory = null;
            _decodedDirectory = null;
            _orphansDirectory = null;
            _initialized = true;
            
            _logger.Info($"[{_logName}] Initialized instance {_instanceId} ({_instanceName})");
            _logger.Info($"[{_logName}] Input: {_inputPath}");
        }
        
        /// <summary>
        /// Process all .bf files in input directory.
        /// </summary>
        public bool Process()
        {
            EnsureInitialized();
            
            var stats = new OSVParserStats();
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.Info($"[{_logName}] Session start, instance {_instanceId}");

                // Phase 0: Create settings if not injected
                var effectiveSettings = _settings ?? CreateDefaultSettings();
                var effectiveSipEndpoints = _sipEndpoints ?? CreateDefaultSipEndpoints();
                var effectivePendingCalls = _pendingCalls ?? CreateDefaultPendingCalls();

                // Initialize directory members from settings provider.
                InitializeDirectories(effectiveSettings);

                // Phase 1: Cleanup artifacts from previous failed session (if any)
                CleanupFailedSessionArtifacts();

                // Phase 2: Clean work directory (recovery)
                CleanWorkDirectoryInternal();

                // Phase 3: Mark session start for recovery
                WriteFailedSessionMarker(System.DateTime.UtcNow);

                // Phase 4: Ensure directories exist
                EnsureDirectories(effectiveSettings);
                
                // Phase 5: Create and run engine
                var cache = new InMemoryCacheStore();
                _engine = new CdrProcessorEngine(
                    effectiveSettings,
                    _logger,
                    effectiveSipEndpoints,
                    effectivePendingCalls,
                    cache,
                    _processingTracer);
                
                var result = _engine.ProcessFolder(_inputPath, null, _abortHelper);
                _preprocessedInputFile = Path.Combine(ProcessedDirectory, "legs_*.csv");

                // Phase 4: Output is streamed by CdrProcessorEngine (legs writer)
                // No end-of-run WriteOutput needed.

                // Phase 5: Archive maintenance
                if (!result.WasAborted)
                {
                    ArchiveEndOfDay();
                }
                
                // Collect stats
                stats.FilesProcessed = result.TotalFilesProcessed;
                stats.RecordsProcessed = result.TotalRecordsProcessed;
                stats.CallsIdentified = result.TotalCallsIdentified;
                stats.LegsCreated = result.TotalLegsCreated;
                stats.WasAborted = result.WasAborted;
                stats.Errors = result.Errors;
                stats.PreprocessedInputFile = _preprocessedInputFile;
                
                stopwatch.Stop();
                stats.ProcessingTimeMs = stopwatch.ElapsedMilliseconds;
                _lastStats = stats;
                
                _logger.Info($"[{_logName}] Session end: {stats.FilesProcessed} files, " +
                    $"{stats.RecordsProcessed} records, {stats.CallsIdentified} calls, " +
                    $"{stats.LegsCreated} legs, {stats.ProcessingTimeMs}ms" +
                    (stats.WasAborted ? " (ABORTED)" : ""));
                
                var success = !result.WasAborted && result.Errors.Count == 0;
                if (success)
                {
                    DeleteFailedSessionMarker();
                }
                return success;
            }
            catch (Exception ex)
            {
                _logger.Error($"[{_logName}] Process failed", ex);
                stats.Errors.Add(ex.Message);
                stats.PreprocessedInputFile = _preprocessedInputFile;
                _lastStats = stats;
                return false;
            }
        }
        
        /// <summary>
        /// Clean work directory (for recovery).
        /// </summary>
        public void CleanWorkDirectory()
        {
            EnsureInitialized();
            CleanWorkDirectoryInternal();
        }
        
        private void CleanupFailedSessionArtifacts()
        {
            if (!File.Exists(FailedSessionMarkerPath)) return;

            DateTime startUtc;
            if (!DateTime.TryParse(File.ReadAllText(FailedSessionMarkerPath).Trim(), out startUtc))
            {
                _logger.Warn($"[{_logName}] Failed-session marker unreadable, skipping cleanup");
                return;
            }

            startUtc = DateTime.SpecifyKind(startUtc, DateTimeKind.Utc);
            _logger.Warn($"[{_logName}] Recovery mode: cleaning outputs from failed session starting {startUtc:o}");

            var deleted = 0;
            deleted += DeleteFilesNewerThan(ArchiveDirectory, startUtc);
            deleted += DeleteFilesNewerThan(OrphansDirectory, startUtc);
            deleted += DeleteFilesNewerThan(DecodedOutputFolder, startUtc);

            if (deleted > 0)
                _logger.Warn($"[{_logName}] Removed {deleted} files from previous failed session");

            DeleteFailedSessionMarker();
        }

        private int DeleteFilesNewerThan(string directory, DateTime startUtc)
        {
            if (string.IsNullOrEmpty(directory) || !Directory.Exists(directory)) return 0;

            var deleted = 0;
            foreach (var file in Directory.GetFiles(directory))
            {
                try
                {
                    if (File.GetLastWriteTimeUtc(file) >= startUtc)
                    {
                        File.Delete(file);
                        deleted++;
                    }
                }
                catch (Exception ex)
                {
                    _logger.Warn($"[{_logName}] Failed to delete {file}: {ex.Message}");
                }
            }
            return deleted;
        }

        private void WriteFailedSessionMarker(DateTime startUtc)
        {
            try
            {
                var dir = Path.Combine(_inputPath, _instanceId.ToString());
                if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
                File.WriteAllText(FailedSessionMarkerPath, startUtc.ToString("o"));
            }
            catch (Exception ex)
            {
                _logger.Warn($"[{_logName}] Failed to write failed-session marker: {ex.Message}");
            }
        }

        private void DeleteFailedSessionMarker()
        {
            try
            {
                if (File.Exists(FailedSessionMarkerPath))
                    File.Delete(FailedSessionMarkerPath);
            }
            catch (Exception ex)
            {
                _logger.Warn($"[{_logName}] Failed to delete failed-session marker: {ex.Message}");
            }
        }
        private void CleanWorkDirectoryInternal()
        {
            if (Directory.Exists(WorkDirectory))
            {
                var files = Directory.GetFiles(WorkDirectory);
                if (files.Length > 0)
                {
                    _logger.Warn($"[{_logName}] Recovery mode: {files.Length} leftover files in work dir, clearing");
                    foreach (var file in files)
                    {
                        try { File.Delete(file); }
                        catch (Exception ex) { _logger.Warn($"[{_logName}] Failed to delete {file}: {ex.Message}"); }
                    }
                }
            }
        }
        
        /// <summary>
        /// Archive maintenance - zip files older than today.
        /// </summary>
        public void ArchiveEndOfDay()
        {
            EnsureInitialized();
            
            ZipOldFiles(ArchiveDirectory, "archive");
            ZipOldFiles(DecodedDirectory, "decoded");
            ZipOldFiles(OrphansDirectory, "orphans");
        }
        
        /// <summary>
        /// Get stats from last run.
        /// </summary>
        public OSVParserStats GetStats()
        {
            return _lastStats ?? new OSVParserStats();
        }

        public string GetPreprocessedInputFile()
        {
            return _preprocessedInputFile;
        }
        
        // --- Private helpers ---
        
        private void EnsureInitialized()
        {
            if (!_initialized)
                throw new InvalidOperationException("Not initialized. Call Initialize() first.");
        }

        private void InitializeDirectories(ISettingsProvider settings)
        {
            if (settings == null)
                throw new ArgumentNullException(nameof(settings));

            _processedDirectory = string.IsNullOrWhiteSpace(settings.OutputFolder)
                ? Path.Combine(_inputPath, "Processed")
                : settings.OutputFolder;

            _archiveDirectory = string.IsNullOrWhiteSpace(settings.ArchiveFolder)
                ? Path.Combine(_inputPath, _instanceId.ToString(), "OSVParserArchive")
                : settings.ArchiveFolder;

            _orphansDirectory = string.IsNullOrWhiteSpace(settings.OrphanFolder)
                ? Path.Combine(_inputPath, _instanceId.ToString(), "OSVParserOrphans")
                : settings.OrphanFolder;

            _workDirectory = string.IsNullOrWhiteSpace(settings.WorkFolder)
                ? Path.Combine(_inputPath, _instanceId.ToString(), "OSVParserWork")
                : settings.WorkFolder;

            _decodedDirectory = string.IsNullOrWhiteSpace(settings.DecodedFolder)
                ? Path.Combine(_inputPath, _instanceId.ToString(), "OSVParserDecoded")
                : settings.DecodedFolder;
        }

        private void EnsureDirectories(ISettingsProvider settings)
        {
            CreateDirectoryIfNeeded(WorkDirectory);
            CreateDirectoryIfNeeded(ArchiveDirectory);
            CreateDirectoryIfNeeded(ProcessedDirectory);
            
            if (settings?.WriteDecodedCdrs ?? false)
                CreateDirectoryIfNeeded(DecodedDirectory);
            
            CreateDirectoryIfNeeded(OrphansDirectory);
        }
        
        private void CreateDirectoryIfNeeded(string path)
        {
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
                _logger.Debug($"[{_logName}] Created directory: {path}");
            }
        }
        
        private void ZipOldFiles(string directory, string prefix)
        {
            if (!Directory.Exists(directory)) return;
            
            var today = DateTime.Today;
            var files = Directory.GetFiles(directory)
                .Where(f => !f.EndsWith(".zip"))
                .ToList();
            
            // Group by date (files older than today)
            var oldFiles = files
                .Select(f => new { Path = f, Date = File.GetLastWriteTime(f).Date })
                .Where(f => f.Date < today)
                .GroupBy(f => f.Date)
                .ToList();
            
            foreach (var group in oldFiles)
            {
                var zipName = $"{prefix}_{group.Key:yyyy-MM-dd}.zip";
                var zipPath = Path.Combine(directory, zipName);
                
                try
                {
                    using (var archive = System.IO.Compression.ZipFile.Open(zipPath, 
                        File.Exists(zipPath) 
                            ? System.IO.Compression.ZipArchiveMode.Update 
                            : System.IO.Compression.ZipArchiveMode.Create))
                    {
                        foreach (var file in group)
                        {
                            var entryName = Path.GetFileName(file.Path);
                            archive.CreateEntryFromFile(file.Path, entryName);
                        }
                    }
                    
                    // Delete originals after successful zip
                    foreach (var file in group)
                    {
                        File.Delete(file.Path);
                    }
                    
                    _logger.Info($"[{_logName}] Archived {group.Count()} files to {zipName}");
                }
                catch (Exception ex)
                {
                    _logger.Warn($"[{_logName}] Failed to archive files for {group.Key:yyyy-MM-dd}: {ex.Message}");
                }
            }
        }
        
        // --- Default adapters for standalone use ---
        
        private ISettingsProvider CreateDefaultSettings()
        {
            return new OSVParserDefaultSettings(_inputPath, _instanceId.ToString());
        }
        
        private ISipEndpointsProvider CreateDefaultSipEndpoints()
        {
            return new FileSipEndpointsProvider(_settings?.SipEndpointsFile);
        }
        
        private IPendingCallsRepository CreateDefaultPendingCalls()
        {
            return new NullPendingCallsRepository();
        }
        
        /// <summary>
        /// Default settings for standalone use.
        /// </summary>
        private class OSVParserDefaultSettings : ISettingsProvider
        {
            private readonly string _inputPath;
            private readonly string _workPathParent;
            private readonly string _instanceId;
            private readonly string _sipImportDir;

            public OSVParserDefaultSettings(string inputPath, string instanceId)
            {
                var inputParent = Directory.GetParent(inputPath)?.FullName ?? inputPath;
                _sipImportDir = Path.Combine(inputParent, "SIPEndPoint");
                _workPathParent = Path.Combine(inputParent, instanceId);
                _inputPath = inputPath;
                _instanceId = instanceId;
            }
            
            public string InputFolder => _inputPath;
            public string OutputFolder => Path.Combine(_inputPath, "Processed");
            public string ArchiveFolder => Path.Combine(_workPathParent, "OSVParserArchive");
            public string WorkFolder => Path.Combine(_workPathParent, "OSVParserWork");
            public string DecodedFolder => Path.Combine(_workPathParent, "OSVParserDecoded");
            public string OrphanFolder => Path.Combine(_workPathParent, "OSVParserOrphans");
            public string SipEndpointsFile => Path.Combine(_sipImportDir, "SipEndpoints.xml");
            
            public IList<string> ExtensionRanges => new List<string>();
            public string VoicemailNumber => null;
            public IList<string> RoutingNumbers => new List<string>();
            public IList<string> HuntGroupNumbers => new List<string>();
            
            public string InstanceId => _instanceId;
            public string FilePattern => "*.bf";
            public int IncompleteRetentionHours => 24;
            public int MaxPendingQueueSize => 10000;
            
            public bool WriteDecodedCdrs => false;
            public bool DeleteInputFiles => true;
        }
    }
}







