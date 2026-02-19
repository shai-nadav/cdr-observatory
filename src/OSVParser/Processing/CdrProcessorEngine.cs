using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Pipeline.Components.OSVParser;
using Pipeline.Components.OSVParser.Cache;
using Pipeline.Components.OSVParser.Config;
using System.Text.RegularExpressions;
using Pipeline.Components.OSVParser.Models;
using Pipeline.Components.OSVParser.Parser;
using Pipeline.Components.OSVParser.Processing.Pipeline;


namespace Pipeline.Components.OSVParser.Processing
{
    /// <summary>
    /// Main CDR processing engine. Fully instance-scoped -- no statics.
    /// Processes CSV files from PBX, links legs by GID, outputs structured calls.
    /// Supports both config-based (standalone) and interface-based (TEM-CA) usage.
    /// </summary>
    public class CdrProcessorEngine
    {
        // Interface-based dependencies (preferred)
        private readonly ISettingsProvider _settings;
        private readonly IProcessorLogger _logger;
        private readonly IPendingCallsRepository _pendingRepo;
        
        // Legacy config (kept for backward compatibility)
        private readonly CdrProcessorConfig _config;
        
        private readonly ExtensionRangeParser _extensionRange;
        private readonly ISipEndpointResolver _sipResolver;
        private readonly HashSet<string> _unknownSipEndpoints = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly DirectionResolver _directionResolver;
        private readonly IProcessingTracer _tracer;
        private readonly ICacheStore _cache;
        private readonly CdrCsvParser _parser;
        private readonly Dictionary<string, CandidateExtension> _candidates;

        private readonly HashSet<string> _routingNumbers;
        private readonly HashSet<string> _huntGroupNumbers;
        // Auto-detected routing numbers from HG records (pilot numbers)
        private readonly HashSet<string> _detectedRoutingNumbers;
        // Auto-detected voicemail number (from legs with CF-to-Voicemail flag)
        private string _detectedVoicemailNumber;
        // Maps GID hex suffix -> ThreadId so HG records arriving after CDRs can find the legs
        private readonly Dictionary<string, string> _gidHexToThreadId;
        // Maps GID hex suffix -> full GID (for HG-only legs stored by full GID)
        private readonly Dictionary<string, string> _gidHexToFullGid;
        
        // Extension discovery: track numbers seen as caller vs callee
        private readonly HashSet<string> _seenAsCallers;
        private readonly HashSet<string> _seenAsCallees;
        private readonly HashSet<string> _discoveredExtensions;

        // Streaming output: calls output early (before end of batch)
        private readonly List<ProcessedCall> _earlyOutputCalls;
        // Track which ThreadIds have been output early (to avoid double output)
        private readonly HashSet<string> _outputtedThreadIds;
        private CsvOutputWriter.LegsStreamWriter _legsStreamWriter;
        private DecodedCdrWriter _decodedCdrWriter;
        private readonly Pipeline.PipelineContext _pipelineContext;
        private readonly Pipeline.LegMerger _legMerger;
        private readonly Pipeline.TransferChainResolver _transferChainResolver;
/// <summary>
        /// Create engine with interface-based dependencies (for TEM-CA or testing).
        /// </summary>
        public CdrProcessorEngine(
            ISettingsProvider settings,
            IProcessorLogger logger,
            ISipEndpointsProvider sipProvider,
            IPendingCallsRepository pendingRepo,
            ICacheStore cache,
            IProcessingTracer tracer = null)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (logger == null) throw new ArgumentNullException(nameof(logger));
            if (sipProvider == null) throw new ArgumentNullException(nameof(sipProvider));
            if (pendingRepo == null) throw new ArgumentNullException(nameof(pendingRepo));
            if (cache == null) throw new ArgumentNullException(nameof(cache));
            _settings = settings;
            _logger = logger;
            _pendingRepo = pendingRepo;
            _cache = cache;
            _tracer = tracer ?? NullProcessingTracer.Instance;
            
            // Build internal state from settings
            _extensionRange = new ExtensionRangeParser(settings.ExtensionRanges);
            // Unified SIP resolver: prefer file-based mapper if SipEndpointsFile is set, else wrap the provider
            if (!string.IsNullOrEmpty(settings.SipEndpointsFile))
            {
                var mapper = new SipEndpointMapper();
                mapper.LoadFromFile(settings.SipEndpointsFile);
                _sipResolver = mapper;
            }
            else
            {
                _sipResolver = new SipEndpointProviderAdapter(sipProvider);
            }
            _directionResolver = new DirectionResolver(_extensionRange, _sipResolver, _cache, IsInternalNumber, GetVoicemailNumber, _logger, _tracer);

            _parser = new CdrCsvParser();
            _candidates = new Dictionary<string, CandidateExtension>(StringComparer.OrdinalIgnoreCase);
            _routingNumbers = new HashSet<string>(settings.RoutingNumbers ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
            _huntGroupNumbers = new HashSet<string>(settings.HuntGroupNumbers ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
            _detectedRoutingNumbers = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _gidHexToThreadId = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            _gidHexToFullGid = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            _seenAsCallers = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _seenAsCallees = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _discoveredExtensions = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _earlyOutputCalls = new List<ProcessedCall>();
            _outputtedThreadIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        
            // Keep legacy _config for backward compat with internal methods
            _config = null; // Will use _settings instead
            _pipelineContext = BuildPipelineContext();
            _legMerger = new Pipeline.LegMerger(_pipelineContext);
            _transferChainResolver = new Pipeline.TransferChainResolver(_pipelineContext);

            try
            {
                _logger.Info(
                    "CdrProcessorEngine initialized (interface mode). SettingsProvider={0}; SipEndpointsProvider={1}; Settings={2}; SipProviderState={3}; EngineState={4}",
                    settings.GetType().FullName,
                    sipProvider.GetType().FullName,
                    BuildSettingsProviderSnapshot(settings),
                    BuildSipEndpointsProviderSnapshot(sipProvider),
                    BuildEngineStateSnapshot());
            }
            catch (Exception ex)
            {
                _logger.Warn("Failed to log CdrProcessorEngine constructor state: {0}", ex.Message);
            }
        }

        /// <summary>
        /// Create engine with legacy config (for backward compatibility).
        /// </summary>
        public CdrProcessorEngine(CdrProcessorConfig config, ICacheStore cache, IProcessorLogger logger = null, IProcessingTracer tracer = null)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (cache == null) throw new ArgumentNullException(nameof(cache));
            _config = config;
            _cache = cache;
            _logger = logger ?? new NullProcessorLogger();
            _tracer = tracer ?? NullProcessingTracer.Instance;
            _extensionRange = new ExtensionRangeParser(ExtensionRangeLoader.LoadRanges(config));
            var mapper = new SipEndpointMapper();
            if (!string.IsNullOrEmpty(config.SipEndpointsFile))
                mapper.LoadFromFile(config.SipEndpointsFile);
            _sipResolver = mapper;
            _directionResolver = new DirectionResolver(_extensionRange, _sipResolver, _cache, IsInternalNumber, GetVoicemailNumber, _logger, _tracer);
            _parser = new CdrCsvParser();
            _candidates = new Dictionary<string, CandidateExtension>(StringComparer.OrdinalIgnoreCase);
            _routingNumbers = new HashSet<string>(config.RoutingNumbers ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
            _huntGroupNumbers = new HashSet<string>(config.HuntGroupNumbers ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
            _detectedRoutingNumbers = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _gidHexToThreadId = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            _gidHexToFullGid = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            _seenAsCallers = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _seenAsCallees = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _discoveredExtensions = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _earlyOutputCalls = new List<ProcessedCall>();
            _outputtedThreadIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        _settings = null;
            _pendingRepo = null;
            _pipelineContext = BuildPipelineContext();
            _legMerger = new Pipeline.LegMerger(_pipelineContext);
            _transferChainResolver = new Pipeline.TransferChainResolver(_pipelineContext);
        }

        private PipelineContext BuildPipelineContext()
        {
            return new PipelineContext(
                _logger,
                _tracer,
                _sipResolver,
                _directionResolver,
                _extensionRange,
                _cache,
                _routingNumbers,
                _huntGroupNumbers,
                _detectedRoutingNumbers,
                _gidHexToThreadId,
                _gidHexToFullGid,
                _candidates,
                _seenAsCallers,
                _seenAsCallees,
                _discoveredExtensions,
                _unknownSipEndpoints,
                IsInternalNumber,
                GetVoicemailNumber,
                IsRoutingNumber,
                IsHuntGroupNumber,
                IsVmLeg,
                IsSipPstn,
                IsSipKnown,
                NormalizeEndpoint,
                IsInternalDestForEmptyRanges,
                () => _detectedVoicemailNumber,
                value => _detectedVoicemailNumber = value);
        }

        // Helper properties to get settings from either interface or legacy config
        private string InputFolder => _settings?.InputFolder ?? _config?.InputFolder;
        private string OutputFolder => _settings?.OutputFolder ?? _config?.OutputFolder;
        private string ArchiveFolder => _settings?.ArchiveFolder ?? _config?.ArchiveFolder;
        private string OrphanFolder => _settings?.OrphanFolder ?? _config?.OrphanFolder;
        private string FilePattern => _settings?.FilePattern ?? _config?.FilePattern ?? "*.csv";
        private bool WriteDecodedCdrs => _settings?.WriteDecodedCdrs ?? _config?.WriteDecodedCdrs ?? false;
        
        private bool DeleteInputFiles => _settings?.DeleteInputFiles ?? true;
private string VoicemailNumber => _settings?.VoicemailNumber ?? _config?.VoicemailNumber;
        private string DiscoveredExtensionsFile => _config?.DiscoveredExtensionsFile;
        private bool EnableCallCompletionDetection => _config?.EnableCallCompletionDetection ?? false;
        private int MaxCachedLegs => _config?.MaxCachedLegs ?? 0;

        private static string BuildSettingsProviderSnapshot(ISettingsProvider settings)
        {
            if (settings == null) return "null";

            return string.Format(
                "InstanceId={0}, InputFolder={1}, OutputFolder={2}, ArchiveFolder={3}, WorkFolder={4}, DecodedFolder={5}, OrphanFolder={6}, SipEndpointsFile={7}, FilePattern={8}, IncompleteRetentionHours={9}, MaxPendingQueueSize={10}, WriteDecodedCdrs={11}, DeleteInputFiles={12}, VoicemailNumber={13}, ExtensionRanges={14}, RoutingNumbers={15}, HuntGroupNumbers={16}",
                settings.InstanceId ?? string.Empty,
                settings.InputFolder ?? string.Empty,
                settings.OutputFolder ?? string.Empty,
                settings.ArchiveFolder ?? string.Empty,
                settings.WorkFolder ?? string.Empty,
                settings.DecodedFolder ?? string.Empty,
                settings.OrphanFolder ?? string.Empty,
                settings.SipEndpointsFile ?? string.Empty,
                settings.FilePattern ?? string.Empty,
                settings.IncompleteRetentionHours,
                settings.MaxPendingQueueSize,
                settings.WriteDecodedCdrs,
                settings.DeleteInputFiles,
                settings.VoicemailNumber ?? string.Empty,
                JoinValues(settings.ExtensionRanges),
                JoinValues(settings.RoutingNumbers),
                JoinValues(settings.HuntGroupNumbers));
        }

        private static string BuildSipEndpointsProviderSnapshot(ISipEndpointsProvider sipProvider)
        {
            if (sipProvider == null) return "null";

            var map = sipProvider.LoadAddressToTrunkMap();
            var mapCount = map?.Count ?? 0;
            var allEntries = mapCount == 0
                ? string.Empty
                : string.Join(" | ", map.OrderBy(kvp => kvp.Key, StringComparer.OrdinalIgnoreCase)
                    .Select(kvp => $"{kvp.Key}->{kvp.Value}"));

            return string.Format(
                "IsLoaded={0}, PstnCount={1}, AddressToTrunkCount={2}, AddressToTrunkMap={3}",
                sipProvider.IsLoaded,
                sipProvider.PstnCount,
                mapCount,
                allEntries);
        }

        private string BuildEngineStateSnapshot()
        {
            return string.Format(
                "_configNull={0}, ExtensionRangeCount={1}, SipMapperIsEmpty={2}, CandidatesCount={3}, RoutingNumbersCount={4}, HuntGroupNumbersCount={5}, DetectedRoutingNumbersCount={6}, GidHexToThreadIdCount={7}, GidHexToFullGidCount={8}, SeenAsCallersCount={9}, SeenAsCalleesCount={10}, DiscoveredExtensionsCount={11}, EarlyOutputCallsCount={12}, OutputtedThreadIdsCount={13}, LegsStreamWriterInitialized={14}, CacheCount={15}",
                _config == null,
                _extensionRange?.Count ?? 0,
                _sipResolver == null || _sipResolver.IsEmpty,
                _candidates?.Count ?? 0,
                _routingNumbers?.Count ?? 0,
                _huntGroupNumbers?.Count ?? 0,
                _detectedRoutingNumbers?.Count ?? 0,
                _gidHexToThreadId?.Count ?? 0,
                _gidHexToFullGid?.Count ?? 0,
                _seenAsCallers?.Count ?? 0,
                _seenAsCallees?.Count ?? 0,
                _discoveredExtensions?.Count ?? 0,
                _earlyOutputCalls?.Count ?? 0,
                _outputtedThreadIds?.Count ?? 0,
                _legsStreamWriter != null,
                _cache?.Count ?? 0);
        }

        private static string JoinValues(IEnumerable<string> values)
        {
            if (values == null) return string.Empty;
            return string.Join("|", values.Where(v => !string.IsNullOrWhiteSpace(v)));
        }

        /// <summary>
        /// Extract the hex suffix from a GID string (the unique call identifier).
        /// GID format: "timestamp:HEX_ID"  the timestamp can drift slightly between
        /// HG and CDR records, but the hex part is stable.
        /// </summary>
        private static string GetGidHex(string gid)
        {
            if (string.IsNullOrEmpty(gid)) return null;
            var lastColon = gid.LastIndexOf(':');
            return lastColon >= 0 && lastColon < gid.Length - 1
                ? gid.Substring(lastColon + 1)
                : gid;
        }

        /// <summary>
        /// Process all CDR files in the configured input folder.
        /// Files are processed in chronological order (by filename, then modified date).
        /// </summary>
        public ProcessingResult ProcessFolder(string folderPath = null, IProgress<int> progress = null)
        {
            return ProcessFolder(folderPath, progress, NullAbortHelper.Instance);
        }
        
        /// <summary>
        /// Process all CDR files with abort signal support.
        /// Checks abort signal between files for graceful shutdown.
        /// </summary>
        public ProcessingResult ProcessFolder(string folderPath, IProgress<int> progress, IAbortHelper abortHelper)
        {
            var result = new ProcessingResult();
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var folder = folderPath ?? _config?.InputFolder ?? InputFolder;

            if (!Directory.Exists(folder))
            {
                _logger.Error($"Input folder not found: {folder}");
                result.Errors.Add($"Input folder not found: {folder}");
                return result;
            }

            _logger.Info($"Starting CDR processing. Input: {folder}, Ranges: {_extensionRange.Count}, Pattern: {FilePattern}");

            var files = Directory.GetFiles(folder, FilePattern)
                .OrderBy(f => f) // Sort by filename first
                .ThenBy(f => File.GetLastWriteTimeUtc(f)) // Then by modified date
                .ToArray();

            _logger.Info($"Found {files.Length} CDR files to process");

            // Create writer factory for decoded CDR output if enabled
            CsvOutputWriter csvOutputWriter = WriteDecodedCdrs ? new CsvOutputWriter() : null;

            var outputFolder = OutputFolder ?? _config?.OutputFolder;
            if (string.IsNullOrWhiteSpace(outputFolder))
                throw new InvalidOperationException("OutputFolder is required for streaming legs output.");

            var archiveFolder = ArchiveFolder;
            if (DeleteInputFiles && string.IsNullOrWhiteSpace(archiveFolder))
            {
                var archiveError = "ArchiveFolder is required when DeleteInputFiles is true.";
                _logger.Error(archiveError);
                result.Errors.Add(archiveError);
                return result;
            }

            _legsStreamWriter = new CsvOutputWriter().CreateLegsStreamWriter(outputFolder);

            for (int i = 0; i < files.Length; i++)
            {
                // Check abort signal before processing each file
                if (abortHelper.IsAbortRequested)
                {
                    _logger.Warn($"Processing aborted after {i} files");
                    result.WasAborted = true;
                    break;
                }
                
                var fileName = Path.GetFileName(files[i]);
                try
                {
                    int recordCount = 0;

                    if (WriteDecodedCdrs)
                        _decodedCdrWriter = csvOutputWriter.CreateDecodedCdrWriter(files[i], _settings?.DecodedFolder);

                    foreach (var raw in _parser.StreamParseFile(files[i]))
                    {
                        ProcessSingleRecord(raw, result);
                        recordCount++;
                    }

                    if (_decodedCdrWriter != null)
                    {
                        _decodedCdrWriter.Dispose();
                        _decodedCdrWriter = null;
                        _logger.Debug(string.Format("Streamed {0} records from {1} (with decoded output)", recordCount, fileName));
                    }
                    else
                    {
                        _logger.Debug(string.Format("Parsed {0} records from {1}", recordCount, fileName));
                    }

                    result.TotalFilesProcessed++;
                    // Move processed source file into archive when input-file deletion is enabled.
                    if (DeleteInputFiles)
                    {
                        try
                        {
                            if (!Directory.Exists(archiveFolder))
                                Directory.CreateDirectory(archiveFolder);
                            var destPath = Path.Combine(archiveFolder, fileName);
                            // Handle duplicate filenames with timestamp suffix
                            if (File.Exists(destPath))
                            {
                                var nameWithoutExt = Path.GetFileNameWithoutExtension(fileName);
                                var ext = Path.GetExtension(fileName);
                                destPath = Path.Combine(archiveFolder, 
                                    $"{nameWithoutExt}_{DateTime.UtcNow:yyyyMMddHHmmss}{ext}");
                            }
                            File.Move(files[i], destPath);
                            _logger.Debug($"Archived {fileName} to {destPath}");
                        }
                        catch (Exception archiveEx)
                        {
                            _logger.Warn($"Failed to archive {fileName}. Exception: {archiveEx}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error($"Error processing file {fileName}", ex);
                    result.Errors.Add($"Error processing {fileName}: {ex.Message}");
                }

                progress?.Report((int)((i + 1.0) / files.Length * 100));
            }

            // Add early-output calls (streaming mode)
            if (_earlyOutputCalls.Count > 0)
            {
                foreach (var call in _earlyOutputCalls)
                {
                    EmitCall(call, result);
                }
                _logger.Info($"Added {_earlyOutputCalls.Count} early-output calls");
            }

            // Assemble remaining calls from cache (skips already-output calls)
            AssembleCalls(result);

                // Sort calls by timestamp for deterministic output (filesystem order varies)
                result.Calls = result.Calls
                    .OrderBy(c => c.Legs.FirstOrDefault()?.InLegConnectTime ?? "")
                    .ThenBy(c => c.GlobalCallId)
                    .ToList();

            // Add candidate extensions
            result.CandidateExtensions = _candidates.Values
                .OrderByDescending(c => c.Occurrences)
                .ToList();

            stopwatch.Stop();
            result.ProcessingTimeMs = stopwatch.ElapsedMilliseconds;

            _logger.Info($"Processing complete. Files: {result.TotalFilesProcessed}, Records: {result.TotalRecordsProcessed}, Calls: {result.TotalCallsIdentified}, Candidates: {result.CandidateExtensions.Count}, Time: {result.ProcessingTimeMs}ms, Speed: {result.RecordsPerSecond:F0} rec/sec");

            if (result.CandidateExtensions.Count > 0)
            {
                foreach (var c in result.CandidateExtensions)
                {
                    _logger.Warn($"Candidate extension: {c.Number} ({c.Occurrences}x) - {string.Join(", ", c.Reasons)}");
                }
            }

            if (_detectedRoutingNumbers.Count > 0)
            {
                _logger.Info($"Auto-detected routing numbers from HG records: {string.Join(", ", _detectedRoutingNumbers.OrderBy(n => n))}");
            }

            _legsStreamWriter?.Dispose();

            // Log unknown SIP endpoints encountered during processing
            if (_unknownSipEndpoints.Count > 0)
            {
                _logger.Warn(string.Format("Unknown SIP endpoints encountered during processing ({0}): {1}",
                    _unknownSipEndpoints.Count,
                    string.Join(", ", _unknownSipEndpoints.OrderBy(e => e))));
            }
            return result;
        }

        /// <summary>
        /// Process a list of raw CDR records (from a single file).
        /// </summary>
        public void ProcessRecords(List<RawCdrRecord> records, ProcessingResult result)
        {
            foreach (var raw in records)
            {
                ProcessSingleRecord(raw, result);
            }
        }

        /// <summary>
        /// Process a single CDR record. Used for streaming mode.
        /// </summary>
        private void ProcessSingleRecord(RawCdrRecord raw, ProcessingResult result)
        {
            // Write decoded CDR if enabled
            _decodedCdrWriter?.WriteRecord(raw);

            result.TotalRecordsProcessed++;

            // Early feature code filtering: skip *44/#44 call forward activation/deactivation
            // before any leg creation occurs
            if (raw.RecordType == CdrRecordType.FullCdr && !string.IsNullOrEmpty(raw.DialedNumber))
            {
                var dialed = raw.DialedNumber;
                if (dialed.Contains("*44") || dialed.Contains("#44"))
                {
                    var threadId = raw.ThreadIdSequence ?? raw.ThreadIdNode ?? raw.GlobalCallId;
                    _logger.Debug($"Early filtering feature code call: {dialed}, GidSequence={raw.GidSequence}");
                    _tracer.TraceSuppressedLeg(
                        threadId,
                        string.Format("Record Dialed={0} GidSeq={1}", dialed, raw.GidSequence ?? ""),
                        string.Format("Feature code filtering (early): DialedNumber contains {0}", dialed.Contains("*44") ? "*44" : "#44"));
                    return;
                }
            }

            switch (raw.RecordType)
            {
                case CdrRecordType.FullCdr:
                    ProcessFullCdr(raw, result);
                    break;
                case CdrRecordType.HuntGroup:
                    ProcessHuntGroup(raw, result);
                    break;
                case CdrRecordType.CallForward:
                    ProcessCallForward(raw, result);
                    break;
            }
        }

        private void ProcessFullCdr(RawCdrRecord raw, ProcessingResult result)
        {
            // Thread ID groups legs into a single logical call.
            // ThreadIdSequence is the unique per-call sequence number (spec field 125).
            // ThreadIdNode is the node name (spec field 124), not unique per call.
            var threadId = raw.ThreadIdSequence ?? raw.ThreadIdNode ?? raw.GlobalCallId;

            var leg = new ProcessedLeg
            {
                GlobalCallId = raw.GlobalCallId,
                ThreadId = threadId,
                GidSequence = raw.GidSequence,
                DialedNumber = raw.DialedNumber,
                DestinationExt = raw.DestinationExt,
                Duration = raw.Duration,
                CallAnswerTime = raw.CallAnswerTime,
                InLegConnectTime = raw.InLegConnectTime,
                OutLegReleaseTime = raw.OutLegReleaseTime,
                OutLegConnectTime = raw.OutLegConnectTime,
                CallReleaseTime = raw.CallReleaseTime,
                SourceFile = raw.SourceFile,
                SourceLine = raw.SourceLine,
                IngressEndpoint = raw.IngressEndpoint,
                EgressEndpoint = raw.EgressEndpoint,
            };

            // Trace field origins for all significant fields
            _tracer.TraceFieldOrigin(threadId, 0, "DialedNumber", raw.DialedNumber, raw.SourceFile, raw.SourceLine, "spec101");
            _tracer.TraceFieldOrigin(threadId, 0, "DestinationExt", raw.DestinationExt, raw.SourceFile, raw.SourceLine, "spec128");
            _tracer.TraceFieldOrigin(threadId, 0, "IngressEndpoint", raw.IngressEndpoint, raw.SourceFile, raw.SourceLine, "spec126");
            _tracer.TraceFieldOrigin(threadId, 0, "EgressEndpoint", raw.EgressEndpoint, raw.SourceFile, raw.SourceLine, "spec127");
            _tracer.TraceFieldOrigin(threadId, 0, "CallingNumber", raw.CallingNumber, raw.SourceFile, raw.SourceLine, "spec12");
            _tracer.TraceFieldOrigin(threadId, 0, "CalledParty", raw.CalledParty, raw.SourceFile, raw.SourceLine, "spec11");
            _tracer.TraceFieldOrigin(threadId, 0, "ForwardingParty", raw.ForwardingParty, raw.SourceFile, raw.SourceLine, "spec65");

            // Debug: verify endpoint copying
            if (!string.IsNullOrEmpty(raw.IngressEndpoint) || !string.IsNullOrEmpty(raw.EgressEndpoint))
            {
                _logger.Debug($"Created ProcessedLeg with Ingress={leg.IngressEndpoint}, Egress={leg.EgressEndpoint}, GidSequence={leg.GidSequence}");
            }

            // Store raw fields for transfer chain analysis
            leg.CalledParty = raw.CalledParty;
            leg.CallingNumber = raw.CallingNumber;

            // Determine if answered
            leg.IsAnswered = (raw.Duration > 0 && raw.CauseCode == 16)
                || raw.PerCallFeature == 8
                || (raw.MediaType == 1 && raw.CauseCode == 16);

            // Calculate RingTime (seconds between InLegConnectTime and CallAnswerTime)
            leg.RingTime = CalculateRingTime(raw.InLegConnectTime, raw.CallAnswerTime);

            // Store raw forwarding party for transfer chain
            leg.ForwardingParty = raw.ForwardingParty;

            // Determine if forwarded
            leg.IsForwarded = !string.IsNullOrEmpty(raw.ForwardingParty);

            // Determine if pickup
            leg.IsPickup = (raw.CallEventIndicator & 8192) == 8192;

            // Determine if voicemail (CF-to-Voicemail flag bit 64, or destination matches VM number)
            var vmNum = GetVoicemailNumber();
            leg.IsVoicemail = (raw.PerCallFeatureExt & 64) == 64
                || (!string.IsNullOrEmpty(vmNum) && raw.CalledParty == vmNum);

            // Hunt group detection: only from HG records (ProcessHuntGroup)
            // Don't set HG from CalledParty heuristic  it catches VM codes,
            // feature codes (*44, #44), and regular extensions as "HG".

                        // Call direction rules:
            // 1) SIP endpoints (if known)
            // 2) PartyId fallback (OrigPartyId/TermPartyId)
            // 3) Thread context if both endpoints unknown
            bool callerIsInternal;
            bool destIsInternal;

            leg.CallDirection = _directionResolver.ResolveDirection(raw, threadId, out callerIsInternal, out destIsInternal);
            _directionResolver.AssignCallerCalledFields(leg, raw, leg.CallDirection, callerIsInternal, destIsInternal);

            _tracer.TraceDirectionDecision(
                threadId, leg.LegIndex, leg.CallDirection.ToString(),
                string.Format("SIP/PartyId resolve: Ingress={0} Egress={1}", raw.IngressEndpoint ?? "null", raw.EgressEndpoint ?? "null"),
                callerIsInternal, destIsInternal,
                raw.IngressEndpoint, raw.EgressEndpoint,
                raw.OrigPartyId, raw.TermPartyId);

            if (!string.IsNullOrEmpty(leg.CallerExtension))
                _tracer.TraceFieldOrigin(threadId, 0, "CallerExtension", leg.CallerExtension, raw.SourceFile, raw.SourceLine, "CallingNumber(internal)");
            if (!string.IsNullOrEmpty(leg.CallerExternal))
                _tracer.TraceFieldOrigin(threadId, 0, "CallerExternal", leg.CallerExternal, raw.SourceFile, raw.SourceLine, "CallingNumber(external)");
            if (!string.IsNullOrEmpty(leg.CalledExtension))
                _tracer.TraceFieldOrigin(threadId, 0, "CalledExtension", leg.CalledExtension, raw.SourceFile, raw.SourceLine, "DestinationExt(internal)");
            if (!string.IsNullOrEmpty(leg.CalledExternal))
                _tracer.TraceFieldOrigin(threadId, 0, "CalledExternal", leg.CalledExternal, raw.SourceFile, raw.SourceLine, "DestinationExt(external)");

            // Forward from/to
            if (leg.IsForwarded)
            {
                leg.ForwardFromExt = raw.ForwardingParty;
                leg.ForwardToExt = raw.DestinationExt;
            }

            // Pickup
            if (leg.IsPickup)
            {
                leg.ForwardFromExt = raw.CalledParty;
                leg.ForwardToExt = raw.DestinationExt;
            }

            // Companion text fields
            leg.CauseCode = raw.CauseCode;
            leg.CauseCodeText = FieldMappings.GetReleaseCauseText(raw.CauseCode);
            leg.PerCallFeature = raw.PerCallFeature;
            leg.PerCallFeatureText = FieldMappings.GetPerCallFeatureText(raw.PerCallFeature);
            leg.AttemptIndicator = raw.AttemptIndicator;
            leg.AttemptIndicatorText = FieldMappings.GetAttemptIndicatorText(raw.AttemptIndicator);
            leg.PerCallFeatureExt = raw.PerCallFeatureExt;
            leg.PerCallFeatureExtText = FieldMappings.GetPerCallFeatureExtText(raw.PerCallFeatureExt);
            
            // Auto-detect voicemail number: if bit 64 is set (CF-to-Voicemail) and CalledParty is present
            if ((raw.PerCallFeatureExt & 64) != 0 && !string.IsNullOrEmpty(raw.CalledParty)
                && string.IsNullOrEmpty(_detectedVoicemailNumber))
            {
                _detectedVoicemailNumber = raw.CalledParty;
                _logger.Debug($"Auto-detected voicemail number: {_detectedVoicemailNumber}");
                _tracer.TraceSpecialNumber(_detectedVoicemailNumber, "Voicemail", "PerCallFeatureExt bit 64 (CF-to-Voicemail)");
            }
            
            leg.CallEventIndicator = raw.CallEventIndicator;
            leg.CallEventIndicatorText = FieldMappings.GetCallEventIndicatorText(raw.CallEventIndicator);
            leg.OrigPartyId = raw.OrigPartyId;
            leg.OrigPartyIdText = FieldMappings.GetPartyIdText(raw.OrigPartyId);
            leg.TermPartyId = raw.TermPartyId;
            leg.TermPartyIdText = FieldMappings.GetPartyIdText(raw.TermPartyId);

            // Before storing, check if there are HG-only legs for this thread -- merge their info.
            // HG records are stored by GID, CDR legs by ThreadId. Check both keys.
            // GIDs may differ slightly in timestamp between HG and CDR records,
            // so also check by hex suffix.
            var existingLegs = _cache.GetPendingLegs(threadId);
            var gid = raw.GlobalCallId;
            var gidHex = GetGidHex(gid);
            List<ProcessedLeg> gidLegs = new List<ProcessedLeg>();
            string hgOnlyGidKey = null; // track the cache key where HG-only legs are stored

            if (!string.IsNullOrEmpty(gid) && gid != threadId)
            {
                gidLegs = _cache.GetPendingLegs(gid);
                if (gidLegs.Count > 0) hgOnlyGidKey = gid;
            }
            // If exact GID didn't find HG legs, try by hex suffix
            string fullGid;
            if (gidLegs.Count == 0 && !string.IsNullOrEmpty(gidHex)
                && _gidHexToFullGid.TryGetValue(gidHex, out fullGid)
                && fullGid != threadId)
            {
                gidLegs = _cache.GetPendingLegs(fullGid);
                if (gidLegs.Count > 0) hgOnlyGidKey = fullGid;
            }

            foreach (var existing in existingLegs.Concat(gidLegs))
            {
                if (existing.IsHgOnly)
                {
                    if (string.IsNullOrEmpty(leg.HuntGroupNumber) && !string.IsNullOrEmpty(existing.HuntGroupNumber))
                    {
                        leg.HuntGroupNumber = existing.HuntGroupNumber;
                    }
                }
            }

            if (!string.IsNullOrEmpty(leg.HuntGroupNumber))
                _tracer.TraceFieldOrigin(threadId, 0, "HuntGroupNumber", leg.HuntGroupNumber, raw.SourceFile, raw.SourceLine, "HG record merge");

            // Remove HG-only legs now that we have a real CDR for this thread
            foreach (var hgLeg in existingLegs.Where(l => l.IsHgOnly).ToList())
            {
                _cache.RemovePendingLeg(threadId, hgLeg.InLegConnectTime);
            }
            if (hgOnlyGidKey != null)
            {
                foreach (var hgLeg in gidLegs.Where(l => l.IsHgOnly).ToList())
                {
                    _cache.RemovePendingLeg(hgOnlyGidKey, hgLeg.InLegConnectTime);
                }
            }

            // Register GID hex  ThreadId mapping for HG records that arrive later
            if (!string.IsNullOrEmpty(gidHex))
            {
                _gidHexToThreadId[gidHex] = threadId;
            }

            // Store in cache keyed by Thread ID (groups all legs of the same call)
            _cache.StorePendingLeg(threadId, leg);

            // Streaming: check for early output and enforce cache limit
            CheckStreamingOutput(threadId, result);

            // Candidate extension detection
            DetectCandidateExtension(raw.CallingNumber, "CallingNumber", raw);
            DetectCandidateExtension(raw.DestinationExt, "DestinationExt", raw);
            DetectCandidateExtension(raw.DialedNumber, "DialedNumber", raw);
        }

        private void ProcessHuntGroup(RawCdrRecord raw, ProcessingResult result)
        {
            // Auto-detect HG pilot numbers as routing numbers
            // Auto-detect HG pilot numbers as routing numbers
            if (!string.IsNullOrEmpty(raw.HuntGroupNumber))
            {
                _detectedRoutingNumbers.Add(raw.HuntGroupNumber);
                _tracer.TraceSpecialNumber(raw.HuntGroupNumber, "HuntGroup", "HG record pilot number (auto-detected routing)");
            }

            // HG records supplement the full CDR -- store in cache linked by GID
            var leg = new ProcessedLeg
            {
                GlobalCallId = raw.GlobalCallId,
                HuntGroupNumber = raw.HuntGroupNumber,
                CalledExtension = raw.RoutedToExtension,
                DestinationExt = raw.RoutedToExtension,
                CallDirection = CallDirection.Internal,
                InLegConnectTime = raw.HGStartTime ?? raw.Timestamp,
                SourceFile = raw.SourceFile,
                SourceLine = raw.SourceLine,
                IngressEndpoint = raw.IngressEndpoint,
                EgressEndpoint = raw.EgressEndpoint,
            };

            // Try to merge HG info into existing legs.
            // CDR legs are stored by ThreadId, but HG records only have GID.
            // Use the GID hex suffix  ThreadId mapping (timestamps can drift 0.1s).
            var gidHex = GetGidHex(raw.GlobalCallId);
            var existingLegs = _cache.GetPendingLegs(raw.GlobalCallId);
            string threadId;
            if (existingLegs.Count == 0 && !string.IsNullOrEmpty(gidHex)
                && _gidHexToThreadId.TryGetValue(gidHex, out threadId))
            {
                existingLegs = _cache.GetPendingLegs(threadId);
            }

            if (existingLegs.Count > 0)
            {
                foreach (var existing in existingLegs)
                {
                    if (string.IsNullOrEmpty(existing.HuntGroupNumber))
                    {
                        existing.HuntGroupNumber = raw.HuntGroupNumber;
                    }
                }
            }
            else
            {
                // HG record arrived before the full CDR -- store by GID, marked as HG-only
                leg.IsHgOnly = true;
                _cache.StorePendingLeg(raw.GlobalCallId, leg);
                // Register hex mapping so CDRs with slightly different timestamps can find it.
                // Keep the FIRST mapping (parent HG)  don't overwrite with secondary HGs.
                if (!string.IsNullOrEmpty(gidHex) && !_gidHexToFullGid.ContainsKey(gidHex))
                {
                    _gidHexToFullGid[gidHex] = raw.GlobalCallId;
                }
            }
        }

        private void ProcessCallForward(RawCdrRecord raw, ProcessingResult result)
        {
            // CF records track forward activation -- create a minimal leg
            var leg = new ProcessedLeg
            {
                GlobalCallId = null, // CF records may not have GID
                CallerExtension = raw.OrigExtension,
                ForwardFromExt = raw.OrigExtension,
                ForwardToExt = raw.ForwardDestination,
                IsForwarded = true,
                InLegConnectTime = raw.Timestamp,
                SourceFile = raw.SourceFile,
                SourceLine = raw.SourceLine,
                IngressEndpoint = raw.IngressEndpoint,
                EgressEndpoint = raw.EgressEndpoint,
            };

            // Determine if forward destination is internal or external
            if (IsInternalNumber(raw.ForwardDestination))
            {
                leg.CalledExtension = raw.ForwardDestination;
                leg.CallDirection = CallDirection.Internal;
            }
            else
            {
                leg.CalledExternal = raw.ForwardDestination;
                leg.CallDirection = CallDirection.TrunkToTrunk;
            }

            // CF records don't always have GID -- store only if linkable
            if (!string.IsNullOrEmpty(raw.GlobalCallId))
            {
                _cache.StorePendingLeg(raw.GlobalCallId, leg);
            }
        }
        

        /// <summary>
        /// Identify discovered extensions (numbers seen as both caller and callee)
        /// and save them to the configured file.
        /// </summary>
        /// A leg is a VM leg when:
        /// 1. PerCallFeatureExt bit 64 is set ("CF to Voicemail" flag), OR
        /// 2. CalledParty matches the configured or auto-detected voicemail number
        /// </summary>
        private bool IsVmLeg(ProcessedLeg leg)
        {
            // Bit 64 = "CF to Voicemail" flag in PerCallFeatureExt
            if ((leg.PerCallFeatureExt & 64) != 0)
                return true;
            
            // Check configured voicemail number first
            if (!string.IsNullOrEmpty(VoicemailNumber) && leg.CalledParty == VoicemailNumber)
                return true;
            
            // Fallback: check auto-detected voicemail number
            return !string.IsNullOrEmpty(_detectedVoicemailNumber) && leg.CalledParty == _detectedVoicemailNumber;
        }

        /// <summary>
        /// Get the effective voicemail number (configured or auto-detected).
        /// </summary>
        private string GetVoicemailNumber()
        {
            return !string.IsNullOrEmpty(VoicemailNumber)
                ? VoicemailNumber
                : _detectedVoicemailNumber;
        }

        private string NormalizeEndpoint(string endpoint)
        {
            if (string.IsNullOrWhiteSpace(endpoint)) return null;
            var e = endpoint.Trim();
            if (e.Contains(","))
            {
                var parts = e.Split(',');
                e = parts[parts.Length - 1].Trim();
            }
            var lastColon = e.LastIndexOf(':');
            if (lastColon > -1 && lastColon < e.Length - 1)
            {
                var port = e.Substring(lastColon + 1);
                var isPort = port.All(char.IsDigit);
                if (isPort && e.Count(c => c == ':') == 1)
                {
                    e = e.Substring(0, lastColon);
                }
            }
            return e;
        }

        private bool IsSipPstn(string endpoint)
        {
            var e = NormalizeEndpoint(endpoint);
            if (string.IsNullOrEmpty(e)) return false;
            return _sipResolver.IsPstn(e);
        }

        private bool IsSipKnown(string endpoint)
        {
            var e = NormalizeEndpoint(endpoint);
            if (string.IsNullOrEmpty(e)) return false;
            var known = _sipResolver.IsKnown(e);
            if (!known && _sipResolver.IsLoaded)
                _unknownSipEndpoints.Add(e);
            return known;
        }

        /// <summary>
        /// True when the number is a configured routing-only extension (CMS, pilot, etc.).
        /// </summary>
        
        private bool IsInternalDestForEmptyRanges(ProcessedLeg leg)
        {
            if (leg == null) return false;
            // Prefer SIP endpoints when available
            if (IsSipKnown(leg.EgressEndpoint))
            {
                return !IsSipPstn(leg.EgressEndpoint);
            }
            // PartyId fallback
            if (leg.TermPartyId == 902) return true;
            if (leg.TermPartyId == 901) return false;
            return false;
        }
        private bool IsRoutingNumber(string number)
        {
            if (string.IsNullOrEmpty(number)) return false;
            // Check both configured routing numbers and auto-detected HG pilots
            return _routingNumbers.Contains(number) || _detectedRoutingNumbers.Contains(number);
        }

        /// <summary>
        /// Calculate ring time in seconds from timestamp strings.
        /// </summary>
        private int? CalculateRingTime(string inLegConnectTime, string callAnswerTime)
        {
            if (string.IsNullOrEmpty(inLegConnectTime) || string.IsNullOrEmpty(callAnswerTime))
                return null;

            DateTimeOffset connectTime;
            DateTimeOffset answerTime;
            if (DateTimeOffset.TryParse(inLegConnectTime, out connectTime) &&
                DateTimeOffset.TryParse(callAnswerTime, out answerTime))
            {
                var ringSeconds = (int)(answerTime - connectTime).TotalSeconds;
                return ringSeconds >= 0 ? (int?)ringSeconds : null;
            }
            return null;
        }


        /// <summary>
        /// True when the number is a configured Hunt Group number.
        /// Used for leg merging (HG forwarding is not "real" forwarding).
        /// Does not affect direction detection.
        /// </summary>
        private bool IsHuntGroupNumber(string number)
        {
            return !string.IsNullOrEmpty(number) && _huntGroupNumbers.Count > 0
                && _huntGroupNumbers.Contains(number);
        }

        /// <summary>
        /// True when a leg is a routing pass-through that should be suppressed:
        ///   - Destination is a routing number AND duration = 0 (unanswered routing leg), OR
        ///   - CallingNumber is a routing number AND duration = 0 AND not answered
        ///     AND destination is also routing or empty with no forwarding
        ///     (CMS outgoing "setup" leg that initiates a transfer, NOT the CMS
        ///     agent's actual outbound call to a real extension/VM)
        /// Answered routing legs (dur>0) are real calls and are kept.
        /// </summary>
        private bool IsRoutingOnlyLeg(ProcessedLeg leg)
        {
            if (leg.Duration == 0)
            {
                if (IsRoutingNumber(leg.DestinationExt))
                    return true;
                // CallingNumber is routing: only suppress if it's a pure setup leg
                // (e.g. CMSextension with no talk). Don't suppress CMS outbound calls
                // to real destinations (they have ForwardingParty or non-routing CalledParty
                // with actual routing info like DialedNumber).
                if (IsRoutingNumber(leg.CallingNumber) && !leg.IsAnswered
                    && string.IsNullOrEmpty(leg.ForwardingParty)
                    && (string.IsNullOrEmpty(leg.DestinationExt) || IsRoutingNumber(leg.DestinationExt)))
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Suppress routing-only legs from the output.
        /// Routing numbers (CMS, pilot, etc.) that act as pure pass-throughs
        /// should not appear as call records. Info is preserved in Transfer From/To.
        ///
        /// Must be called AFTER ComputeTransferChain (so CMS numbers are already
        /// in the transfer fields) and BEFORE computing call-level fields.
        /// </summary>
        private void SuppressCmsLegs(List<ProcessedLeg> orderedLegs)
        {
            if (_routingNumbers.Count == 0 && _detectedRoutingNumbers.Count == 0) return;

            // Identify CMS-routing leg indices
            var cmsIndices = new HashSet<int>();
            for (int i = 0; i < orderedLegs.Count; i++)
            {
                if (IsRoutingOnlyLeg(orderedLegs[i]))
                {
                    cmsIndices.Add(i);
                    var suppLeg = orderedLegs[i];
                    _tracer.TraceSuppressedLeg(
                        suppLeg.ThreadId,
                        string.Format("Leg#{0} DestExt={1} CallingNum={2} Duration={3} Line={4}",
                            i + 1, suppLeg.DestinationExt ?? "", suppLeg.CallingNumber ?? "",
                            suppLeg.Duration, suppLeg.SourceLine),
                        string.Format("Routing-only: {0}",
                            IsRoutingNumber(suppLeg.DestinationExt) ? "DestinationExt is routing number" : "CallingNumber is routing number + no forwarding"));
                }
            }

            // Answered CMS legs (dur>0) are real calls where user talked to CMS
            // -- keep them as-is with CMS as DestinationExt.

            if (cmsIndices.Count == 0) return;

            _logger.Debug($"Suppressing {cmsIndices.Count} CMS-routing legs");

            // Update adjacent legs' Transfer From/To to bridge over suppressed CMS legs
            foreach (var idx in cmsIndices)
            {
                var cmsLeg = orderedLegs[idx];
                // CMS routing number: prefer DestinationExt, fall back to CallingNumber
                var cmsNumber = !string.IsNullOrEmpty(cmsLeg.DestinationExt)
                    ? cmsLeg.DestinationExt
                    : cmsLeg.CallingNumber;
                // The destination the CMS leg was routing to
                var cmsTarget = !string.IsNullOrEmpty(cmsLeg.CalledParty) && !IsRoutingNumber(cmsLeg.CalledParty)
                    ? cmsLeg.CalledParty
                    : cmsLeg.DestinationExt;

                // Find previous non-CMS leg
                int prevIdx = -1;
                for (int i = idx - 1; i >= 0; i--)
                {
                    if (!cmsIndices.Contains(i)) { prevIdx = i; break; }
                }

                // Find next non-CMS leg
                int nextIdx = -1;
                for (int i = idx + 1; i < orderedLegs.Count; i++)
                {
                    if (!cmsIndices.Contains(i)) { nextIdx = i; break; }
                }

                // Previous leg's TransferTo: point to the destination the CMS was routing to
                if (prevIdx >= 0 && !string.IsNullOrEmpty(cmsTarget))
                {
                    orderedLegs[prevIdx].TransferTo = cmsTarget;
                }

                // Next leg's TransferFrom: set to CMS number if not already set
                // (shows "forwarded from CMS" in the routing chain)
                if (nextIdx >= 0)
                {
                    var nextLeg = orderedLegs[nextIdx];
                    if (string.IsNullOrEmpty(nextLeg.TransferFrom))
                    {
                        nextLeg.TransferFrom = cmsNumber;
                    }
                    // Propagate DialedNumber from suppressed CMS leg's target
                    if (string.IsNullOrEmpty(nextLeg.DialedNumber) && !string.IsNullOrEmpty(cmsTarget))
                    {
                        nextLeg.DialedNumber = cmsTarget;
                    }
                    // FIX: For Internal calls, also populate CalledExtension (Dest Ext) from CMS target
                    // Every Internal call must have Dest Ext populated
                    if (string.IsNullOrEmpty(nextLeg.CalledExtension) && !string.IsNullOrEmpty(cmsTarget)
                        && IsInternalNumber(cmsTarget))
                    {
                        nextLeg.CalledExtension = cmsTarget;
                    }
                }
            }

            // Collect HG numbers from legs about to be suppressed
            var suppressedHGs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var idx in cmsIndices)
            {
                var hg = orderedLegs[idx].HuntGroupNumber;
                if (!string.IsNullOrEmpty(hg))
                    suppressedHGs.Add(hg);
            }

            // Capture original caller before suppression (first non-CMS leg's caller,
            // or first leg's caller if all are CMS)
            string originalCallerExt = null;
            for (int i = 0; i < orderedLegs.Count; i++)
            {
                if (!cmsIndices.Contains(i) && !string.IsNullOrEmpty(orderedLegs[i].CallerExtension)
                    && !IsRoutingNumber(orderedLegs[i].CallerExtension))
                {
                    originalCallerExt = orderedLegs[i].CallerExtension;
                    break;
                }
            }
            // Fallback: if no non-CMS leg found, use first leg's caller if it's not a routing number
            if (string.IsNullOrEmpty(originalCallerExt) && orderedLegs.Count > 0
                && !string.IsNullOrEmpty(orderedLegs[0].CallerExtension)
                && !IsRoutingNumber(orderedLegs[0].CallerExtension))
            {
                originalCallerExt = orderedLegs[0].CallerExtension;
            }

            // Direction aggregation: capture most external direction from legs about to be suppressed
            var dirPriority = new Dictionary<CallDirection, int>
            {
                { CallDirection.TrunkToTrunk, 4 },
                { CallDirection.Outgoing, 3 },
                { CallDirection.Incoming, 2 },
                { CallDirection.Internal, 1 },
                { CallDirection.Unknown, 0 }
            };
            CallDirection mostExternalDir = CallDirection.Internal;
            int maxPri = 1;
            foreach (var idx in cmsIndices)
            {
                var dir = orderedLegs[idx].CallDirection;
                int p;
                var pri = dirPriority.TryGetValue(dir, out p) ? p : 0;
                if (pri > maxPri) { maxPri = pri; mostExternalDir = dir; }
            }

            // Remove CMS legs (iterate in reverse to preserve indices)
            for (int i = orderedLegs.Count - 1; i >= 0; i--)
            {
                if (cmsIndices.Contains(i))
                    orderedLegs.RemoveAt(i);
            }

            // Propagate external direction from suppressed legs to remaining legs
            if (maxPri > 1)
            {
                foreach (var leg in orderedLegs)
                {
                    int lp;
                    var legPri = dirPriority.TryGetValue(leg.CallDirection, out lp) ? lp : 0;
                    if (legPri < maxPri)
                        leg.CallDirection = mostExternalDir;
                }
            }

            // Propagate HG numbers from suppressed CMS legs to remaining legs.
            // Do this BEFORE overwriting CallingNumber with original caller.
            if (suppressedHGs.Count > 0)
            {
                var hgNumber = suppressedHGs.First();
                foreach (var leg in orderedLegs)
                {
                    if (string.IsNullOrEmpty(leg.HuntGroupNumber)
                        && IsRoutingNumber(leg.CallingNumber))
                        leg.HuntGroupNumber = hgNumber;
                }
            }

            // Propagate original caller to remaining legs that have routing numbers as caller
            if (!string.IsNullOrEmpty(originalCallerExt))
            {
                foreach (var leg in orderedLegs)
                {
                    if (IsRoutingNumber(leg.CallerExtension) || IsRoutingNumber(leg.CallingNumber))
                    {
                        leg.CallerExtension = originalCallerExt;
                        leg.CallingNumber = originalCallerExt;
                    }
                }
            }

            // Re-number remaining legs
            for (int i = 0; i < orderedLegs.Count; i++)
            {
                orderedLegs[i].LegIndex = i + 1;
            }

        }

        /// <summary>
        /// Apply post-processing to legs after call assembly: direction propagation,
        /// DialedAni computation, Extension/DestExt field handling.
        /// Shared between AssembleCalls and AssembleSingleCall to avoid duplication.
        /// </summary>
        private void ApplyLegPostProcessing(ProcessedCall call, List<ProcessedLeg> orderedLegs)
        {
            // Direction propagation: all legs inherit call-level direction for external calls
            if (call.CallDirection == CallDirection.Incoming
                || call.CallDirection == CallDirection.Outgoing
                || call.CallDirection == CallDirection.TrunkToTrunk)
            {
                foreach (var leg in orderedLegs)
                {
                    leg.CallDirection = call.CallDirection;
                }
            }

            // Compute DialedAni: the external number for the call
            HashSet<string> internalNumsForCaller = null;
            if (_extensionRange.IsEmpty)
            {
                internalNumsForCaller = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var l in orderedLegs)
                {
                    if (l.OrigPartyId == 900 && !string.IsNullOrEmpty(l.CallingNumber))
                    {
                        internalNumsForCaller.Add(l.CallingNumber);
                    }

                    if (l.TermPartyId == 902)
                    {
                        if (!string.IsNullOrEmpty(l.CalledParty)) internalNumsForCaller.Add(l.CalledParty);
                        if (!string.IsNullOrEmpty(l.DestinationExt)) internalNumsForCaller.Add(l.DestinationExt);
                        if (!string.IsNullOrEmpty(l.CalledExtension)) internalNumsForCaller.Add(l.CalledExtension);
                        if (!string.IsNullOrEmpty(l.ForwardingParty)) internalNumsForCaller.Add(l.ForwardingParty);
                    }
                }
            }

            var excludedCallerNums = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (internalNumsForCaller != null)
            {
                foreach (var n in internalNumsForCaller) excludedCallerNums.Add(n);
            }
            foreach (var l in orderedLegs)
            {
                if (!string.IsNullOrEmpty(l.DestinationExt)) excludedCallerNums.Add(l.DestinationExt);
                if (!string.IsNullOrEmpty(l.CalledParty)) excludedCallerNums.Add(l.CalledParty);
                if (!string.IsNullOrEmpty(l.ForwardingParty)) excludedCallerNums.Add(l.ForwardingParty);
            }

            var externalCaller = orderedLegs
                .Where(l => l.OrigPartyId == 901 && !string.IsNullOrEmpty(l.CallerExternal)
                    && !excludedCallerNums.Contains(l.CallerExternal)
                    && !IsRoutingNumber(l.CallerExternal))
                .Select(l => l.CallerExternal)
                .FirstOrDefault()
                ?? orderedLegs.Select(l => l.CallerExternal)
                    .FirstOrDefault(e => !string.IsNullOrEmpty(e)
                        && !excludedCallerNums.Contains(e)
                        && !IsRoutingNumber(e))
                    ?? "";
            var externalDest = orderedLegs
                .Select(l => l.CalledExternal)
                .FirstOrDefault(e => !string.IsNullOrEmpty(e)) ?? "";

            foreach (var leg in orderedLegs)
            {
                switch (call.CallDirection)
                {
                    case CallDirection.Incoming:
                        leg.DialedAni = externalCaller;
                        break;
                    case CallDirection.Outgoing:
                    case CallDirection.TrunkToTrunk:
                        leg.DialedAni = !string.IsNullOrEmpty(externalDest) ? externalDest : leg.DialedNumber;
                        break;
                    default:
                        leg.DialedAni = leg.DialedNumber;
                        break;
                }
            }

            // Extension/DestExt field handling
            foreach (var leg in orderedLegs)
            {
                if (call.CallDirection == CallDirection.Outgoing)
                {
                    leg.Extension = leg.CallerExtension;
                    leg.DestinationExt = "";
                }
                else
                if (call.CallDirection == CallDirection.Incoming
                    || call.CallDirection == CallDirection.Outgoing
                    || call.CallDirection == CallDirection.TrunkToTrunk
                    || call.CallDirection == CallDirection.T2TIn
                    || call.CallDirection == CallDirection.T2TOut)
                {
                    // Non-Internal: Extension = DestinationExt, then clear DestExt
                    leg.Extension = !string.IsNullOrEmpty(leg.DestinationExt)
                        ? leg.DestinationExt
                        : (leg.CalledParty ?? "");
                    leg.DestinationExt = "";
                }
                else
                {
                    // Internal: Extension = calling extension
                    leg.Extension = call.CallerExtension ?? "";
                    // Internal: DestExt should show destination - fallback to CalledParty if empty
                    if (string.IsNullOrEmpty(leg.DestinationExt) && !string.IsNullOrEmpty(leg.CalledParty))
                    {
                        leg.DestinationExt = leg.CalledParty;
                    }
                }

                if (leg.IsPickup && !string.IsNullOrEmpty(leg.TransferFrom) )
                {
                    _logger.Debug($"Clear TransferFrom for pickup call");
                    leg.TransferFrom = "";
                }
            }

           
        }

        private void EmitCall(ProcessedCall call, ProcessingResult result)

        {

            if (_legsStreamWriter == null) throw new InvalidOperationException("Legs stream writer not initialized.");

            _legsStreamWriter.WriteCall(call);

        }



        private void HandleEarlyOutputCall(ProcessedCall call, ProcessingResult result)

        {

            if (_legsStreamWriter == null) throw new InvalidOperationException("Legs stream writer not initialized.");

            _legsStreamWriter.WriteCall(call);

        }


        /// <summary>
        /// Assemble all cached legs into complete calls grouped by Thread ID.
        /// Thread ID (CDR field 124) is the authoritative call grouping identifier.
        /// Falls back to GID when Thread ID is not available.
        /// </summary>
        private void AssembleCalls(ProcessingResult result)
        {
            // Build groups from cache (keyed by Thread ID or GID), filtering out HG-only legs
            var groups = new List<List<ProcessedLeg>>();
            foreach (var key in _cache.GetAllGids())
            {
                // Skip calls already output via streaming
                if (_outputtedThreadIds.Contains(key)) continue;

                var allLegs = _cache.GetPendingLegs(key);
                var hgOnlyLegs = allLegs.Where(l => l.IsHgOnly).ToList();
                var legs = allLegs.Where(l => !l.IsHgOnly).ToList();
                
                // Trace HG-only legs that have no matching CDR
                foreach (var hgLeg in hgOnlyLegs)
                {
                    _tracer.TraceSuppressedLeg(
                        key,
                        string.Format("HG-only Leg GID={0} HG={1} DestExt={2} Line={3}",
                            hgLeg.GlobalCallId ?? "", hgLeg.HuntGroupNumber ?? "",
                            hgLeg.DestinationExt ?? "", hgLeg.SourceLine),
                        "HG-only leg with no matching CDR record (orphaned)");
                }
                
                if (legs.Count == 0) continue;
                groups.Add(legs);
            }

            // Build ProcessedCall objects
            foreach (var legs in groups)
            {
                var orderedLegs = legs
                    .OrderBy(l => l.InLegConnectTime ?? "")
                    .ThenBy(l => l.SourceLine)
                    .ToList();

                for (int i = 0; i < orderedLegs.Count; i++)
                {
                    orderedLegs[i].LegIndex = i + 1;
                }

                // Merge consecutive attempt+answer legs targeting same extension
                orderedLegs = _legMerger.MergeAttemptAnswerLegs(orderedLegs);

                // Direction aggregation at leg level: for each leg, if SIP endpoints indicate external, update direction
                foreach (var leg in orderedLegs)
                {
                    var ingressIsPstn = !string.IsNullOrEmpty(leg.IngressEndpoint) && IsSipPstn(leg.IngressEndpoint);
                    var egressIsPstn = !string.IsNullOrEmpty(leg.EgressEndpoint) && IsSipPstn(leg.EgressEndpoint);
                    var prevDir = leg.CallDirection;
                    
                    // Aggregate direction based on endpoints (most external wins)
                    if (ingressIsPstn && egressIsPstn && leg.CallDirection != CallDirection.TrunkToTrunk)
                        leg.CallDirection = CallDirection.TrunkToTrunk;
                    else if (egressIsPstn && leg.CallDirection == CallDirection.Internal)
                        leg.CallDirection = CallDirection.Outgoing;
                    else if (ingressIsPstn && leg.CallDirection == CallDirection.Internal)
                        leg.CallDirection = CallDirection.Incoming;
                    
                    if (leg.CallDirection != prevDir)
                    {
                        _tracer.TraceDirectionDecision(
                            leg.ThreadId, leg.LegIndex, leg.CallDirection.ToString(),
                            string.Format("SIP PSTN override in AssembleCalls: {0}->{1} (Ingress PSTN={2}, Egress PSTN={3})",
                                prevDir, leg.CallDirection, ingressIsPstn, egressIsPstn),
                            !ingressIsPstn, !egressIsPstn,
                            leg.IngressEndpoint, leg.EgressEndpoint,
                            leg.OrigPartyId, leg.TermPartyId);
                    }
                }

                // Compute Transfer From / Transfer To for each leg
                _transferChainResolver.ComputeTransferChain(orderedLegs);

                //  Capture pre-suppression info for call-level fields 
                // OriginalDialedDigits: from the first leg with a DialedNumber
                // BEFORE CMS suppression and BEFORE "Internal: Dialed=DestExt" override.
                var preSuppressionFirstDialed = orderedLegs
                    .Select(l => l.DialedNumber)
                    .FirstOrDefault(d => !string.IsNullOrEmpty(d));
                // First leg's DestExt before suppression (for Incoming extension)
                var preSuppressionFirstDestExt = orderedLegs.First().DestinationExt;
                // Caller info before suppression (CMS legs may alter the first remaining leg)
                var preSuppressionCallerLeg = orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.CallerExternal))
                    ?? orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.CallerExtension))
                    ?? orderedLegs.First();
                // Capture direction before any modifications (routing number logic may change it)
                var preSuppressionCallerDirection = preSuppressionCallerLeg.CallDirection;
                // Direction aggregation: if ANY leg has external direction, use that
                // Priority: T2T > Outgoing > Incoming > Internal (most external wins)
                var externalLeg = orderedLegs.FirstOrDefault(l =>
                    l.CallDirection == CallDirection.TrunkToTrunk ||
                    l.CallDirection == CallDirection.Outgoing ||
                    l.CallDirection == CallDirection.Incoming);
                if (externalLeg != null)
                {
                    preSuppressionCallerDirection = externalLeg.CallDirection;
                }

                // Auto-detect CMS: numbers that appear as DestinationExt AND CallingNumber
                // in the same call are routing intermediaries (CMS receives call, then calls agent)
                var destinations = new HashSet<string>(
                    orderedLegs
                        .Where(l => !string.IsNullOrEmpty(l.DestinationExt))
                        .Select(l => l.DestinationExt),
                    StringComparer.OrdinalIgnoreCase);
                var callers = new HashSet<string>(
                    orderedLegs
                        .Select(l => l.CallingNumber)
                        .Where(c => !string.IsNullOrEmpty(c)),
                    StringComparer.OrdinalIgnoreCase);
                foreach (var num in destinations.Where(d => callers.Contains(d)))
                {
                    if (!_detectedRoutingNumbers.Contains(num))
                    {
                        _detectedRoutingNumbers.Add(num);
                        _logger.Debug($"Auto-detected routing number (CMS pattern): {num}");
                        _tracer.TraceSpecialNumber(num, "CMS", "CMS pattern: appears as both DestinationExt and CallingNumber in same call");
                    }
                }

                // Suppress routing-only legs (CMS, pilot  only in Transfer From/To)
                SuppressCmsLegs(orderedLegs);

                // After CMS suppression, legs whose TransferFrom is a routing number
                // represent internal routing steps (CMSagent)  direction=Internal
                foreach (var leg in orderedLegs)
                {
                    if (!string.IsNullOrEmpty(leg.TransferFrom) && IsRoutingNumber(leg.TransferFrom)
                        && leg.CallDirection != CallDirection.Internal)
                    {
                        leg.CallDirection = CallDirection.Internal;
                    }
                }

                // General HG propagation: if any leg has HG, forward-propagate
                // to subsequent non-VM legs that don't have one.
                // This handles cases like HGHG2 where leg 2 should inherit parent HG.
                string propagatedHG = null;
                foreach (var leg in orderedLegs)
                {
                    if (!string.IsNullOrEmpty(leg.HuntGroupNumber))
                    {
                        propagatedHG = leg.HuntGroupNumber;
                    }
                    else if (propagatedHG != null && !IsVmLeg(leg))
                    {
                        leg.HuntGroupNumber = propagatedHG;
                    }
                }

                // MLHG detection from PerCallFeatureExt bit 1024 (Call to MLHG)
                // Only apply if no HG was found via normal propagation
                if (propagatedHG == null)
                {
                    var mlhgPilot = orderedLegs
                        .Where(l => (l.PerCallFeatureExt & 1024) != 0 && !string.IsNullOrEmpty(l.CalledParty))
                        .Select(l => l.CalledParty)
                        .FirstOrDefault();
                    if (!string.IsNullOrEmpty(mlhgPilot))
                    {
                        foreach (var leg in orderedLegs.Where(l => string.IsNullOrEmpty(l.HuntGroupNumber)))
                        {
                            leg.HuntGroupNumber = mlhgPilot;
                        }
                    }
                }

                // VM legs: adjust DestinationExt based on whether VM actually answered
                foreach (var vmLeg in orderedLegs.Where(l => IsVmLeg(l)))
                {
                    if (!vmLeg.IsAnswered && vmLeg.Duration == 0 && !string.IsNullOrEmpty(vmLeg.ForwardingParty))
                    {
                        // Unanswered VM leg = the forwarding extension that sent to VM
                        // DestinationExt = the forwarding extension (where the call rang)
                        vmLeg.DestinationExt = vmLeg.ForwardingParty;
                        vmLeg.CalledExtension = vmLeg.ForwardingParty;
                    }
                    else
                    {
                        // Answered VM leg: DestinationExt = VM code
                        var vmNum = GetVoicemailNumber();
                        if (!string.IsNullOrEmpty(vmNum) && vmLeg.DestinationExt != vmNum)
                        {
                            vmLeg.DestinationExt = vmNum;
                            vmLeg.CalledExtension = vmNum;
                        }
                    }
                }

                // Rule: for Internal calls, Dialed = Dest Ext (always show destination extension)
                foreach (var leg in orderedLegs)
                {
                    if (leg.CallDirection == CallDirection.Internal && !string.IsNullOrEmpty(leg.DestinationExt))
                    {
                        leg.DialedNumber = leg.DestinationExt;
                    }
                }

                if (orderedLegs.Count == 0) continue; // All legs were routing-only

                var firstLeg = orderedLegs.First();
                var answeredLeg = orderedLegs.LastOrDefault(l => l.IsAnswered);
                var lastLeg = orderedLegs.Last();

                // Use pre-suppression caller info (CMS suppression may remove the original caller leg)
                var callerLeg = preSuppressionCallerLeg;

                // Find the best dialed number (post-suppression)
                var dialedLeg = orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.DialedNumber))
                    ?? firstLeg;

                var call = new ProcessedCall
                {
                    GlobalCallId = firstLeg.GlobalCallId,
                    TotalLegs = orderedLegs.Count,
                    IsAnswered = orderedLegs.Any(l => l.IsAnswered),
                    // Use the maximum duration among answered legs (not sum).
                    // CMS/HG scenarios have multiple "answered" routing legs;
                    // the primary leg has the longest duration.
                    TotalDuration = orderedLegs.Where(l => l.IsAnswered).DefaultIfEmpty()
                        .Max(l => l?.Duration ?? 0),
                    CallerExtension = callerLeg.CallerExtension,
                    CallerExternal = callerLeg.CallerExternal,
                    DialedNumber = dialedLeg.DialedNumber,
                    // Original Dialed Digits: from the first leg BEFORE CMS suppression
                    OriginalDialedDigits = preSuppressionFirstDialed,
                    HuntGroupNumber = orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.HuntGroupNumber))?.HuntGroupNumber,
                    ThreadId = firstLeg.ThreadId,
                    CallDirection = preSuppressionCallerDirection,
                    Legs = orderedLegs,
                };

                // Detect TrunkToTrunk at call level: external caller with external destination
                // (leg direction may be Incoming due to forwarding, but call is T2T)
                if (!string.IsNullOrEmpty(call.CallerExternal) && string.IsNullOrEmpty(call.CallerExtension))
                {
                    // Caller is external. Check if destination is also external.
                    var anyPstnToPstn = _extensionRange.IsEmpty && orderedLegs.Any(l =>
                        (IsSipKnown(l.IngressEndpoint)
                            && IsSipPstn(l.IngressEndpoint)
                            && IsSipKnown(l.EgressEndpoint)
                            && IsSipPstn(l.EgressEndpoint))
);

                    var anyInternalDest = orderedLegs.Any(l =>
                        (_extensionRange.IsEmpty
                            ? IsInternalDestForEmptyRanges(l)
                            : (IsInternalNumber(l.DestinationExt) || IsInternalNumber(l.CalledExtension))));
                    if (anyPstnToPstn || !anyInternalDest)
                    {
                        call.CallDirection = CallDirection.TrunkToTrunk;
                    }
                }
                // Update call.DialedNumber after Internal override (it may have been empty before)
                if (string.IsNullOrEmpty(call.DialedNumber))
                {
                    var updatedDialedLeg = orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.DialedNumber));
                    if (updatedDialedLeg != null) call.DialedNumber = updatedDialedLeg.DialedNumber;
                }

                // Fallback: if OriginalDialedDigits is empty but DialedNumber is set, use DialedNumber
                if (string.IsNullOrEmpty(call.OriginalDialedDigits) && !string.IsNullOrEmpty(call.DialedNumber))
                {
                    call.OriginalDialedDigits = call.DialedNumber;
                }

                // Extension = "our extension" regardless of direction
                // Incoming: the first leg's DestExt (before suppression)  shows which
                //           extension the call was originally directed to
                // Outgoing: the extension that made the call (CallerExtension)
                // Internal: the originating extension (CallerExtension)
                // T2T: the extension that triggered the trunk-to-trunk (ForwardingParty or caller)
                switch (call.CallDirection)
                {
                    case CallDirection.Incoming:
                        // Use first leg's DestExt from before CMS suppression
                        call.Extension = preSuppressionFirstDestExt;
                        if (string.IsNullOrEmpty(call.Extension))
                            call.Extension = (answeredLeg ?? lastLeg).DestinationExt;
                        break;
                    case CallDirection.Outgoing:
                        call.Extension = call.CallerExtension;
                        break;
                    case CallDirection.Internal:
                        call.Extension = call.CallerExtension;
                        break;
                    case CallDirection.TrunkToTrunk:
                        // The extension that triggered the T2T (forwarding party or caller)
                        call.Extension = orderedLegs
                            .Select(l => l.ForwardingParty)
                            .FirstOrDefault(fp => !string.IsNullOrEmpty(fp))
                            ?? call.CallerExtension;
                        break;
                    default:
                        call.Extension = call.CallerExtension;
                        break;
                }

                // Apply shared leg post-processing (direction, DialedAni, Extension/DestExt)
                ApplyLegPostProcessing(call, orderedLegs);

                // NOTE: Feature code filtering (*44/#44) is now handled early in ProcessSingleRecord()
                // before leg creation. Any feature code calls that somehow reach here are unexpected.

                // === T2T splitting: split TrunkToTrunk calls into T2T-In + T2T-Out ===
                // Only split when there's an internal extension involved (ForwardingParty).
                // Pure T2T (external-to-external with no internal routing) stays as 1 call.
                HashSet<string> internalNums = null;
                if (_extensionRange.IsEmpty)
                {
                    internalNums = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    foreach (var l in orderedLegs)
                    {
                        if (l.OrigPartyId == 900 || l.TermPartyId == 902)
                        {
                            if (!string.IsNullOrEmpty(l.CallingNumber)) internalNums.Add(l.CallingNumber);
                            if (!string.IsNullOrEmpty(l.CalledParty)) internalNums.Add(l.CalledParty);
                            if (!string.IsNullOrEmpty(l.DestinationExt)) internalNums.Add(l.DestinationExt);
                            if (!string.IsNullOrEmpty(l.CalledExtension)) internalNums.Add(l.CalledExtension);
                            if (!string.IsNullOrEmpty(l.ForwardingParty)) internalNums.Add(l.ForwardingParty);
                        }
                    }
                }

                var pstnToPstn = _extensionRange.IsEmpty && orderedLegs.Any(l =>
                    IsSipPstn(l.IngressEndpoint) && IsSipPstn(l.EgressEndpoint));

                var forwardingExt = orderedLegs
                    .Select(l => l.ForwardingParty)
                    .FirstOrDefault(fp =>
                        !string.IsNullOrEmpty(fp) &&
                        (_extensionRange.IsEmpty
                            ? (internalNums != null && (internalNums.Contains(fp)
                                || (pstnToPstn && !IsRoutingNumber(fp))))
                            : IsInternalNumber(fp)));
                
                if (call.CallDirection == CallDirection.TrunkToTrunk && orderedLegs.Count > 0
                    && !string.IsNullOrEmpty(forwardingExt))
                {
                    var t2tLeg = orderedLegs[0];
                    var ext = forwardingExt;
                    var external1 = call.CallerExternal ?? t2tLeg.CallerExternal ?? "";
                    var t2tExternalDest = orderedLegs
                        .Select(l => l.CalledExternal)
                        .FirstOrDefault(e => !string.IsNullOrEmpty(e)) ?? "";
                    var external2 = !string.IsNullOrEmpty(t2tExternalDest) ? t2tExternalDest : (t2tLeg.DialedAni ?? "");
                    var transferFrom = ext;
                    var originalDnis = t2tLeg.DialedNumber ?? "";

                    _logger.Debug($"Splitting T2T: {external1} -> {ext} -> {external2}");

                    // T2T-In: External caller -> Internal extension
                    var t2tInLeg = new ProcessedLeg
                    {
                        GlobalCallId = t2tLeg.GlobalCallId,
                        ThreadId = t2tLeg.ThreadId,
                        GidSequence = t2tLeg.GidSequence,
                        LegIndex = 1,
                        CallDirection = CallDirection.T2TIn,
                        CallerExternal = external1,
                        CallingNumber = external1,
                        CalledExtension = ext,
                        CalledParty = ext,
                        DestinationExt = "",
                        Extension = ext,
                        DialedNumber = ext,
                        DialedAni = external1,
                        TransferFrom = "", //incoming leg, no transfer from
                        TransferTo = "",
                        Duration = call.TotalDuration,
                        IsAnswered = call.IsAnswered,
                        IsForwarded = t2tLeg.IsForwarded,
                        HuntGroupNumber = t2tLeg.HuntGroupNumber,
                        CauseCode = t2tLeg.CauseCode,
                        CauseCodeText = t2tLeg.CauseCodeText,
                        PerCallFeature = t2tLeg.PerCallFeature,
                        PerCallFeatureText = t2tLeg.PerCallFeatureText,
                        AttemptIndicator = t2tLeg.AttemptIndicator,
                        AttemptIndicatorText = t2tLeg.AttemptIndicatorText,
                        InLegConnectTime = t2tLeg.InLegConnectTime,
                        CallAnswerTime = t2tLeg.CallAnswerTime,
                        SourceFile = t2tLeg.SourceFile,
                        SourceLine = t2tLeg.SourceLine,
                    };

                    var t2tInCall = new ProcessedCall
                    {
                        GlobalCallId = call.GlobalCallId,
                        CallDirection = CallDirection.T2TIn,
                        TotalLegs = 1,
                        IsAnswered = call.IsAnswered,
                        TotalDuration = call.TotalDuration,
                        CallerExternal = external1,
                        Extension = ext,
                        DialedNumber = ext,
                        OriginalDialedDigits = originalDnis,
                        HuntGroupNumber = call.HuntGroupNumber,
                        ThreadId = call.ThreadId,
                        Legs = new List<ProcessedLeg> { t2tInLeg },
                    };

                    // T2T-Out: Internal extension -> External destination
                    var t2tOutLeg = new ProcessedLeg
                    {
                        GlobalCallId = t2tLeg.GlobalCallId,
                        ThreadId = t2tLeg.ThreadId,
                        GidSequence = t2tLeg.GidSequence,
                        LegIndex = 2,
                        CallDirection = CallDirection.T2TOut,
                        CallerExtension = ext,
                        CallingNumber = ext,
                        CalledExternal = external2,
                        CalledParty = external2,
                        DestinationExt = "",
                        Extension = ext,
                        DialedNumber = external2,
                        DialedAni = external2,
                        TransferFrom = transferFrom,
                        TransferTo = "",
                        Duration = call.TotalDuration,
                        IsAnswered = call.IsAnswered,
                        IsForwarded = t2tLeg.IsForwarded,
                        HuntGroupNumber = t2tLeg.HuntGroupNumber,
                        CauseCode = t2tLeg.CauseCode,
                        CauseCodeText = t2tLeg.CauseCodeText,
                        PerCallFeature = t2tLeg.PerCallFeature,
                        PerCallFeatureText = t2tLeg.PerCallFeatureText,
                        AttemptIndicator = t2tLeg.AttemptIndicator,
                        AttemptIndicatorText = t2tLeg.AttemptIndicatorText,
                        InLegConnectTime = t2tLeg.InLegConnectTime,
                        CallAnswerTime = t2tLeg.CallAnswerTime,
                        SourceFile = t2tLeg.SourceFile,
                        SourceLine = t2tLeg.SourceLine,
                    };

                    var t2tOutCall = new ProcessedCall
                    {
                        GlobalCallId = call.GlobalCallId + "_out",
                        CallDirection = CallDirection.T2TOut,
                        TotalLegs = 1,
                        IsAnswered = call.IsAnswered,
                        TotalDuration = call.TotalDuration,
                        CallerExtension = ext,
                        Extension = ext,
                        DialedNumber = external2,
                        OriginalDialedDigits = originalDnis,
                        HuntGroupNumber = call.HuntGroupNumber,
                        ThreadId = call.ThreadId,
                        Legs = new List<ProcessedLeg> { t2tOutLeg },
                    };

                    EmitCall(t2tInCall, result);
                    EmitCall(t2tOutCall, result);
                    result.TotalCallsIdentified += 2;
                    continue; // Don't add the original T2T call
                }
                else if (call.CallDirection == CallDirection.TrunkToTrunk)
                {
                    var forwardingCandidates = JoinValues(
                        orderedLegs.Select(l => l.ForwardingParty).Distinct(StringComparer.OrdinalIgnoreCase));
                    var callingCandidates = JoinValues(
                        orderedLegs.Select(l => l.CallingNumber).Distinct(StringComparer.OrdinalIgnoreCase));
                    var destinationCandidates = JoinValues(
                        orderedLegs.Select(l => l.DestinationExt).Distinct(StringComparer.OrdinalIgnoreCase));
                    var calledPartyCandidates = JoinValues(
                        orderedLegs.Select(l => l.CalledParty).Distinct(StringComparer.OrdinalIgnoreCase));

                    _logger.Info(
                        "T2T split skipped. ThreadId={0}, GlobalCallId={1}, LegsCount={2}, ForwardingExt={3}, ForwardingCandidates={4}, ExtensionRangeIsEmpty={5}, InternalNumsCount={6}, PstnToPstn={7}, CallerExternal={8}, CallerExtension={9}, CallingCandidates={10}, DestinationCandidates={11}, CalledPartyCandidates={12}",
                        call.ThreadId,
                        call.GlobalCallId,
                        orderedLegs.Count,
                        forwardingExt ?? string.Empty,
                        forwardingCandidates,
                        _extensionRange.IsEmpty,
                        internalNums?.Count ?? 0,
                        pstnToPstn,
                        call.CallerExternal ?? string.Empty,
                        call.CallerExtension ?? string.Empty,
                        callingCandidates,
                        destinationCandidates,
                        calledPartyCandidates);
                }

                EmitCall(call, result);
                result.TotalCallsIdentified++;
            }
        }

        private HashSet<string> GetCallers(List<ProcessedLeg> legs)
        {
            var callers = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var leg in legs)
            {
                if (!string.IsNullOrEmpty(leg.CallerExternal))
                    callers.Add(leg.CallerExternal);
                if (!string.IsNullOrEmpty(leg.CallerExtension))
                    callers.Add(leg.CallerExtension);
            }
            return callers;
        }

        private HashSet<string> GetDialedNumbers(List<ProcessedLeg> legs)
        {
            var dialed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var leg in legs)
            {
                if (!string.IsNullOrEmpty(leg.DialedNumber))
                    dialed.Add(leg.DialedNumber);
            }
            return dialed;
        }

        private bool AreWithinTimeWindow(List<ProcessedLeg> groupA, List<ProcessedLeg> groupB, int windowSeconds)
        {
            // Find earliest and latest setup times in each group
            foreach (var legA in groupA)
            {
                foreach (var legB in groupB)
                {
                    if (string.IsNullOrEmpty(legA.InLegConnectTime) || string.IsNullOrEmpty(legB.InLegConnectTime))
                        continue;

                    // Simple string comparison works for ISO timestamps
                    var diff = Math.Abs(StringTimeApproxDiffSeconds(legA.InLegConnectTime, legB.InLegConnectTime));
                    if (diff <= windowSeconds)
                        return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Approximate time difference in seconds between two ISO timestamp strings.
        /// </summary>
        private double StringTimeApproxDiffSeconds(string timeA, string timeB)
        {
            DateTime dtA;
            DateTime dtB;
            if (DateTime.TryParse(timeA, out dtA) && DateTime.TryParse(timeB, out dtB))
            {
                return (dtA - dtB).TotalSeconds;
            }
            return double.MaxValue;
        }

        private bool IsInternalNumber(string number)
        {
            if (string.IsNullOrEmpty(number)) return false;
            return _extensionRange.IsExtension(number);
        }

        private void DetectCandidateExtension(string number, string fieldName, RawCdrRecord raw)
        {
            if (string.IsNullOrEmpty(number)) return;
            // If range configured and number is already known, skip
            // If no range configured, run in discovery mode (report all heuristic matches)
            if (!_extensionRange.IsEmpty && _extensionRange.IsExtension(number)) return;

            // Detect internal numbers using PBX metadata (OrigPartyId)
            string reason = null;

            if (fieldName == "CallingNumber" && raw.OrigPartyId == 900)
                reason = "Internal origin (OrigPartyId=900, caller on OpenScape)";
            else if ((fieldName == "DestinationExt" || fieldName == "DialedNumber") && raw.OrigPartyId == 902)
                reason = "Internal destination (OrigPartyId=902, destination on OpenScape)";

            if (reason == null) return;

            CandidateExtension candidate;
            if (!_candidates.TryGetValue(number, out candidate))
            {
                candidate = new CandidateExtension { Number = number };
                _candidates[number] = candidate;
            }

            candidate.Occurrences++;
            if (!candidate.Reasons.Contains(reason))
                candidate.Reasons.Add(reason);
        }

        // 
        // Streaming Output & Memory Management
        // 

        /// <summary>
        /// Check if a call is ready for early output (direction is reliably determined).
        /// Returns false for potential T2T calls to avoid wrong direction output.
        /// </summary>
        private bool IsCallReadyForEarlyOutput(List<ProcessedLeg> legs, out CallDirection direction)
        {
            direction = CallDirection.Unknown;
            if (legs == null || legs.Count == 0) return false;

            var firstWithDirection = legs.FirstOrDefault(l => l.CallDirection != CallDirection.Unknown);
            if (firstWithDirection == null) return false;

            // Check if caller is external (potential T2T)
            var hasExternalCaller = legs.Any(l =>
                !string.IsNullOrEmpty(l.CallerExternal) &&
                string.IsNullOrEmpty(l.CallerExtension));

            if (hasExternalCaller)
            {
                // External caller: need to confirm it's not T2T
                // T2T = external caller + ALL destinations are external
                // If we see an internal destination => confirmed Incoming
                // If no internal destination yet => might be T2T, wait
                var hasInternalDest = legs.Any(l =>
                    !string.IsNullOrEmpty(l.DestinationExt) &&
                    IsInternalNumber(l.DestinationExt));

                if (!hasInternalDest)
                {
                    // Could be T2T - check for forwarding (strong T2T indicator)
                    var hasForwarding = legs.Any(l => l.IsForwarded || !string.IsNullOrEmpty(l.ForwardingParty));
                    if (hasForwarding)
                    {
                        return false; // Might be T2T, wait for more legs
                    }
                }

                direction = CallDirection.Incoming;
                return true;
            }

            // Internal caller: use first leg's direction
            direction = firstWithDirection.CallDirection;
            return true;
        }

        /// <summary>
        /// Called after storing a leg. Checks for early output and enforces cache limit.
        /// </summary>
        private void CheckStreamingOutput(string threadId, ProcessingResult result)
        {
            // Early output (when enabled): output calls as soon as direction is known
            if (EnableCallCompletionDetection && !_outputtedThreadIds.Contains(threadId))
            {
                var legs = _cache.GetPendingLegs(threadId);
                CallDirection direction;
                if (IsCallReadyForEarlyOutput(legs, out direction))
                {
                    var call = AssembleSingleCall(legs);
                    if (call != null)
                    {
                        HandleEarlyOutputCall(call, result);
                        _outputtedThreadIds.Add(threadId);
                        _cache.RemoveCall(threadId);
                        _logger.Debug($"Early output: {threadId} as {call.CallDirection}");
                    }
                }
            }

            // Always enforce cache limit via eviction (production default)
            EnforceCacheLimit(result);
        }

        /// <summary>
        /// Evict oldest calls when cache exceeds MaxCachedLegs.
        /// </summary>
        private void EnforceCacheLimit(ProcessingResult result)
        {
            if (MaxCachedLegs <= 0) return; // No limit

            while (_cache.Count > MaxCachedLegs)
            {
                // Find oldest call (by earliest leg timestamp)
                string oldestKey = null;
                string oldestTime = null;

                foreach (var key in _cache.GetAllGids())
                {
                    if (_outputtedThreadIds.Contains(key)) continue;
                    var legs = _cache.GetPendingLegs(key);
                    var earliest = legs.Min(l => l.InLegConnectTime ?? "");
                    if (oldestTime == null || string.Compare(earliest, oldestTime, StringComparison.Ordinal) < 0)
                    {
                        oldestTime = earliest;
                        oldestKey = key;
                    }
                }

                if (oldestKey == null) break; // Nothing to evict

                var evictLegs = _cache.GetPendingLegs(oldestKey);
                var evictCall = AssembleSingleCall(evictLegs);
                if (evictCall != null)
                {
                    HandleEarlyOutputCall(evictCall, result);
                    _logger.Debug($"Eviction output: {oldestKey} (cache={_cache.Count})");
                }
                _outputtedThreadIds.Add(oldestKey);
                _cache.RemoveCall(oldestKey);
            }
        }

        /// <summary>
        /// Assemble a single call from legs (used for early/eviction output).
        /// Simplified version of AssembleCalls for single-call processing.
        /// </summary>
        private ProcessedCall AssembleSingleCall(List<ProcessedLeg> legs)
        {
            if (legs == null || legs.Count == 0) return null;

            // Filter out HG-only legs
            legs = legs.Where(l => !l.IsHgOnly).ToList();
            if (legs.Count == 0) return null;

            var orderedLegs = legs
                .OrderBy(l => l.InLegConnectTime ?? "")
                .ThenBy(l => l.SourceLine)
                .ToList();

            for (int i = 0; i < orderedLegs.Count; i++)
                orderedLegs[i].LegIndex = i + 1;

            // Apply standard transformations
            orderedLegs = _legMerger.MergeAttemptAnswerLegs(orderedLegs);

            // Direction aggregation at leg level: for each leg, if SIP endpoints indicate external, update direction
            foreach (var leg in orderedLegs)
            {
                var ingressIsPstn = !string.IsNullOrEmpty(leg.IngressEndpoint) && IsSipPstn(leg.IngressEndpoint);
                var egressIsPstn = !string.IsNullOrEmpty(leg.EgressEndpoint) && IsSipPstn(leg.EgressEndpoint);
                
                // Aggregate direction based on endpoints (most external wins)
                if (ingressIsPstn && egressIsPstn && leg.CallDirection != CallDirection.TrunkToTrunk)
                    leg.CallDirection = CallDirection.TrunkToTrunk;
                else if (egressIsPstn && leg.CallDirection == CallDirection.Internal)
                    leg.CallDirection = CallDirection.Outgoing;
                else if (ingressIsPstn && leg.CallDirection == CallDirection.Internal)
                    leg.CallDirection = CallDirection.Incoming;
            }

            _transferChainResolver.ComputeTransferChain(orderedLegs);

            var preSuppressionFirstDialed = orderedLegs
                .Select(l => l.DialedNumber)
                .FirstOrDefault(d => !string.IsNullOrEmpty(d));
            var preSuppressionFirstDestExt = orderedLegs.First().DestinationExt;
            var preSuppressionCallerLeg = orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.CallerExternal))
                ?? orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.CallerExtension))
                ?? orderedLegs.First();
            var preSuppressionCallerDirection = preSuppressionCallerLeg.CallDirection;
                // Direction aggregation: if ANY leg has external direction, use that
                // Priority: T2T > Outgoing > Incoming > Internal (most external wins)
                var externalLeg = orderedLegs.FirstOrDefault(l =>
                    l.CallDirection == CallDirection.TrunkToTrunk ||
                    l.CallDirection == CallDirection.Outgoing ||
                    l.CallDirection == CallDirection.Incoming);
                if (externalLeg != null)
                {
                    preSuppressionCallerDirection = externalLeg.CallDirection;
                }

            SuppressCmsLegs(orderedLegs);

            if (orderedLegs.Count == 0) return null;

            var firstLeg = orderedLegs.First();
            var answeredLeg = orderedLegs.LastOrDefault(l => l.IsAnswered);
            var callerLeg = preSuppressionCallerLeg;
            var dialedLeg = orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.DialedNumber)) ?? firstLeg;

            var call = new ProcessedCall
            {
                GlobalCallId = firstLeg.GlobalCallId,
                TotalLegs = orderedLegs.Count,
                IsAnswered = orderedLegs.Any(l => l.IsAnswered),
                TotalDuration = orderedLegs.Where(l => l.IsAnswered).DefaultIfEmpty().Max(l => l?.Duration ?? 0),
                CallerExtension = callerLeg.CallerExtension,
                CallerExternal = callerLeg.CallerExternal,
                DialedNumber = dialedLeg.DialedNumber,
                OriginalDialedDigits = preSuppressionFirstDialed,
                HuntGroupNumber = orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.HuntGroupNumber))?.HuntGroupNumber,
                ThreadId = firstLeg.ThreadId,
                CallDirection = preSuppressionCallerDirection,
                Legs = orderedLegs,
            };

            // T2T detection at call level
            if (!string.IsNullOrEmpty(call.CallerExternal) && string.IsNullOrEmpty(call.CallerExtension))
            {
                var anyInternalDest = orderedLegs.Any(l =>
                    IsInternalNumber(l.DestinationExt) || IsInternalNumber(l.CalledExtension));
                if (!anyInternalDest)
                    call.CallDirection = CallDirection.TrunkToTrunk;
            }

            // === POST-PROCESSING (shared with AssembleCalls) ===

            // VM legs: adjust DestinationExt based on whether VM actually answered
            foreach (var vmLeg in orderedLegs.Where(l => IsVmLeg(l)))
            {
                if (!vmLeg.IsAnswered && vmLeg.Duration == 0 && !string.IsNullOrEmpty(vmLeg.ForwardingParty))
                {
                    vmLeg.DestinationExt = vmLeg.ForwardingParty;
                    vmLeg.CalledExtension = vmLeg.ForwardingParty;
                }
                else
                {
                    var vmNum = GetVoicemailNumber();
                    if (!string.IsNullOrEmpty(vmNum) && vmLeg.DestinationExt != vmNum)
                    {
                        vmLeg.DestinationExt = vmNum;
                        vmLeg.CalledExtension = vmNum;
                    }
                }
            }

            // Rule: for Internal calls, Dialed = Dest Ext
            foreach (var leg in orderedLegs)
            {
                if (leg.CallDirection == CallDirection.Internal && !string.IsNullOrEmpty(leg.DestinationExt))
                {
                    leg.DialedNumber = leg.DestinationExt;
                }
            }
            // Update call.DialedNumber after Internal override (it may have been empty before)
            if (string.IsNullOrEmpty(call.DialedNumber))
            {
                var updatedDialedLeg = orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.DialedNumber));
                if (updatedDialedLeg != null) call.DialedNumber = updatedDialedLeg.DialedNumber;
            }

            // Fallback: if OriginalDialedDigits is empty but DialedNumber is set, use DialedNumber
            if (string.IsNullOrEmpty(call.OriginalDialedDigits) && !string.IsNullOrEmpty(call.DialedNumber))
            {
                call.OriginalDialedDigits = call.DialedNumber;
            }

            // Set Extension based on direction
            switch (call.CallDirection)
            {
                case CallDirection.Incoming:
                    call.Extension = preSuppressionFirstDestExt;
                    if (string.IsNullOrEmpty(call.Extension))
                        call.Extension = (answeredLeg ?? orderedLegs.Last()).DestinationExt;
                    break;
                case CallDirection.Outgoing:
                case CallDirection.Internal:
                    call.Extension = call.CallerExtension;
                    break;
                case CallDirection.TrunkToTrunk:
                    call.Extension = orderedLegs
                        .Select(l => l.ForwardingParty)
                        .FirstOrDefault(fp => !string.IsNullOrEmpty(fp))
                        ?? call.CallerExtension;
                    break;
            }

            // Apply shared leg post-processing (direction, DialedAni, Extension/DestExt)
            ApplyLegPostProcessing(call, orderedLegs);

            return call;
        }
    }
}

