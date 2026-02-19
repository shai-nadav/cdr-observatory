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
        private readonly Pipeline.LegSuppressor _legSuppressor;
        private readonly Pipeline.CallFinalizer _callFinalizer;
        private readonly Pipeline.CallAssembler _callAssembler;
        private readonly Pipeline.LegBuilder _legBuilder;
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
            _legSuppressor = new Pipeline.LegSuppressor(_pipelineContext);
            _callFinalizer = new Pipeline.CallFinalizer(_pipelineContext);
            _callAssembler = new Pipeline.CallAssembler(_pipelineContext, _legMerger, _transferChainResolver, _legSuppressor, _callFinalizer, _outputtedThreadIds, EmitCall);
            _legBuilder = new Pipeline.LegBuilder(_pipelineContext, CalculateRingTime, GetGidHex, CheckStreamingOutput, DetectCandidateExtension);

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
            _legSuppressor = new Pipeline.LegSuppressor(_pipelineContext);
            _callFinalizer = new Pipeline.CallFinalizer(_pipelineContext);
            _callAssembler = new Pipeline.CallAssembler(_pipelineContext, _legMerger, _transferChainResolver, _legSuppressor, _callFinalizer, _outputtedThreadIds, EmitCall);
            _legBuilder = new Pipeline.LegBuilder(_pipelineContext, CalculateRingTime, GetGidHex, CheckStreamingOutput, DetectCandidateExtension);
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
            _callAssembler.AssembleCalls(result);

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
                    _legBuilder.ProcessFullCdr(raw, result);
                    break;
                case CdrRecordType.HuntGroup:
                    _legBuilder.ProcessHuntGroup(raw, result);
                    break;
                case CdrRecordType.CallForward:
                    _legBuilder.ProcessCallForward(raw, result);
                    break;
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

        /// <summary>
        /// Compute Transfer From on a single leg (without inheritance).
        /// All rules are field-condition based  no hardcoded numbers.
        ///
        /// Priority order matters:
        ///   1. CalledParty is a routing intermediary  CalledParty
        ///      (HG/queue number that routed the call; overrides ForwardingParty
        ///      because the HG is more specific than a pilot/redirect source)
        ///   2. ForwardingParty is set  ForwardingParty
        ///      (the extension that forwarded/redirected the call)
        ///   3. CallingNumber differs from original caller  CallingNumber
        ///      (a routing agent like CMS placed the call on behalf of the caller)
        ///   4. CalledParty differs from DestinationExt (DestExt present, not VM)
        ///       CalledParty (catch-all for intermediate routing not caught above)
        /// </summary>

        /// <summary>
        /// Compute Transfer From / Transfer To for all legs in a call.
        ///
        /// GENERAL RULES (field-condition based, no hardcoded numbers):
        ///
        /// Transfer From = "who routed/transferred this call to this leg"
        ///   - See ComputeLegTransferFrom for the 4 priority rules
        ///   - No match  inherit previous leg's TransferFrom
        ///
        /// Transfer To = "where the call goes next"
        ///   - VM leg (CalledParty = configured VM code)  CalledParty
        ///   - Has next leg  next leg's Transfer From (look-ahead), or next leg's
        ///     DestinationExt, or next leg's CalledParty
        ///   - Dedup: if Transfer To == Transfer From, use next leg's endpoint instead
        ///   - Last leg  null
        /// </summary>


        /// <summary>
        /// Suppress routing-only legs from the output.
        /// Routing numbers (CMS, pilot, etc.) that act as pure pass-throughs
        /// should not appear as call records. Info is preserved in Transfer From/To.
        ///
        /// Must be called AFTER ComputeTransferChain (so CMS numbers are already
        /// in the transfer fields) and BEFORE computing call-level fields.
        /// </summary>

        /// <summary>
        /// Apply post-processing to legs after call assembly: direction propagation,
        /// DialedAni computation, Extension/DestExt field handling.
        /// Shared between AssembleCalls and AssembleSingleCall to avoid duplication.
        /// </summary>

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




        /// <summary>
        /// Approximate time difference in seconds between two ISO timestamp strings.
        /// </summary>

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
                    var call = _callAssembler.AssembleSingleCall(legs);
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
                var evictCall = _callAssembler.AssembleSingleCall(evictLegs);
                if (evictCall != null)
                {
                    HandleEarlyOutputCall(evictCall, result);
                    _logger.Debug($"Eviction output: {oldestKey} (cache={_cache.Count})");
                }
                _outputtedThreadIds.Add(oldestKey);
                _cache.RemoveCall(oldestKey);
            }
        }

    }
}

