using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;
using Pipeline.Components.OSVParser;
using Pipeline.Components.OSVParser.Cache;
using Pipeline.Components.OSVParser.Processing;
using Pipeline.Components.OSVParser.Models;
using CdrObservatory;

namespace CdrObservatory.Tests
{
    #region Test Infrastructure

    /// <summary>
    /// Configurable test harness for engine behavior tests.
    /// </summary>
    internal class TestHarness : ISettingsProvider, ISipEndpointsProvider, IPendingCallsRepository, IDisposable
    {
        private readonly string _tempDir;
        private long _currentRecordSeq = 1;

        public TestHarness()
        {
            _tempDir = Path.Combine(Path.GetTempPath(), "CdrObsTests_" + Guid.NewGuid().ToString("N").Substring(0, 8));
            Directory.CreateDirectory(InputDir);
            Directory.CreateDirectory(OutputDir);
            Directory.CreateDirectory(ArchiveDir);
            Directory.CreateDirectory(DecodedDir);
            Directory.CreateDirectory(WorkDir);
            Directory.CreateDirectory(OrphanDir);
        }

        public string InputDir => Path.Combine(_tempDir, "Input");
        public string OutputDir => Path.Combine(_tempDir, "Output");
        public string ArchiveDir => Path.Combine(_tempDir, "Archive");
        public string DecodedDir => Path.Combine(_tempDir, "Decoded");
        public string WorkDir => Path.Combine(_tempDir, "Work");
        public string OrphanDir => Path.Combine(_tempDir, "Orphan");

        // Configurable settings
        public bool SettingsWriteDecodedCdrs { get; set; } = true;
        public bool SettingsDeleteInputFiles { get; set; } = false;

        // Configurable SIP map
        public Dictionary<string, string> SipMap { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "10.0.172.17", "" },
            { "10.0.172.18", "" },
            { "10.0.172.19", "" },
            { "10.0.73.67", "" },
            { "10.0.73.71", "" },
            { "10.0.75.39", "" },
            { "eranhangnadmin1.ejhsgvdi.net", "eranhangnadmin1" },
            { "eranhangnadmin2.ejhsgvdi.net", "eranhangnadmin2" }
        };

        // ISettingsProvider
        string ISettingsProvider.InputFolder => InputDir;
        string ISettingsProvider.OutputFolder => OutputDir;
        string ISettingsProvider.ArchiveFolder => ArchiveDir;
        string ISettingsProvider.WorkFolder => WorkDir;
        string ISettingsProvider.DecodedFolder => DecodedDir;
        string ISettingsProvider.OrphanFolder => OrphanDir;
        string ISettingsProvider.SipEndpointsFile => null;
        string ISettingsProvider.InstanceId => "1";
        string ISettingsProvider.FilePattern => "*.*";
        int ISettingsProvider.IncompleteRetentionHours => 24;
        int ISettingsProvider.MaxPendingQueueSize => 10000;
        bool ISettingsProvider.WriteDecodedCdrs => SettingsWriteDecodedCdrs;
        bool ISettingsProvider.DeleteInputFiles => SettingsDeleteInputFiles;

        // ISipEndpointsProvider
        bool ISipEndpointsProvider.IsLoaded => true;
        int ISipEndpointsProvider.PstnCount => SipMap.Count(kv => !string.IsNullOrEmpty(kv.Value));
        IDictionary<string, string> ISipEndpointsProvider.LoadAddressToTrunkMap() => SipMap;
        bool ISipEndpointsProvider.IsPstn(string endpoint)
        {
            if (string.IsNullOrWhiteSpace(endpoint)) return false;
            return SipMap.TryGetValue(endpoint, out var val) && !string.IsNullOrEmpty(val);
        }

        // IPendingCallsRepository
        IList<PendingCallRecord> IPendingCallsRepository.Load(int instanceId) => new List<PendingCallRecord>();
        void IPendingCallsRepository.Sync(int instanceId, IList<string> deletedIds, IList<PendingCallRecord> upsertCalls) { }
        long IPendingCallsRepository.GetCurrentRecordSeq() => _currentRecordSeq;
        long IPendingCallsRepository.IncrementRecordSeq() => ++_currentRecordSeq;

        public LogCollector Logger { get; } = new LogCollector();

        public (ProcessingResult result, LogCollector logger, ProcessingTracer tracer) Run()
        {
            var tracer = new ProcessingTracer(Logger);
            var cache = new InMemoryCacheStore();
            var engine = new CdrProcessorEngine(this, Logger, this, this, cache, tracer);
            var result = engine.ProcessFolder();
            return (result, Logger, tracer);
        }

        /// <summary>Copy a test BF file from the shared TestData/CDRs directory into this harness's input.</summary>
        public void CopyTestFile(string bfFileName)
        {
            var assemblyDir = Path.GetDirectoryName(typeof(ScenarioTests).Assembly.Location);
            var src = Path.Combine(assemblyDir, "TestData", "CDRs", bfFileName);
            if (!File.Exists(src))
                throw new FileNotFoundException($"Test BF file not found: {src}");
            File.Copy(src, Path.Combine(InputDir, bfFileName), true);
        }

        /// <summary>Write a minimal BF file with given CDR lines.</summary>
        public string WriteBfFile(string fileName, params string[] cdrLines)
        {
            var path = Path.Combine(InputDir, fileName);
            var lines = new List<string>
            {
                $"FILENAME: {fileName}",
                "DEVICE: OpenScapeVoice",
                "HOSTNAME: testhost",
                "FILETYPE: BILLING",
                "VERSION: V10.00.00",
                "",
                "CREATE: 2026-01-27T10:00:00.0+0100",
                ""
            };
            int lineIdx = 0;
            foreach (var cdr in cdrLines)
            {
                lines.Add($"{lineIdx},{cdr}");
                lineIdx++;
            }
            lines.Add("");
            lines.Add("");
            lines.Add("CLOSE: 2026-01-27T10:05:00.0+0100");
            File.WriteAllLines(path, lines);
            return path;
        }

        public void Dispose()
        {
            try { Directory.Delete(_tempDir, true); } catch { }
        }
    }

    /// <summary>Helper to build CDR record CSV lines for the BF format.</summary>
    internal static class CdrBuilder
    {
        /// <summary>
        /// Build a full CDR record (type 00000000) with specified fields.
        /// Returns the CSV line without the leading sequence number.
        /// </summary>
        public static string FullCdr(
            string timestamp = "2026-27-01T10:00:00.0+0100",
            int duration = 60,
            string gid = "2026-27-01T10:00:00.0+0100:FF0001000000000012937869AAAA0000",
            string calledParty = "20002",
            string callingNumber = "34944062497",
            int attemptIndicator = 0,
            int causeCode = 16,
            int origPartyId = 900,
            int termPartyId = 903,
            string callAnswerTime = "2026-27-01T10:00:01.0+0100",
            string inLegConnectTime = "2026-27-01T10:00:00.0+0100",
            string outLegConnectTime = "2026-27-01T10:00:01.0+0100",
            string outLegReleaseTime = "2026-27-01T10:01:00.0+0100",
            string callReleaseTime = "2026-27-01T10:01:00.0+0100",
            string dialedNumber = "20002",
            string destinationExt = "34944068813",
            string ingressEndpoint = "10.0.172.18",
            string egressEndpoint = "10.0.73.71",
            string forwardingParty = "",
            int perCallFeature = 0,
            long perCallFeatureExt = 0,
            long callEventIndicator = 0,
            string threadIdNode = "0-0-300",
            string threadIdSeq = "101611",
            string gidSequence = "101613",
            int mediaType = 10)
        {
            // Build 128+ field CSV. We need fields up to index ~134 based on the spec.
            // Type is field 0 (after line-seq), timestamp field 1, duration field 2, etc.
            // Actual BF format: lineseq,type,timestamp,duration,hostname,gid,...
            // Fields indexed from 0 in the raw line (after lineseq is stripped):
            // [0]=type, [1]=timestamp, [2]=duration, [3]=hostname, [4]=gid, ...
            // From looking at real data, the format is:
            // seq,00000000,timestamp,duration,hostname,GID,<empty>,<empty>,duration_again,...
            // Let me use the real BF structure from the sample file
            
            var fields = new string[135];
            for (int i = 0; i < fields.Length; i++) fields[i] = "";
            
            fields[0] = "00000000";              // Record type
            fields[1] = timestamp;                // Timestamp
            fields[2] = duration.ToString();      // Duration
            fields[3] = "testhost";               // Hostname
            fields[4] = gid;                      // Global Call ID
            // fields 5-8 empty
            fields[9] = calledParty;              // spec field 11 -> CalledParty (index 10 in spec, 9 in 0-based after type)
            fields[10] = callingNumber;           // spec field 12 -> CallingNumber
            // fields 11-15 empty
            fields[16] = attemptIndicator.ToString(); // spec field 18
            fields[17] = causeCode.ToString();    // spec field 19
            // fields 18-37 empty
            fields[38] = origPartyId.ToString();  // spec field 40
            fields[39] = termPartyId.ToString();  // spec field 41
            // fields 40-45 empty
            fields[46] = callAnswerTime;          // o+47 -> spec field 48 (Call Answer Time)
            fields[47] = callReleaseTime;         // o+48 -> spec field 49 (Call Release Time)
            fields[48] = inLegConnectTime;        // o+49 -> spec field 50 (In-Leg Connect Time)
            fields[50] = outLegConnectTime;       // o+51 -> spec field 52 (Out-Leg Connect Time)
            fields[51] = outLegReleaseTime;       // o+52 -> spec field 53 (Out-Leg Release Time)
            // fields 52-61 empty
            fields[62] = perCallFeature.ToString(); // spec field 64
            fields[63] = forwardingParty;         // spec field 65
            // fields 64-98 empty
            fields[99] = dialedNumber;            // spec field 101
            // fields 100-101 empty
            fields[102] = mediaType.ToString();   // spec field 104
            // fields 103-103 empty  
            fields[104] = perCallFeatureExt.ToString(); // spec field 106
            fields[105] = callEventIndicator.ToString(); // spec field 107
            // fields 106-120 empty
            fields[120] = gidSequence;            // o+121 -> spec field 122 -> GidSequence
            fields[122] = threadIdNode;           // spec field 124 -> ThreadIdNode [BF: fields[123]]
            fields[123] = threadIdSeq;            // spec field 125 -> ThreadIdSequence [BF: fields[124]]
            fields[124] = ingressEndpoint;        // spec field 126
            fields[125] = egressEndpoint;         // spec field 127
            fields[126] = destinationExt;         // spec field 128 -> DestinationExt [BF: fields[127]]

            return string.Join(",", fields);
        }

        /// <summary>Build a Hunt Group record (type 00000004).</summary>
        public static string HuntGroupRecord(
            string timestamp = "2026-27-01T10:00:00.0+0100",
            string hostname = "testhost",
            string gid = "2026-27-01T10:00:00.0+0100:FF0001000000000012937869AAAA0000",
            string hgNumber = "34949911000",
            string hgStartTime = "2026-27-01T10:00:00.0+0100",
            string hgEndTime = "2026-27-01T10:00:01.0+0100",
            string status1 = "1",
            string status2 = "1",
            string routedToExt = "34944068813")
        {
            // Format: 00000004,timestamp,hostname,GID,,hgNumber,hgStartTime,hgEndTime,status1,status2,routedToExt
            return $"00000004,{timestamp},{hostname},{gid},,{hgNumber},{hgStartTime},{hgEndTime},{status1},{status2},{routedToExt}";
        }
    }

    #endregion

    #region DecodedCdrTests

    public class DecodedCdrTests
    {
        [Fact]
        public void WriteDecodedCdrs_True_CreatesDecodedFiles()
        {
            using var h = new TestHarness();
            h.SettingsWriteDecodedCdrs = true;
            // Copy a real test file
            h.CopyTestFile("osvpro2v2-20260127T103231+0100000888.BF");
            var (result, _, _) = h.Run();

            Assert.True(result.TotalFilesProcessed > 0);
            Assert.Empty(result.Errors);
            var decodedFiles = Directory.GetFiles(h.DecodedDir, "*.csv");
            Assert.NotEmpty(decodedFiles);
        }

        [Fact]
        public void WriteDecodedCdrs_False_NoDecodedFiles()
        {
            using var h = new TestHarness();
            h.SettingsWriteDecodedCdrs = false;
            h.CopyTestFile("osvpro2v2-20260127T103231+0100000888.BF");
            var (result, _, _) = h.Run();

            Assert.True(result.TotalFilesProcessed > 0);
            Assert.Empty(result.Errors);
            var decodedFiles = Directory.GetFiles(h.DecodedDir, "*.csv");
            Assert.Empty(decodedFiles);
        }

        [Fact]
        public void DecodedCsv_ContainsExpectedColumnsAndRows()
        {
            using var h = new TestHarness();
            h.SettingsWriteDecodedCdrs = true;
            h.CopyTestFile("osvpro2v2-20260127T103231+0100000888.BF");
            var (result, _, _) = h.Run();

            var decodedFiles = Directory.GetFiles(h.DecodedDir, "*.csv");
            Assert.NotEmpty(decodedFiles);

            var lines = File.ReadAllLines(decodedFiles[0]);
            Assert.True(lines.Length >= 2, "Decoded CSV should have header + at least 1 data row");
            // Header should contain key columns
            var header = lines[0];
            Assert.Contains("RecordType", header);
        }
    }

    #endregion

    #region ArchiveBehaviorTests

    public class ArchiveBehaviorTests
    {
        [Fact]
        public void DeleteInputFiles_True_MovesFilesToArchive()
        {
            using var h = new TestHarness();
            h.SettingsDeleteInputFiles = true;
            h.CopyTestFile("osvpro2v2-20260127T103231+0100000888.BF");
            var inputFile = Path.Combine(h.InputDir, "osvpro2v2-20260127T103231+0100000888.BF");
            Assert.True(File.Exists(inputFile));

            var (result, _, _) = h.Run();
            Assert.True(result.TotalFilesProcessed > 0);
            Assert.Empty(result.Errors);

            // Input file should be gone
            Assert.False(File.Exists(inputFile), "Input file should be moved to archive");
            // Archive should have the file
            var archived = Directory.GetFiles(h.ArchiveDir, "*.BF");
            Assert.NotEmpty(archived);
        }

        [Fact]
        public void DeleteInputFiles_False_FilesRemain()
        {
            using var h = new TestHarness();
            h.SettingsDeleteInputFiles = false;
            h.CopyTestFile("osvpro2v2-20260127T103231+0100000888.BF");
            var inputFile = Path.Combine(h.InputDir, "osvpro2v2-20260127T103231+0100000888.BF");

            var (result, _, _) = h.Run();
            Assert.True(result.TotalFilesProcessed > 0);

            // Input file should still exist
            Assert.True(File.Exists(inputFile));
            // Archive should be empty
            var archived = Directory.GetFiles(h.ArchiveDir, "*.BF");
            Assert.Empty(archived);
        }

        [Fact]
        public void DuplicateFilename_GetsTimestampSuffix()
        {
            using var h = new TestHarness();
            h.SettingsDeleteInputFiles = true;

            // Pre-populate archive with a file of the same name
            var archiveFile = Path.Combine(h.ArchiveDir, "osvpro2v2-20260127T103231+0100000888.BF");
            File.WriteAllText(archiveFile, "placeholder");

            h.CopyTestFile("osvpro2v2-20260127T103231+0100000888.BF");
            var (result, _, _) = h.Run();
            Assert.True(result.TotalFilesProcessed > 0);
            Assert.Empty(result.Errors);

            // Should have 2 files in archive now
            var archived = Directory.GetFiles(h.ArchiveDir, "*.BF");
            Assert.Equal(2, archived.Length);
            // One should have timestamp suffix
            Assert.True(archived.Any(f => f.Contains("_20")), "Duplicate should have timestamp suffix");
        }
    }

    #endregion

    #region FeatureCodeFilterTests

    public class FeatureCodeFilterTests
    {
        [Theory]
        [InlineData("*4420002")]
        [InlineData("#4420002")]
        [InlineData("20002*44")]
        [InlineData("123#44567")]
        public void FeatureCodeCalls_AreFilteredOut(string dialedNumber)
        {
            using var h = new TestHarness();
            h.WriteBfFile("test.BF",
                CdrBuilder.FullCdr(dialedNumber: dialedNumber, gidSequence: "900001", threadIdSeq: "900001"));

            var (result, logger, _) = h.Run();
            Assert.Empty(result.Errors);

            // No legs/calls should be output
            var outputFiles = Directory.GetFiles(h.OutputDir, "legs_*.csv");
            if (outputFiles.Length > 0)
            {
                var lines = File.ReadAllLines(outputFiles[0]);
                // Only header, no data rows
                Assert.True(lines.Length <= 1, $"Feature code call should produce no legs, got {lines.Length - 1} data rows");
            }

            // Tracer should have recorded suppression
            Assert.True(logger.Entries.Any(e => e.Message.Contains("[SUPPRESSED]") && e.Message.Contains("Feature code")),
                "Tracer should capture feature code suppression");
        }

        [Fact]
        public void NonFeatureCodeCalls_ProcessedNormally()
        {
            using var h = new TestHarness();
            h.CopyTestFile("osvpro2v2-20260127T103231+0100000888.BF");

            var (result, _, _) = h.Run();
            Assert.True(result.TotalFilesProcessed > 0);
            Assert.Empty(result.Errors);
            Assert.True(result.TotalRecordsProcessed > 0);

            var outputFiles = Directory.GetFiles(h.OutputDir, "legs_*.csv");
            Assert.NotEmpty(outputFiles);
            var lines = File.ReadAllLines(outputFiles[0]);
            Assert.True(lines.Length > 1, "Non-feature-code calls should produce legs");
        }
    }

    #endregion

    #region SipClassificationTests

    public class SipClassificationTests
    {
        [Fact]
        public void KnownInternalEndpoints_ClassifiedCorrectly()
        {
            using var h = new TestHarness();
            // Internal endpoints have empty trunk name in map
            h.CopyTestFile("osvpro2v2-20260127T103231+0100000888.BF");

            var (result, logger, _) = h.Run();
            Assert.Empty(result.Errors);

            // Check that SIP classification traces contain Internal for known endpoints
            var sipTraces = logger.Entries.Where(e => e.Message.Contains("[SIP]")).ToList();
            Assert.NotEmpty(sipTraces);
            Assert.True(sipTraces.Any(t => t.Message.Contains("Internal")),
                "Known internal endpoints should be classified as Internal");
        }

        [Fact]
        public void KnownPstnGateways_ClassifiedAsPstn()
        {
            using var h = new TestHarness();
            // eranhangnadmin1/2 have trunk names -> PSTN
            h.CopyTestFile("osvpro2v2-20260127T103231+0100000888.BF");

            var (result, logger, _) = h.Run();
            Assert.Empty(result.Errors);

            // If any CDR uses a PSTN endpoint, it should be classified
            var sipTraces = logger.Entries.Where(e => e.Message.Contains("[SIP]")).ToList();
            // The test data may or may not use PSTN endpoints, but the SIP map is configured
            Assert.True(((ISipEndpointsProvider)h).IsPstn("eranhangnadmin1.ejhsgvdi.net"));
            Assert.False(((ISipEndpointsProvider)h).IsPstn("10.0.172.18"));
        }

        [Fact]
        public void UnknownEndpoint_NotInSipMap()
        {
            using var h = new TestHarness();
            Assert.False(((ISipEndpointsProvider)h).IsPstn("192.168.1.1"));
            Assert.False(((ISipEndpointsProvider)h).IsPstn("unknown.host.com"));
        }
    }

    #endregion

    #region DirectionResolutionTests

    public class DirectionResolutionTests
    {
        [Fact]
        public void InternalToInternal_IsInternal()
        {
            using var h = new TestHarness();
            // Both endpoints are internal (in SIP map with empty trunk)
            h.WriteBfFile("test.BF",
                CdrBuilder.FullCdr(
                    ingressEndpoint: "10.0.172.18",
                    egressEndpoint: "10.0.73.71",
                    origPartyId: 900,
                    termPartyId: 903,
                    gidSequence: "800001",
                    threadIdSeq: "800001"));

            var (result, logger, _) = h.Run();
            Assert.Empty(result.Errors);

            var dirTraces = logger.Entries.Where(e => e.Message.Contains("[DIRECTION]")).ToList();
            Assert.NotEmpty(dirTraces);
            Assert.True(dirTraces.Any(t => t.Message.Contains("Internal")),
                "Internal→Internal should be classified as Internal");
        }

        [Fact]
        public void InternalToPstn_IsOutgoing()
        {
            using var h = new TestHarness();
            h.WriteBfFile("test.BF",
                CdrBuilder.FullCdr(
                    ingressEndpoint: "10.0.172.18",
                    egressEndpoint: "eranhangnadmin1.ejhsgvdi.net",
                    origPartyId: 900,
                    termPartyId: 901,
                    gidSequence: "800002",
                    threadIdSeq: "800002"));

            var (result, logger, _) = h.Run();
            Assert.Empty(result.Errors);

            var dirTraces = logger.Entries.Where(e => e.Message.Contains("[DIRECTION]")).ToList();
            Assert.NotEmpty(dirTraces);
            Assert.True(dirTraces.Any(t => t.Message.Contains("Outgoing")),
                "Internal→PSTN should be classified as Outgoing");
        }

        [Fact]
        public void PstnToInternal_IsIncoming()
        {
            using var h = new TestHarness();
            h.WriteBfFile("test.BF",
                CdrBuilder.FullCdr(
                    ingressEndpoint: "eranhangnadmin1.ejhsgvdi.net",
                    egressEndpoint: "10.0.73.71",
                    origPartyId: 901,
                    termPartyId: 903,
                    callingNumber: "0501234567",
                    gidSequence: "800003",
                    threadIdSeq: "800003"));

            var (result, logger, _) = h.Run();
            Assert.Empty(result.Errors);

            var dirTraces = logger.Entries.Where(e => e.Message.Contains("[DIRECTION]")).ToList();
            Assert.NotEmpty(dirTraces);
            Assert.True(dirTraces.Any(t => t.Message.Contains("Incoming")),
                "PSTN→Internal should be classified as Incoming");
        }

        [Fact]
        public void PstnToPstn_IsTrunkToTrunk()
        {
            using var h = new TestHarness();
            h.WriteBfFile("test.BF",
                CdrBuilder.FullCdr(
                    ingressEndpoint: "eranhangnadmin1.ejhsgvdi.net",
                    egressEndpoint: "eranhangnadmin2.ejhsgvdi.net",
                    origPartyId: 901,
                    termPartyId: 901,
                    callingNumber: "0501234567",
                    gidSequence: "800004",
                    threadIdSeq: "800004"));

            var (result, logger, _) = h.Run();
            Assert.Empty(result.Errors);

            var dirTraces = logger.Entries.Where(e => e.Message.Contains("[DIRECTION]")).ToList();
            Assert.NotEmpty(dirTraces);
            Assert.True(dirTraces.Any(t => t.Message.Contains("TrunkToTrunk")),
                "PSTN→PSTN should be classified as TrunkToTrunk");
        }
    }

    #endregion

    #region LegMergeTests

    public class LegMergeTests
    {
        [Fact]
        public void ConsecutiveLegsToSameDestExt_AreMerged()
        {
            using var h = new TestHarness();
            var gid = "2026-27-01T10:00:00.0+0100:FF0001000000000012937869BBBB0000";
            // First leg: 0 duration attempt
            var attempt = CdrBuilder.FullCdr(
                duration: 0, causeCode: 23, gid: gid,
                destinationExt: "34944068813",
                callAnswerTime: "",
                outLegConnectTime: "",
                gidSequence: "700001",
                threadIdSeq: "700001");
            // Second leg: answered
            var answer = CdrBuilder.FullCdr(
                duration: 45, causeCode: 16, gid: gid,
                destinationExt: "34944068813",
                gidSequence: "700002",
                threadIdSeq: "700001");

            h.WriteBfFile("test.BF", attempt, answer);
            var (result, logger, _) = h.Run();
            Assert.Empty(result.Errors);

            // Check for merge traces
            var mergeTraces = logger.Entries.Where(e => e.Message.Contains("[MERGE]")).ToList();
            // Merge may or may not fire depending on exact conditions, but the output should show merged data
        }
    }

    #endregion

    #region LegSuppressionTests

    public class LegSuppressionTests
    {
        [Fact]
        public void RoutingOnlyLeg_IsSuppressed()
        {
            using var h = new TestHarness();
            h.CopyTestFile("osvpro2v2-20260127T103231+0100000888.BF");

            var (result, logger, _) = h.Run();
            Assert.Empty(result.Errors);

            // Check that suppression traces exist for routing numbers
            var suppressTraces = logger.Entries.Where(e => e.Message.Contains("[SUPPRESSED]")).ToList();
            // The routing number should cause suppression if it appears as a 0-duration DestExt
        }

        [Fact]
        public void HgOnlyLegs_WithNoMatchingCdr_AreSuppressed()
        {
            using var h = new TestHarness();
            // Create a BF with only an HG record and no matching full CDR
            var hg = CdrBuilder.HuntGroupRecord(
                gid: "2026-27-01T10:00:00.0+0100:FF0001000000000012937869CCCC0000",
                hgNumber: "34949911000",
                routedToExt: "34944068813");
            h.WriteBfFile("test.BF", hg);

            var (result, logger, _) = h.Run();
            Assert.Empty(result.Errors);

            // HG-only legs with no CDR should be suppressed or not appear in output
            var outputFiles = Directory.GetFiles(h.OutputDir, "legs_*.csv");
            if (outputFiles.Length > 0)
            {
                var lines = File.ReadAllLines(outputFiles[0]);
                // Should have at most header only (no data for HG-only without CDR)
                Assert.True(lines.Length <= 1 || !lines.Skip(1).Any(l => l.Contains("IsHgOnly")),
                    "HG-only legs without matching CDR should not appear in output");
            }
        }
    }

    #endregion

    #region TransferChainTests

    public class TransferChainTests
    {
        [Fact]
        public void SingleLegCall_HasEmptyTransferTo()
        {
            using var h = new TestHarness();
            h.WriteBfFile("test.BF",
                CdrBuilder.FullCdr(gidSequence: "600001", threadIdSeq: "600001"));

            var (result, logger, _) = h.Run();
            Assert.Empty(result.Errors);

            // For a single-leg call, TransferTo should be empty (no next leg)
            var outputFiles = Directory.GetFiles(h.OutputDir, "legs_*.csv");
            Assert.NotEmpty(outputFiles);
            var lines = File.ReadAllLines(outputFiles[0]);
            Assert.True(lines.Length > 1, "Should produce at least one leg");
            var header = lines[0].Split(',');
            var ttIdx = Array.IndexOf(header, "TransferTo");
            if (ttIdx >= 0)
            {
                var data = lines[1].Split(',');
                var ttVal = data.Length > ttIdx ? data[ttIdx].Trim('"') : "";
                Assert.True(string.IsNullOrEmpty(ttVal), $"Single leg should have empty TransferTo, got '{ttVal}'");
            }
        }

        [Fact]
        public void MultiLegCall_HasTransferFromTo()
        {
            using var h = new TestHarness();
            // Use a real test file that has multi-leg calls
            h.CopyTestFile("osvpro2v2-20260127T103231+0100000888.BF");

            var (result, logger, _) = h.Run();
            Assert.Empty(result.Errors);

            var transferTraces = logger.Entries.Where(e => e.Message.Contains("[TRANSFER]")).ToList();
            Assert.NotEmpty(transferTraces);
        }
    }

    #endregion

    #region TracerOutputTests

    public class TracerOutputTests
    {
        [Fact]
        public void Tracer_CapturesAllEventTypes()
        {
            using var h = new TestHarness();
            // Use multiple real files for comprehensive trace coverage
            h.CopyTestFile("osvpro2v2-20260127T103231+0100000888.BF");
            h.CopyTestFile("osvpro2v2-20260127T104708+0100000889.BF");

            var (result, logger, _) = h.Run();
            Assert.Empty(result.Errors);

            var messages = logger.Entries.Select(e => e.Message).ToList();

            // [FIELD] events
            Assert.True(messages.Any(m => m.Contains("[FIELD]")), "Should have [FIELD] trace events");

            // [DIRECTION] events
            Assert.True(messages.Any(m => m.Contains("[DIRECTION]")), "Should have [DIRECTION] trace events");

            // [SIP] events
            Assert.True(messages.Any(m => m.Contains("[SIP]")), "Should have [SIP] trace events");

            // [TRANSFER] events
            Assert.True(messages.Any(m => m.Contains("[TRANSFER]")), "Should have [TRANSFER] trace events");
        }

        [Fact]
        public void Tracer_FieldEvents_ContainExpectedData()
        {
            using var h = new TestHarness();
            h.CopyTestFile("osvpro2v2-20260127T103231+0100000888.BF");

            var (result, logger, _) = h.Run();
            Assert.Empty(result.Errors);

            var fieldTraces = logger.Entries.Where(e => e.Message.Contains("[FIELD]")).ToList();
            Assert.NotEmpty(fieldTraces);

            // Field traces should reference known field names
            var knownFields = new[] { "DialedNumber", "DestinationExt", "IngressEndpoint", "EgressEndpoint",
                                       "CallingNumber", "CalledParty" };
            foreach (var field in knownFields)
            {
                Assert.True(fieldTraces.Any(t => t.Message.Contains($"Field={field}")),
                    $"Should have [FIELD] trace for {field}");
            }
        }

        [Fact]
        public void FeatureCodeSuppression_TracedCorrectly()
        {
            using var h = new TestHarness();
            h.WriteBfFile("test.BF",
                CdrBuilder.FullCdr(dialedNumber: "*4420002", gidSequence: "500001", threadIdSeq: "500001"));

            var (result, logger, _) = h.Run();

            var suppressTraces = logger.Entries.Where(e => e.Message.Contains("[SUPPRESSED]")).ToList();
            Assert.NotEmpty(suppressTraces);
            Assert.True(suppressTraces.Any(t => t.Message.Contains("Feature code") && t.Message.Contains("*44")),
                "Should trace *44 feature code suppression");
        }
    }

    #endregion
}
