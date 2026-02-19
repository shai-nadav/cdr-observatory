using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;
using Pipeline.Components.OSVParser;
using Pipeline.Components.OSVParser.Cache;
using Pipeline.Components.OSVParser.Processing;

namespace CdrObservatory.Tests
{
    public class ScenarioTests : ISettingsProvider, ISipEndpointsProvider, IPendingCallsRepository
    {
        private string _rootTestPath;
        private string _rootTestOutputPath;
        private long _currentRecordSeq = 1;

        // ── ISettingsProvider ──
        string ISettingsProvider.InputFolder => Path.Combine(_rootTestPath, "CDRs");
        string ISettingsProvider.OutputFolder => Path.Combine(_rootTestOutputPath, "OutputFolder");
        string ISettingsProvider.ArchiveFolder => Path.Combine(_rootTestOutputPath, "ArchiveFolder");
        string ISettingsProvider.WorkFolder => Path.Combine(_rootTestOutputPath, "WorkFolder");
        string ISettingsProvider.DecodedFolder => Path.Combine(_rootTestOutputPath, "DecodedFolder");
        string ISettingsProvider.OrphanFolder => Path.Combine(_rootTestOutputPath, "OrphanFolder");
        string ISettingsProvider.SipEndpointsFile => null;
        string ISettingsProvider.VoicemailNumber => "";
        IList<string> ISettingsProvider.RoutingNumbers => new List<string>();
        IList<string> ISettingsProvider.HuntGroupNumbers => new List<string>();
        string ISettingsProvider.InstanceId => "1";
        string ISettingsProvider.FilePattern => "*.*";
        int ISettingsProvider.IncompleteRetentionHours => 24;
        int ISettingsProvider.MaxPendingQueueSize => 10000;
        bool ISettingsProvider.WriteDecodedCdrs => true;
        bool ISettingsProvider.DeleteInputFiles => false;

        // ── ISipEndpointsProvider ──
        bool ISipEndpointsProvider.IsLoaded => true;
        int ISipEndpointsProvider.PstnCount => 2;

        private readonly IDictionary<string, string> _sipMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
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

        IDictionary<string, string> ISipEndpointsProvider.LoadAddressToTrunkMap() => _sipMap;

        bool ISipEndpointsProvider.IsPstn(string endpoint)
        {
            if (string.IsNullOrWhiteSpace(endpoint)) return false;
            string mappedValue;
            if (_sipMap.TryGetValue(endpoint, out mappedValue))
                return !string.IsNullOrEmpty(mappedValue);
            return false;
        }

        // ── IPendingCallsRepository ──
        IList<PendingCallRecord> IPendingCallsRepository.Load(int instanceId)
        {
            throw new NotImplementedException();
        }

        void IPendingCallsRepository.Sync(int instanceId, IList<string> deletedIds, IList<PendingCallRecord> upsertCalls) { }

        long IPendingCallsRepository.GetCurrentRecordSeq() => _currentRecordSeq;

        long IPendingCallsRepository.IncrementRecordSeq() => ++_currentRecordSeq;

        // ── Test ──
        [Fact]
        public void TestAllScenarios()
        {
            // Locate test data relative to test assembly
            var assemblyDir = Path.GetDirectoryName(typeof(ScenarioTests).Assembly.Location);
            _rootTestPath = Path.Combine(assemblyDir, "TestData");
            _rootTestOutputPath = Path.Combine(_rootTestPath, "Output");

            Assert.True(Directory.Exists(Path.Combine(_rootTestPath, "CDRs")),
                $"Test CDRs folder not found at {Path.Combine(_rootTestPath, "CDRs")}");

            // Clean and create output dirs
            if (Directory.Exists(_rootTestOutputPath))
                Directory.Delete(_rootTestOutputPath, true);
            Directory.CreateDirectory(Path.Combine(_rootTestOutputPath, "OutputFolder"));
            Directory.CreateDirectory(Path.Combine(_rootTestOutputPath, "ArchiveFolder"));
            Directory.CreateDirectory(Path.Combine(_rootTestOutputPath, "DecodedFolder"));
            Directory.CreateDirectory(Path.Combine(_rootTestOutputPath, "WorkFolder"));
            Directory.CreateDirectory(Path.Combine(_rootTestOutputPath, "OrphanFolder"));

            // Run engine
            var logger = new LogCollector();
            var tracer = new ProcessingTracer(logger);
            var cache = new InMemoryCacheStore();
            var engine = new CdrProcessorEngine(this, logger, this, this, cache, tracer);
            var inputDir = Path.Combine(_rootTestPath, "CDRs");
            var bfFiles = Directory.GetFiles(inputDir, "*.bf");
            var result = engine.ProcessFolder();

            Assert.True(result.TotalFilesProcessed > 0,
                $"No files were processed. InputDir={inputDir}, Exists={Directory.Exists(inputDir)}, " +
                $"BF files found={bfFiles.Length}, Errors={string.Join("; ", result.Errors)}, " +
                $"Log entries={logger.Entries.Count}");
            Assert.Empty(result.Errors);

            // Compare output legs with expected
            var expectedDir = Path.Combine(_rootTestPath, "ExpectedResults");
            var expectedFiles = Directory.GetFiles(expectedDir, "legs_*.csv");
            Assert.Single(expectedFiles);

            var actualDir = Path.Combine(_rootTestOutputPath, "OutputFolder");
            Assert.True(Directory.Exists(actualDir), $"Output folder not found: {actualDir}");
            var actualFiles = Directory.GetFiles(actualDir, "legs_*.csv");
            Assert.Single(actualFiles);

            var mismatches = CompareCsvFiles(expectedFiles[0], actualFiles[0]);

            if (mismatches.Count > 0)
            {
                var sb = new StringBuilder();
                sb.AppendLine($"Legs CSV mismatch ({mismatches.Count} differences):");
                sb.AppendLine($"Expected: {expectedFiles[0]}");
                sb.AppendLine($"Actual:   {actualFiles[0]}");
                foreach (var m in mismatches.Take(50))
                    sb.AppendLine($"  - {m}");
                if (mismatches.Count > 50)
                    sb.AppendLine($"  ... and {mismatches.Count - 50} more");
                Assert.Fail(sb.ToString());
            }

            // Cleanup on success
            if (Directory.Exists(_rootTestOutputPath))
                Directory.Delete(_rootTestOutputPath, true);
        }

        // ── CSV Comparison (ported from work test) ──
        private static List<string> CompareCsvFiles(string expectedPath, string actualPath)
        {
            var mismatches = new List<string>();
            var expectedRows = ReadCsv(expectedPath);
            var actualRows = ReadCsv(actualPath);

            if (expectedRows.Count == 0 && actualRows.Count > 0)
            { mismatches.Add($"Expected CSV is empty, actual has {actualRows.Count} rows."); return mismatches; }
            if (expectedRows.Count > 0 && actualRows.Count == 0)
            { mismatches.Add($"Actual CSV is empty, expected has {expectedRows.Count} rows."); return mismatches; }
            if (expectedRows.Count == 0) return mismatches;

            // Header check
            int expectedCols = expectedRows[0].Count;
            int actualCols = actualRows[0].Count;
            if (expectedCols != actualCols)
                mismatches.Add($"Header column count mismatch. Expected={expectedCols}, Actual={actualCols}");

            for (int col = 0; col < Math.Min(expectedCols, actualCols); col++)
            {
                if (!string.Equals(expectedRows[0][col], actualRows[0][col], StringComparison.Ordinal))
                    mismatches.Add($"Header mismatch at col {col}. Expected='{expectedRows[0][col]}', Actual='{actualRows[0][col]}'");
            }

            int expectedGidIdx = GetColumnIndex(expectedRows[0], "GidSequence");
            int actualGidIdx = GetColumnIndex(actualRows[0], "GidSequence");
            int expectedLegIdx = GetColumnIndex(expectedRows[0], "LegIndex");
            int actualLegIdx = GetColumnIndex(actualRows[0], "LegIndex");

            if (expectedGidIdx < 0 || actualGidIdx < 0)
            { mismatches.Add("Missing 'GidSequence' column."); return mismatches; }
            if (expectedLegIdx < 0 || actualLegIdx < 0)
            { mismatches.Add("Missing 'LegIndex' column."); return mismatches; }

            var expectedByKey = BuildRowsByKey(expectedRows, expectedGidIdx, expectedLegIdx, "expected", mismatches);
            var actualByKey = BuildRowsByKey(actualRows, actualGidIdx, actualLegIdx, "actual", mismatches);

            if (expectedByKey.Count != actualByKey.Count)
                mismatches.Add($"Row count mismatch. Expected={expectedByKey.Count}, Actual={actualByKey.Count}");

            foreach (var key in expectedByKey.Keys.Except(actualByKey.Keys).OrderBy(k => k))
                mismatches.Add($"Missing actual row: {key}");
            foreach (var key in actualByKey.Keys.Except(expectedByKey.Keys).OrderBy(k => k))
                mismatches.Add($"Unexpected actual row: {key}");

            foreach (var key in expectedByKey.Keys.Intersect(actualByKey.Keys).OrderBy(k => k))
            {
                var er = expectedByKey[key];
                var ar = actualByKey[key];
                for (int col = 0; col < Math.Min(er.Count, ar.Count); col++)
                {
                    if (!string.Equals(er[col], ar[col], StringComparison.Ordinal))
                    {
                        string header = col < expectedRows[0].Count ? expectedRows[0][col] : $"Col{col}";
                        mismatches.Add($"Row '{key}', '{header}': Expected='{er[col]}', Actual='{ar[col]}'");
                    }
                }
            }

            return mismatches;
        }

        private static Dictionary<string, List<string>> BuildRowsByKey(
            List<List<string>> rows, int gidIdx, int legIdx, string source, List<string> mismatches)
        {
            var dataRows = rows.Skip(1).ToList();
            var gidCounts = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (var row in dataRows)
            {
                string gid = GetVal(row, gidIdx);
                gidCounts[gid] = gidCounts.ContainsKey(gid) ? gidCounts[gid] + 1 : 1;
            }

            var byKey = new Dictionary<string, List<string>>(StringComparer.Ordinal);
            foreach (var row in dataRows)
            {
                string gid = GetVal(row, gidIdx);
                bool needsLeg = gidCounts.ContainsKey(gid) && gidCounts[gid] > 1;
                string leg = GetVal(row, legIdx);
                string key = needsLeg ? $"{gid}|{leg}" : gid;

                if (byKey.ContainsKey(key))
                { mismatches.Add($"Duplicate key in {source}: {key}"); continue; }
                byKey[key] = row;
            }
            return byKey;
        }

        private static int GetColumnIndex(List<string> header, string name)
        {
            for (int i = 0; i < header.Count; i++)
                if (string.Equals(header[i], name, StringComparison.OrdinalIgnoreCase)) return i;
            return -1;
        }

        private static string GetVal(List<string> row, int idx) =>
            idx >= 0 && idx < row.Count ? (row[idx] ?? "") : "";

        private static List<List<string>> ReadCsv(string path)
        {
            return File.ReadAllLines(path).Select(ParseCsvLine).ToList();
        }

        private static List<string> ParseCsvLine(string line)
        {
            var values = new List<string>();
            var current = new StringBuilder();
            bool inQuotes = false;
            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];
                if (c == '"')
                {
                    if (inQuotes && i + 1 < line.Length && line[i + 1] == '"') { current.Append('"'); i++; }
                    else inQuotes = !inQuotes;
                }
                else if (c == ',' && !inQuotes) { values.Add(current.ToString()); current.Clear(); }
                else current.Append(c);
            }
            values.Add(current.ToString());
            return values;
        }
    }
}
