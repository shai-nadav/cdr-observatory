using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser.Processing
{
    /// <summary>
    /// Streaming writer for decoded CDR records. Write one record at a time to avoid memory buildup.
    /// </summary>
    public class DecodedCdrWriter : IDisposable
    {
        private readonly StreamWriter _writer;
        private static readonly Encoding Utf8Bom = new UTF8Encoding(encoderShouldEmitUTF8Identifier: true);

        public DecodedCdrWriter(string path, string[] headers)
        {
            _writer = new StreamWriter(path, false, Utf8Bom);
            _writer.WriteLine(string.Join(",", headers));
        }

        public void WriteRecord(RawCdrRecord r)
        {
            var threadId = r.ThreadIdSequence ?? r.ThreadIdNode ?? "";

            _writer.WriteLine(string.Join(",", new[]
            {
                // Identification
                Escape(r.RecordType.ToString()),
                Escape(r.GlobalCallId),
                        Escape(r.ThreadIdNode),
                        Escape(r.ThreadIdSequence),
                        Escape(threadId),
                        Escape(r.GidSequence),
                // Parties
                Escape(r.CallingNumber),
                Escape(r.CalledParty),
                Escape(r.DialedNumber),
                Escape(r.DestinationExt),
                Escape(r.ForwardingParty),
                // Direction (with decoded text)
                r.OrigPartyId.ToString(),
                Escape(FieldMappings.GetPartyIdText(r.OrigPartyId)),
                r.TermPartyId.ToString(),
                Escape(FieldMappings.GetPartyIdText(r.TermPartyId)),
                // Timing
                Escape(r.Timestamp),
                        r.Duration.ToString(),
                Escape(r.InLegConnectTime),
                Escape(r.CallAnswerTime),
                Escape(r.CallReleaseTime),
                Escape(r.OutLegConnectTime),
                Escape(r.OutLegReleaseTime),
                // Result codes (with decoded text)
                r.AttemptIndicator.ToString(),
                Escape(FieldMappings.GetAttemptIndicatorText(r.AttemptIndicator)),
                r.CauseCode.ToString(),
                Escape(FieldMappings.GetReleaseCauseText(r.CauseCode)),
                // Feature flags (decoded to flag list)
                r.PerCallFeature.ToString(),
                Escape(FieldMappings.GetPerCallFeatureText(r.PerCallFeature)),
                r.PerCallFeatureExt.ToString(),
                Escape(FieldMappings.GetPerCallFeatureExtText(r.PerCallFeatureExt)),
                r.CallEventIndicator.ToString(),
                Escape(FieldMappings.GetCallEventIndicatorText(r.CallEventIndicator)),
                        r.MediaType.ToString(),
                // Endpoints
                Escape(r.IngressEndpoint),
                Escape(r.EgressEndpoint),
                // Traceability
                Escape(r.SourceFile != null ? Path.GetFileName(r.SourceFile) : null),
                r.SourceLine.ToString(),
            }));
        }

        private static string Escape(string value)
        {
            if (value == null) return "";
            if (value.Contains(",") || value.Contains("\"") || value.Contains("\n") || value.Contains("\r"))
            {
                return "\"" + value.Replace("\"", "\"\"") + "\"";
            }
            return value;
        }

        public void Dispose()
        {
            _writer?.Dispose();
        }
    }

    /// <summary>
    /// Writes processing results to CSV files with headers.
    /// Production-grade: proper escaping, BOM for Excel, consistent encoding.
    /// </summary>
    public class CsvOutputWriter
    {
        private readonly IProcessorLogger _log;
        private static readonly Encoding Utf8Bom = new UTF8Encoding(encoderShouldEmitUTF8Identifier: true);

        public CsvOutputWriter(IProcessorLogger logger = null)
        {
            _log = logger ?? new NullProcessorLogger();
        }

        // Leg-level headers (flat per-leg output)
        private static readonly string[] LegHeaders = new[]
        {
            // 1. Timing
            "StartDate",
            "StartTime",
            "RingTime",
            "Duration",
            // 2. Golden fields
            "CallDirection",
            "Extension",
            "TransferFrom",
            "DestinationExt",
            "TransferTo",
            "HuntGroupNumber",
            "IsAnswered",
            "IsPickup",
            "IsForwarded",
            "IsVoicemail",
            // 3. Gateways
            "IngressEndpoint",
            "EgressEndpoint",
            // 3b. Call identification (moved for debugging)
            "GlobalCallId",
            "ThreadId",
                        // 4. Direction indicators
            "OrigPartyId",
            "OrigPartyIdText",
            "TermPartyId",
            "TermPartyIdText",
            // 5. OSV decoded flags
            "CauseCode",
            "CauseCodeText",
            "PerCallFeature",
            "PerCallFeatureText",
            "AttemptIndicator",
            "AttemptIndicatorText",
            "PerCallFeatureExt",
            "PerCallFeatureExtText",
            "CallEventIndicator",
            "CallEventIndicatorText",
            // 6. Party details
            "CallerExtension",
            "CallerExternal",
            "CalledExtension",
            "CalledExternal",
            "DialedAni",
            "OriginalDialedDigits",
            "CalledParty",
            "CallingNumber",
            "ForwardingParty",
            "ForwardFromExt",
            "ForwardToExt",
            // 7. Reference/tracing (at end)
            "LegIndex",
            "CallAnswerTime",
            "InLegConnectTime",
            "OutLegReleaseTime",
            "OutLegConnectTime",
            "CallReleaseTime",
            "IsHgOnly",
            "SourceFile",
            "SourceLine",
            "GidSequence"
        };

        // Raw CDR input headers - fields actually used by processor, human-friendly order
        private static readonly string[] RawUsedHeaders = new[]
{
    // Identification
    "RecordType (2)",
    "GlobalCallId (5)",
    "ThreadIdNode (124)",
    "ThreadIdSequence (125)",
    "ThreadId (derived)",
    "GidSequence (122)",
    // Parties
    "CallingNumber (12)",
    "CalledParty (11)",
    "DialedNumber (101)",
    "DestinationExt (128)",
    "ForwardingParty (65)",
    // Direction (with decoded text)
    "OrigPartyId (40)",
    "OrigPartyIdText (40-derived)",
    "TermPartyId (41)",
    "TermPartyIdText (41-derived)",
    // Timing
    "Timestamp (2)",
    "Duration (3)",
    "InLegConnectTime (50)",
    "CallAnswerTime (48)",
    "CallReleaseTime (49)",
    "OutLegConnectTime (52)",
    "OutLegReleaseTime (53)",
    // Result codes (with decoded text)
    "AttemptIndicator (18)",
    "AttemptIndicatorText (18-derived)",
    "CauseCode (19)",
    "CauseCodeText (19-derived)",
    // Feature flags (decoded to flag list)
    "PerCallFeature (64)",
    "PerCallFeatureText (64-derived)",
    "PerCallFeatureExt (106)",
    "PerCallFeatureExtText (106-derived)",
    "CallEventIndicator (107)",
    "CallEventIndicatorText (107-derived)",
    "MediaType (104)",
    // Endpoints
    "IngressEndpoint (126)",
    "EgressEndpoint (127)",
    // Traceability
    "SourceFile (derived)",
    "SourceLine (derived)"
};




        /// <summary>
        /// Legacy (compat): write full processing result to output folder.
        /// Kept for tests/older callers.
        /// Creates: legs_*.csv + summary_*.csv
        /// </summary>

        /// <summary>
        /// Legacy (compat): write full processing result to output folder.
        /// Kept for tests/older callers.
        /// Creates: legs_*.csv + summary_*.csv
        /// </summary>
        public void WriteResults(ProcessingResult result, string outputFolder)
        {
            if (!Directory.Exists(outputFolder))
                Directory.CreateDirectory(outputFolder);

            // If streaming already wrote legs, don't overwrite with empty results
            var existingLegs = Directory.GetFiles(outputFolder, "legs_*.csv");
            var hasExistingLegs = existingLegs != null && existingLegs.Length > 0;

            var calls = result?.Calls ?? new List<ProcessedCall>();
            if (!hasExistingLegs)
            {
                var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
                var legsPath = Path.Combine(outputFolder, $"legs_{timestamp}.csv");
                WriteLegsCsv(calls, legsPath);
            }

            var summaryTimestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
            var summaryPath = Path.Combine(outputFolder, $"summary_{summaryTimestamp}.csv");
            WriteSummaryCsv(result ?? new ProcessingResult(), summaryPath);
        }

        private void WriteLegsCsv(List<ProcessedCall> calls, string path)
        {
            using (var writer = new StreamWriter(path, false, Utf8Bom))
            {
                writer.WriteLine(string.Join(",", LegHeaders));

                foreach (var call in calls)
                {
                    foreach (var leg in call.Legs)
                    {
                        writer.WriteLine(FormatLegLine(leg, call));
                    }
                }
            }
        }

        private void WriteSummaryCsv(ProcessingResult result, string path)
        {
            using (var writer = new StreamWriter(path, false, Utf8Bom))
            {
                writer.WriteLine("Metric,Value");
                writer.WriteLine($"Timestamp,{DateTime.UtcNow:o}");
                writer.WriteLine($"TotalFilesProcessed,{result.TotalFilesProcessed}");
                writer.WriteLine($"TotalRecordsProcessed,{result.TotalRecordsProcessed}");
                writer.WriteLine($"TotalCallsIdentified,{result.TotalCallsIdentified}");
                writer.WriteLine($"CandidateExtensions,{result.CandidateExtensions.Count}");
                writer.WriteLine($"OrphanedLegs,{result.OrphanedLegs}");
                writer.WriteLine($"Warnings,{result.Warnings.Count}");
                writer.WriteLine($"Errors,{result.Errors.Count}");
                writer.WriteLine($"ProcessingTimeMs,{result.ProcessingTimeMs}");
                writer.WriteLine($"RecordsPerSecond,{result.RecordsPerSecond:F0}");
            }
        }
    
        /// <summary>
        /// Create a streaming writer for decoded CDR output.
        /// Output path: {decodedFolder}\{inputFileName}.csv (or fallback to {inputFolder}\output\cdrsDecoded)
        /// Call WriteDecodedRecord for each record, then Dispose when done.
        /// </summary>
                /// <summary>
        /// Create a streaming writer for legs output.
        /// Output path: {outputFolder}\\legs_yyyyMMdd_HHmmss.csv
        /// </summary>
        public LegsStreamWriter CreateLegsStreamWriter(string outputFolder)
        {
            if (!Directory.Exists(outputFolder))
                Directory.CreateDirectory(outputFolder);

            var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
            var legsPath = Path.Combine(outputFolder, $"legs_{timestamp}.csv");
            _log?.Info($"Streaming legs output: {{{legsPath}}}");
            return new LegsStreamWriter(this, legsPath);
        }

        public class LegsStreamWriter : IDisposable
        {
            private readonly StreamWriter _writer;
            private readonly CsvOutputWriter _owner;

            public LegsStreamWriter(CsvOutputWriter owner, string path)
            {
                _owner = owner;
                _writer = new StreamWriter(path, false, Utf8Bom);
                _writer.WriteLine(string.Join(",", LegHeaders));
            }

            public void WriteCall(ProcessedCall call)
            {
                if (call == null) return;
                foreach (var leg in call.Legs)
                {
                    _writer.WriteLine(CsvOutputWriter.FormatLegLine(leg, call));
                }
            }

            public void Dispose()
            {
                _writer?.Dispose();
            }
        }

public DecodedCdrWriter CreateDecodedCdrWriter(string inputFilePath, string decodedFolder = null)
        {
            var inputFileName = Path.GetFileNameWithoutExtension(inputFilePath);
            var outputFolder = decodedFolder;
            if (string.IsNullOrWhiteSpace(outputFolder))
            {
                var inputFolder = Path.GetDirectoryName(inputFilePath) ?? ".";
                outputFolder = Path.Combine(inputFolder, "output", "cdrsDecoded");
            }

            if (!Directory.Exists(outputFolder))
                Directory.CreateDirectory(outputFolder);

            var outputPath = Path.Combine(outputFolder, inputFileName + ".csv");
            _log?.Info($"Decoded CDR output: {{{outputPath}}}");
            return new DecodedCdrWriter(outputPath, RawUsedHeaders);
        }





        // -------------------------------------------------------------------
        // Static helpers for external callers (e.g., orphan output)
        // -------------------------------------------------------------------

        /// <summary>
        /// Get the CSV header line for leg output.
        /// </summary>
        public static string GetLegHeader()
        {
            return string.Join(",", LegHeaders);
        }

                /// <summary>
        /// Format a leg as a CSV line.
        /// </summary>
        // CLWD: raw switch datetime parse helper
        private static bool TryExtractLegDateTimeParts(string value, out string datePart, out string timePart)
        {
            datePart = "";
            timePart = "";
            if (string.IsNullOrEmpty(value)) return false;

            var m = Regex.Match(value, @"^(?<date>\d{4}-\d{2}-\d{2})T(?<time>\d{2}:\d{2}:\d{2}(?:\.\d+)?)(?:[+-]\d{4})?$");
            if (!m.Success) return false;

            datePart = m.Groups["date"].Value;
            timePart = m.Groups["time"].Value;
            return true;
        }
        public static string FormatLegLine(ProcessedLeg leg, ProcessedCall call)
        {
                        // Parse StartDate/StartTime from CallAnswerTime (fallback to InLegConnectTime)
            string startDate = "";
            string startTime = "";
            var startSource = !string.IsNullOrEmpty(leg.CallAnswerTime) ? leg.CallAnswerTime : leg.InLegConnectTime;
            if (TryExtractLegDateTimeParts(startSource, out startDate, out startTime))
            {
                // already extracted in switch format
            }

            return string.Join(",", new[]
            {
                // 1. Timing
                startDate,
                startTime,
                leg.RingTime?.ToString() ?? "",
                leg.Duration.ToString(),
                // 2. Golden fields
                Escape(leg.CallDirection.ToString()),
                Escape(leg.Extension),
                Escape(leg.TransferFrom),
                Escape(leg.DestinationExt),
                Escape(leg.TransferTo),
                Escape(leg.HuntGroupNumber),
                leg.IsAnswered ? "true" : "false",
                leg.IsPickup ? "true" : "false",
                leg.IsForwarded ? "true" : "false",
                leg.IsVoicemail ? "true" : "false",
                // 3. Gateways
                Escape(leg.IngressEndpoint),
                Escape(leg.EgressEndpoint),
                // 3b. Call identification (moved for debugging)
                Escape(leg.GlobalCallId),
                Escape(leg.ThreadId),                // 4. Direction indicators
                leg.OrigPartyId.ToString(),
                Escape(leg.OrigPartyIdText),
                leg.TermPartyId.ToString(),
                Escape(leg.TermPartyIdText),
                // 5. OSV decoded flags
                leg.CauseCode.ToString(),
                Escape(leg.CauseCodeText),
                leg.PerCallFeature.ToString(),
                Escape(leg.PerCallFeatureText),
                leg.AttemptIndicator.ToString(),
                Escape(leg.AttemptIndicatorText),
                leg.PerCallFeatureExt.ToString(),
                Escape(leg.PerCallFeatureExtText),
                leg.CallEventIndicator.ToString(),
                Escape(leg.CallEventIndicatorText),
                // 6. Party details
                Escape(leg.CallerExtension),
                Escape(leg.CallerExternal),
                Escape(leg.CalledExtension),
                Escape(leg.CalledExternal),
                Escape(leg.DialedAni),
                Escape(call.OriginalDialedDigits),
                Escape(leg.CalledParty),
                Escape(leg.CallingNumber),
                Escape(leg.ForwardingParty),
                Escape(leg.ForwardFromExt),
                Escape(leg.ForwardToExt),
                // 7. Reference/tracing (at end)
                leg.LegIndex.ToString("D8"),
                Escape(leg.CallAnswerTime),
                Escape(leg.InLegConnectTime),
                Escape(leg.OutLegReleaseTime),
                Escape(leg.OutLegConnectTime),
                Escape(leg.CallReleaseTime),
                leg.IsHgOnly ? "true" : "false",
                Escape(leg.SourceFile != null ? Path.GetFileName(leg.SourceFile) : null),
                leg.SourceLine.ToString(),
                Escape(leg.GidSequence),
            });
        }
        /// <summary>
        /// RFC 4180 compliant CSV field escaping.
        /// </summary>
        private static string Escape(string value)
        {
            if (value == null) return "";
            if (value.Contains(",") || value.Contains("\"") || value.Contains("\n") || value.Contains("\r"))
            {
                return "\"" + value.Replace("\"", "\"\"") + "\"";
            }
            return value;
        }
    }
}
























