using System;
using System.Collections.Generic;
using System.IO;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser.Parser
{
    /// <summary>
    /// Parses CDR files from OpenScape Voice PBX.
    /// Supports both raw .BF files (with headers/footers and sequence-numbered lines)
    /// and plain CSV files. Handles record types: 00000000, 00000004, 00000005, 10000100.
    ///
    /// Field mapping formula: for BF files with offset=1, o=offset-1=0,
    /// fields[o+N] = fields[N] = spec field N+1.
    /// For plain files with offset=0, o=-1, fields[o+N] = fields[N-1] = spec field N.
    /// </summary>
    public class CdrCsvParser
    {
        private readonly IProcessorLogger _logger;

        public CdrCsvParser(IProcessorLogger logger = null)
        {
            _logger = logger ?? new NullProcessorLogger();
        }

        // BF file header/footer keywords to skip
        private static readonly string[] BfKeywords = new[]
        {
            "FILENAME:", "DEVICE:", "HOSTNAME:", "FILETYPE:",
            "VERSION:", "CREATE:", "CLOSE:"
        };

        /// <summary>
        /// Parse a single CDR file (.BF or CSV) into raw CDR records.
        /// Uses streaming to handle large files without loading everything into memory.
        /// </summary>
        public List<RawCdrRecord> ParseFile(string filePath)
        {
            var records = new List<RawCdrRecord>();
            foreach (var record in StreamParseFile(filePath))
            {
                records.Add(record);
            }
            return records;
        }

        /// <summary>
        /// Stream-parse a CDR file, yielding records one at a time.
        /// Memory-efficient for large files (handles 100s of MB).
        /// </summary>
        public IEnumerable<RawCdrRecord> StreamParseFile(string filePath)
        {
            using (var reader = new StreamReader(filePath))
            {
                int lineNumber = 0;
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    lineNumber++;
                    line = line.Trim();
                    if (string.IsNullOrEmpty(line)) continue;

                    // Skip BF file headers and footers
                    if (IsBfHeaderLine(line)) continue;

                    RawCdrRecord record = null;
                    try
                    {
                        record = ParseLine(line, filePath, lineNumber);
                    }
                    catch (Exception ex)
                    {
                        // Log and continue -- don't let one bad line kill the whole file
                        _logger?.Error($"Failed to parse line {lineNumber} in {filePath}", ex);
                    }

                    if (record != null)
                    {
                        yield return record;
                    }
                }
            }
        }

        /// <summary>
        /// Check if a line is a BF file header/footer line.
        /// </summary>
        private bool IsBfHeaderLine(string line)
        {
            foreach (var keyword in BfKeywords)
            {
                if (line.StartsWith(keyword, StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Parse a single CSV line into a raw CDR record.
        /// Handles both formats:
        ///   - Plain: "00000000,timestamp,duration,..."
        ///   - BF seq-prefixed: "0,00000000,timestamp,duration,..."
        /// </summary>
        public RawCdrRecord ParseLine(string line, string sourceFile = null, int sourceLine = 0)
        {
            if (string.IsNullOrEmpty(line)) return null;

            var fields = line.Split(',');
            if (fields.Length < 2) return null;

            // Detect format: if fields[0] is a sequence number and fields[1] is a record type,
            // this is a BF seq-prefixed line. Strip the seq prefix so field indices match the spec.
            var firstField = fields[0].Trim().Trim('"');
            int offset = 0;
            int firstFieldInt = 0;

            if (IsRecordType(firstField))
            {
                // Plain format: fields[0] is already the record type
                offset = 0;
            }
            else if (int.TryParse(firstField, out firstFieldInt) && fields.Length > 2 && IsRecordType(fields[1].Trim().Trim('"')))
            {
                // BF format: fields[0] is seq number, fields[1] is record type
                offset = 1;
            }
            else
            {
                return null; // Unrecognized format
            }

            var recordTypeRaw = fields[offset].Trim().Trim('"');

            // Apply offset to all field accesses
            switch (recordTypeRaw)
            {
                case "00000000":
                    return ParseFullCdr(fields, recordTypeRaw, offset, sourceFile, sourceLine);
                case "00000004":
                    return ParseHuntGroup(fields, recordTypeRaw, offset, sourceFile, sourceLine);
                case "00000005":
                    // Feature/supplementary record -- skip for now (not processed)
                    return null;
                case "10000100":
                    return ParseCallForward(fields, recordTypeRaw, offset, sourceFile, sourceLine);
                default:
                    return null; // Unknown record type -- skip
            }
        }

        private bool IsRecordType(string value)
        {
            return value == "00000000" || value == "00000004" ||
                   value == "00000005" || value == "10000100";
        }

        private RawCdrRecord ParseFullCdr(string[] fields, string recordTypeRaw, int offset, string sourceFile, int sourceLine)
        {
            // Field mapping: o = offset - 1
            // For BF (offset=1): o=0, fields[o+N] = fields[N] = spec field N+1
            // For plain (offset=0): o=-1, fields[o+N] = fields[N-1] = spec field N
            var o = offset - 1;

            var record = new RawCdrRecord
            {
                RecordType = CdrRecordType.FullCdr,
                RecordTypeRaw = recordTypeRaw,
                RawFields = fields,
                SourceFile = sourceFile,
                SourceLine = sourceLine,

                Timestamp = GetField(fields, o + 2),            // spec field 2 (Timestamp)
                Duration = GetIntField(fields, o + 3),           // spec field 3 (Call Duration)
                GlobalCallId = GetField(fields, o + 5),          // spec field 5 (Global Call ID)
                CalledParty = GetField(fields, o + 10),          // spec field 11 (Terminating Number / Called Party)
                CallingNumber = GetField(fields, o + 11),        // spec field 12 (Calling Party Number)
                AttemptIndicator = GetIntField(fields, o + 17),  // spec field 18 (Attempt Indicator)
                CauseCode = GetIntField(fields, o + 18),         // spec field 19 (Release Cause Code)
                OrigPartyId = GetIntField(fields, o + 39),       // spec field 40 (Originating Party Identifier)
                TermPartyId = GetIntField(fields, o + 40),       // spec field 41 (Terminating Party Identifier)
                CallAnswerTime = GetField(fields, o + 47),       // spec field 48 (Call Answer Time)
                CallReleaseTime = GetField(fields, o + 48),      // spec field 49 (Call Release Time)
                InLegConnectTime = GetField(fields, o + 49),     // spec field 50 (In-Leg Connect Time)
                OutLegConnectTime = GetField(fields, o + 51),    // spec field 52 (Out-Leg Connect Time)
                OutLegReleaseTime = GetField(fields, o + 52),    // spec field 53 (Out-Leg Release Time)
                PerCallFeature = GetIntField(fields, o + 63),    // spec field 64 (Per Call Feature - BITWISE)
                ForwardingParty = GetField(fields, o + 64),      // spec field 65 (Forwarding Party Number)
                DialedNumber = GetField(fields, o + 100),        // spec field 101 (Dialed Number)
                MediaType = GetIntField(fields, o + 103),        // spec field 104 (Media Type)
                PerCallFeatureExt = GetLongField(fields, o + 105),   // spec field 106 (Per Call Feature Extension - BITWISE)
                CallEventIndicator = GetLongField(fields, o + 106),  // spec field 107 (Call Event Indicator - BITWISE)
                ThreadIdNode = GetField(fields, o + 123),        // spec field 124 (Global Thread ID - Node)
                ThreadIdSequence = GetField(fields, o + 124),    // spec field 125 (Global Thread ID - Sequence)
                GidSequence = GetField(fields, o + 121),         // spec field 122 (GID Sequence)
                IngressEndpoint = GetField(fields, o + 125),     // spec field 126 (Ingress SIP Endpoint)
                EgressEndpoint = GetField(fields, o + 126),      // spec field 127 (Egress SIP Endpoint)
                DestinationExt = GetField(fields, o + 127),      // spec field 128 (Destination Party Number)
            };

            // Debug: log endpoint parsing
            if (!string.IsNullOrEmpty(record.IngressEndpoint) || !string.IsNullOrEmpty(record.EgressEndpoint))
            {
                _logger?.Debug($"Parsed endpoints: Ingress={{{record.IngressEndpoint}}}, Egress={{{record.EgressEndpoint}}} from {{{sourceFile}}}:{{{sourceLine}}}");
            }

            return record;
        }

        private RawCdrRecord ParseHuntGroup(string[] fields, string recordTypeRaw, int offset, string sourceFile, int sourceLine)
        {
            var o = offset - 1;
            return new RawCdrRecord
            {
                RecordType = CdrRecordType.HuntGroup,
                RecordTypeRaw = recordTypeRaw,
                RawFields = fields,
                SourceFile = sourceFile,
                SourceLine = sourceLine,

                Timestamp = GetField(fields, o + 2),        // [2]
                GlobalCallId = GetField(fields, o + 4),      // [4]
                HuntGroupNumber = GetField(fields, o + 6),   // [6]
                HGStartTime = GetField(fields, o + 7),       // [7]
                HGEndTime = GetField(fields, o + 8),         // [8]
                HGStatus1 = GetField(fields, o + 9),         // [9]
                HGStatus2 = GetField(fields, o + 10),        // [10]
                RoutedToExtension = GetField(fields, o + 11),// [11]
            };
        }

        private RawCdrRecord ParseCallForward(string[] fields, string recordTypeRaw, int offset, string sourceFile, int sourceLine)
        {
            var o = offset - 1;
            return new RawCdrRecord
            {
                RecordType = CdrRecordType.CallForward,
                RecordTypeRaw = recordTypeRaw,
                RawFields = fields,
                SourceFile = sourceFile,
                SourceLine = sourceLine,

                Timestamp = GetField(fields, o + 2),         // [2]
                Duration = GetIntField(fields, o + 3),       // [3]
                ForwardType = GetIntField(fields, o + 4),    // [4]
                OrigExtension = GetField(fields, o + 5),     // [5]
                ForwardDestination = GetField(fields, o + 6),// [6]
            };
        }

        /// <summary>
        /// Get field value by 0-based array index.
        /// </summary>
        private string GetField(string[] fields, int index)
        {
            if (index < 0 || index >= fields.Length) return null;
            var value = fields[index]?.Trim().Trim('"');
            return string.IsNullOrEmpty(value) ? null : value;
        }

        private int GetIntField(string[] fields, int index)
        {
            var value = GetField(fields, index);
            if (value == null) return 0;
            int result;
            return int.TryParse(value, out result) ? result : 0;
        }

        private long GetLongField(string[] fields, int index)
        {
            var value = GetField(fields, index);
            if (value == null) return 0;
            long result;
            return long.TryParse(value, out result) ? result : 0;
        }
    }
}












