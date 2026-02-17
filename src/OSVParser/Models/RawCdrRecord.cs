namespace Pipeline.Components.OSVParser.Models
{
    /// <summary>
    /// Raw parsed CDR record with all fields from CSV.
    /// Field positions use the formula: for BF files with offset=1, o=offset-1=0,
    /// fields[o+N] = fields[N] = spec field N+1.
    /// </summary>
    public class RawCdrRecord
    {
        public CdrRecordType RecordType { get; set; }
        public string RecordTypeRaw { get; set; }       // spec field 1
        public string[] RawFields { get; set; }          // All fields from CSV line

        // Common fields
        public string Timestamp { get; set; }            // spec field 2

        // Full CDR fields (type 00000000)
        public int Duration { get; set; }                // spec field 3 (Call Duration)
        public string GlobalCallId { get; set; }         // spec field 5 (Global Call ID)
        public string CalledParty { get; set; }          // spec field 11 (Terminating Number / Called Party)
        public string CallingNumber { get; set; }        // spec field 12 (Calling Party Number)
        public int AttemptIndicator { get; set; }        // spec field 18 (Attempt Indicator)
        public int CauseCode { get; set; }               // spec field 19 (Release Cause Code)
        public int OrigPartyId { get; set; }             // spec field 40 (Originating Party Identifier)
        public int TermPartyId { get; set; }             // spec field 41 (Terminating Party Identifier)
        public string CallAnswerTime { get; set; }       // spec field 48 (Call Answer Time)
        public string CallReleaseTime { get; set; }      // spec field 49 (Call Release Time)
        public string InLegConnectTime { get; set; }     // spec field 50 (In-Leg Connect Time)
        public string OutLegConnectTime { get; set; }    // spec field 52 (Out-Leg Connect Time)
        public string OutLegReleaseTime { get; set; }    // spec field 53 (Out-Leg Release Time)
        public int PerCallFeature { get; set; }          // spec field 64 (Per Call Feature - BITWISE)
        public string ForwardingParty { get; set; }      // spec field 65 (Forwarding Party Number)
        public string DialedNumber { get; set; }         // spec field 101 (Dialed Number)
        public int MediaType { get; set; }               // spec field 104 (Media Type)
        public long PerCallFeatureExt { get; set; }      // spec field 106 (Per Call Feature Extension - BITWISE)
        public long CallEventIndicator { get; set; }     // spec field 107 (Call Event Indicator - BITWISE)
        public string IngressEndpoint { get; set; }      // spec field 126 (Ingress SIP Endpoint)
        public string EgressEndpoint { get; set; }       // spec field 127 (Egress SIP Endpoint)
        public string DestinationExt { get; set; }       // spec field 128 (Destination Party Number)  [BF: fields[127]]
        public string ThreadIdNode { get; set; }         // spec field 124 (Global Thread ID - Node)   [BF: fields[123]]
        public string ThreadIdSequence { get; set; }     // spec field 125 (Global Thread ID - Sequence) [BF: fields[124]]
        public string GidSequence { get; set; }          // spec field 122 (GID Sequence)\r\n
        // Hunt Group fields (type 00000004)
        public string HuntGroupNumber { get; set; }      // [6]
        public string HGStartTime { get; set; }          // [7]
        public string HGEndTime { get; set; }            // [8]
        public string HGStatus1 { get; set; }            // [9]
        public string HGStatus2 { get; set; }            // [10]
        public string RoutedToExtension { get; set; }    // [11]

        // Call Forward fields (type 10000100)
        public int ForwardType { get; set; }             // [4]
        public string OrigExtension { get; set; }        // [5]
        public string ForwardDestination { get; set; }   // [6]

        /// <summary>Source file path for traceability</summary>
        public string SourceFile { get; set; }

        /// <summary>Line number in source file</summary>
        public int SourceLine { get; set; }
    }
}



