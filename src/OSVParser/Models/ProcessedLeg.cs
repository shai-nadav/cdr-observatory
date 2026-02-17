namespace Pipeline.Components.OSVParser.Models
{
    /// <summary>
    /// A single processed call leg with all derived fields.
    /// </summary>
    public class ProcessedLeg
    {
        public string GlobalCallId { get; set; }
        public int LegIndex { get; set; }

        // Direction
        public CallDirection CallDirection { get; set; }

        // Parties
        public string CallerExtension { get; set; }
        public string CallerExternal { get; set; }
        /// <summary>Raw calling number from CDR (before split into Extension/External).</summary>
        public string CallingNumber { get; set; }
        public string CalledExtension { get; set; }
        public string CalledExternal { get; set; }
        public string DialedNumber { get; set; }
        public string DestinationExt { get; set; }
        /// <summary>The external number for the call: ANI (caller) for Incoming, dialed number for Outgoing/T2T.</summary>
        public string DialedAni { get; set; }
        /// <summary>Per-record extension: for Incoming/Outgoing/T2T this holds DestinationExt (and DestinationExt is cleared).</summary>
        public string Extension { get; set; }

        // Raw Called Party (spec field 11, a.k.a. Terminating Number)
        /// <summary>The raw CalledParty/OrigParty field from the CDR (HG number, VM code, CMS ext, etc.).</summary>
        public string CalledParty { get; set; }

        // Hunt Group
        public string HuntGroupNumber { get; set; }

        // Forwarding
        public string ForwardFromExt { get; set; }
        public string ForwardToExt { get; set; }
        public bool IsForwarded { get; set; }
        /// <summary>Raw Forwarding Party Number from CDR (spec field 65).</summary>
        public string ForwardingParty { get; set; }

        // Pickup
        public bool IsPickup { get; set; }

        // Voicemail
        /// <summary>True if call was to voicemail (CF-to-Voicemail flag or VM number match).</summary>
        public bool IsVoicemail { get; set; }

        // Transfer chain
        /// <summary>The routing entity that sent the call to this leg (HG, CMS ext, forwarding party).</summary>
        public string TransferFrom { get; set; }
        /// <summary>Where the call goes next after this leg (next leg's routing entity or destination).</summary>
        public string TransferTo { get; set; }

        // Answer
        public bool IsAnswered { get; set; }
        public int Duration { get; set; }
        /// <summary>Ring time in seconds (CallAnswerTime - InLegConnectTime).</summary>
        public int? RingTime { get; set; }

        // Timestamps
        public string CallAnswerTime { get; set; }
        public string InLegConnectTime { get; set; }
        public string OutLegReleaseTime { get; set; }
        public string OutLegConnectTime { get; set; }
        public string CallReleaseTime { get; set; }

        // Trunk info (all call types)
        public int OrigPartyId { get; set; }
        public string OrigPartyIdText { get; set; }
        public int TermPartyId { get; set; }
        public string TermPartyIdText { get; set; }

        // Companion text fields for numeric codes
        public int CauseCode { get; set; }
        public string CauseCodeText { get; set; }
        public int PerCallFeature { get; set; }
        public string PerCallFeatureText { get; set; }
        public int AttemptIndicator { get; set; }
        public string AttemptIndicatorText { get; set; }
        public long PerCallFeatureExt { get; set; }
        public string PerCallFeatureExtText { get; set; }
        public long CallEventIndicator { get; set; }
        public string CallEventIndicatorText { get; set; }

        // Traceability
        public string SourceFile { get; set; }
        // SIP Endpoints (for direction aggregation)
        /// <summary>Ingress SIP Endpoint from raw CDR.</summary>
        public string IngressEndpoint { get; set; }
        /// <summary>Egress SIP Endpoint from raw CDR.</summary>
        public string EgressEndpoint { get; set; }
        public int SourceLine { get; set; }
        /// <summary>True if this leg was created from a HG routing record (type 00000004) only - no full CDR.</summary>
        public bool IsHgOnly { get; set; }
        public string GidSequence { get; set; }
        public string ThreadId { get; set; }
    }
}



