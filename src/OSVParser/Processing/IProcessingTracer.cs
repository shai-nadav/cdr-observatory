namespace Pipeline.Components.OSVParser.Processing
{
    /// <summary>
    /// Tracing interface for CDR processing engine.
    /// Captures field origins, direction decisions, leg suppression/merge events,
    /// SIP classification, special number detection, and transfer chain computation.
    /// Implementations can log, accumulate JSON, or feed a live viewer.
    /// </summary>
    public interface IProcessingTracer
    {
        /// <summary>
        /// Trace the origin of a field value on a processed leg.
        /// Called after each significant field assignment (CallerExtension, CalledExtension,
        /// DialedNumber, DestinationExt, IngressEndpoint, EgressEndpoint, HuntGroupNumber,
        /// ForwardingParty, CallingNumber, CalledParty).
        /// </summary>
        /// <param name="threadId">Thread ID grouping this leg's call.</param>
        /// <param name="legIndex">Leg index within the call (1-based).</param>
        /// <param name="legField">Name of the field being set (e.g. "CallerExtension").</param>
        /// <param name="value">The value assigned to the field.</param>
        /// <param name="sourceFile">CDR source file path.</param>
        /// <param name="sourceLine">Line number in the source file.</param>
        /// <param name="sourceColumn">CSV column name or spec field number.</param>
        void TraceFieldOrigin(string threadId, int legIndex, string legField, string value,
            string sourceFile, int sourceLine, string sourceColumn);

        /// <summary>
        /// Trace a direction decision for a leg.
        /// Called after ResolveDirection determines the call direction,
        /// and again during direction aggregation in AssembleCalls.
        /// </summary>
        /// <param name="threadId">Thread ID grouping this leg's call.</param>
        /// <param name="legIndex">Leg index within the call (1-based).</param>
        /// <param name="direction">The resolved call direction.</param>
        /// <param name="reason">Human-readable reason for this direction decision.</param>
        /// <param name="callerIsInternal">Whether the caller side was determined to be internal.</param>
        /// <param name="destIsInternal">Whether the destination side was determined to be internal.</param>
        /// <param name="ingress">Ingress SIP endpoint (may be null).</param>
        /// <param name="egress">Egress SIP endpoint (may be null).</param>
        /// <param name="origPartyId">OrigPartyId from the CDR record.</param>
        /// <param name="termPartyId">TermPartyId from the CDR record.</param>
        void TraceDirectionDecision(string threadId, int legIndex, string direction, string reason,
            bool callerIsInternal, bool destIsInternal,
            string ingress, string egress, int origPartyId, int termPartyId);

        /// <summary>
        /// Trace a leg being suppressed (routing-only, feature code, HG-only, etc.).
        /// Called when a leg is identified for removal from the output.
        /// </summary>
        /// <param name="threadId">Thread ID of the suppressed leg's call.</param>
        /// <param name="legDescription">Human-readable description of the suppressed leg
        /// (e.g. "Leg#2 DestExt=12345 Duration=0").</param>
        /// <param name="reason">Why the leg was suppressed
        /// (e.g. "Routing-only: DestinationExt is routing number", "Feature code *44/#44").</param>
        void TraceSuppressedLeg(string threadId, string legDescription, string reason);

        /// <summary>
        /// Trace an attempt+answer leg merge.
        /// Called when two consecutive legs targeting the same extension are merged.
        /// </summary>
        /// <param name="threadId">Thread ID of the call.</param>
        /// <param name="attemptLine">Source line of the attempt (0-duration) leg.</param>
        /// <param name="answerLine">Source line of the answer leg.</param>
        /// <param name="destinationExt">The shared destination extension.</param>
        /// <param name="reason">Merge reason (e.g. "Attempt(0s)+Answer to same DestExt").</param>
        void TraceLegMerge(string threadId, int attemptLine, int answerLine,
            string destinationExt, string reason);

        /// <summary>
        /// Trace a SIP endpoint classification lookup.
        /// Called during ResolveInternalityForEmptyRanges for each endpoint check.
        /// </summary>
        /// <param name="endpoint">The SIP endpoint address being classified.</param>
        /// <param name="classification">Result: "Internal", "PSTN", "Unknown".</param>
        /// <param name="source">How the classification was determined
        /// (e.g. "SipMapper", "PartyId fallback", "Thread context").</param>
        void TraceSipClassification(string endpoint, string classification, string source);

        /// <summary>
        /// Trace detection of a special number (routing, HG, CMS, voicemail).
        /// Called when a number is auto-detected or matched against configuration.
        /// </summary>
        /// <param name="number">The special number detected.</param>
        /// <param name="type">Type: "Routing", "HuntGroup", "Voicemail", "CMS".</param>
        /// <param name="detectionSource">How it was detected
        /// (e.g. "HG record pilot", "CMS pattern: dest+caller in same call",
        /// "PerCallFeatureExt bit 64", "Configured").</param>
        void TraceSpecialNumber(string number, string type, string detectionSource);

        /// <summary>
        /// Trace transfer chain computation for a leg.
        /// Called during ComputeTransferChain for each leg.
        /// </summary>
        /// <param name="threadId">Thread ID of the call.</param>
        /// <param name="legIndex">Leg index within the call (1-based).</param>
        /// <param name="transferFrom">Computed Transfer From value (may be null).</param>
        /// <param name="transferFromRule">Which rule determined Transfer From
        /// (e.g. "Rule1: CalledParty routing intermediary", "Rule2: ForwardingParty",
        /// "Inherited from previous leg").</param>
        /// <param name="transferTo">Computed Transfer To value (may be null).</param>
        /// <param name="transferToRule">Which rule determined Transfer To
        /// (e.g. "Next leg is VM", "Look-ahead: next leg's TransferFrom",
        /// "Last leg: null").</param>
        void TraceTransferChain(string threadId, int legIndex, string transferFrom, string transferFromRule,
            string transferTo, string transferToRule);
    }
}
