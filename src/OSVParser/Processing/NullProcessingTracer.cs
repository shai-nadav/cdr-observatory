namespace Pipeline.Components.OSVParser.Processing
{
    /// <summary>
    /// No-op tracer implementation. Used when no tracer is injected.
    /// All methods are empty — zero overhead in production.
    /// </summary>
    public sealed class NullProcessingTracer : IProcessingTracer
    {
        /// <summary>Singleton instance for convenience.</summary>
        public static readonly NullProcessingTracer Instance = new NullProcessingTracer();

        public void TraceFieldOrigin(string threadId, int legIndex, string legField, string value,
            string sourceFile, int sourceLine, string sourceColumn) { }

        public void TraceDirectionDecision(string threadId, int legIndex, string direction, string reason,
            bool callerIsInternal, bool destIsInternal,
            string ingress, string egress, int origPartyId, int termPartyId) { }

        public void TraceSuppressedLeg(string threadId, string legDescription, string reason) { }

        public void TraceLegMerge(string threadId, int attemptLine, int answerLine,
            string destinationExt, string reason) { }

        public void TraceSipClassification(string endpoint, string classification, string source) { }

        public void TraceSpecialNumber(string number, string type, string detectionSource) { }

        public void TraceTransferChain(string threadId, int legIndex, string transferFrom, string transferFromRule,
            string transferTo, string transferToRule) { }
    }
}
