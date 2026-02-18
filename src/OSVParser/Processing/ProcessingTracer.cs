using System;

namespace Pipeline.Components.OSVParser.Processing
{
    /// <summary>
    /// Real tracer implementation that logs all trace events at Debug level
    /// using tagged format for structured parsing by the CDR Viewer frontend.
    /// 
    /// Tags: [FIELD], [DIRECTION], [SUPPRESSED], [MERGE], [SIP], [SPECIAL], [TRANSFER]
    /// </summary>
    public sealed class ProcessingTracer : IProcessingTracer
    {
        private readonly IProcessorLogger _logger;

        public ProcessingTracer(IProcessorLogger logger)
        {
            if (logger == null)
                throw new ArgumentNullException(nameof(logger));

            _logger = logger;
        }

        public void TraceFieldOrigin(string threadId, int legIndex, string legField, string value,
            string sourceFile, int sourceLine, string sourceColumn)
        {
            _logger.Debug(
                "[FIELD] ThreadId={0} Leg={1} Field={2} Value={3} SourceFile={4} SourceLine={5} SourceColumn={6}",
                threadId ?? "", legIndex, legField ?? "", value ?? "",
                sourceFile ?? "", sourceLine, sourceColumn ?? "");
        }

        public void TraceDirectionDecision(string threadId, int legIndex, string direction, string reason,
            bool callerIsInternal, bool destIsInternal,
            string ingress, string egress, int origPartyId, int termPartyId)
        {
            _logger.Debug(
                "[DIRECTION] ThreadId={0} Leg={1} Direction={2} Reason={3} CallerInternal={4} DestInternal={5} Ingress={6} Egress={7} OrigPartyId={8} TermPartyId={9}",
                threadId ?? "", legIndex, direction ?? "", reason ?? "",
                callerIsInternal, destIsInternal,
                ingress ?? "", egress ?? "", origPartyId, termPartyId);
        }

        public void TraceSuppressedLeg(string threadId, string legDescription, string reason)
        {
            _logger.Debug(
                "[SUPPRESSED] ThreadId={0} Leg={1} Reason={2}",
                threadId ?? "", legDescription ?? "", reason ?? "");
        }

        public void TraceLegMerge(string threadId, int attemptLine, int answerLine,
            string destinationExt, string reason)
        {
            _logger.Debug(
                "[MERGE] ThreadId={0} AttemptLine={1} AnswerLine={2} DestExt={3} Reason={4}",
                threadId ?? "", attemptLine, answerLine, destinationExt ?? "", reason ?? "");
        }

        public void TraceSipClassification(string endpoint, string classification, string source)
        {
            _logger.Debug(
                "[SIP] Endpoint={0} Classification={1} Source={2}",
                endpoint ?? "", classification ?? "", source ?? "");
        }

        public void TraceSpecialNumber(string number, string type, string detectionSource)
        {
            _logger.Debug(
                "[SPECIAL] Number={0} Type={1} DetectionSource={2}",
                number ?? "", type ?? "", detectionSource ?? "");
        }

        public void TraceTransferChain(string threadId, int legIndex, string transferFrom, string transferFromRule,
            string transferTo, string transferToRule)
        {
            _logger.Debug(
                "[TRANSFER] ThreadId={0} Leg={1} TransferFrom={2} TransferFromRule={3} TransferTo={4} TransferToRule={5}",
                threadId ?? "", legIndex, transferFrom ?? "", transferFromRule ?? "",
                transferTo ?? "", transferToRule ?? "");
        }
    }
}
