using System;
using System.Collections.Generic;
using System.Linq;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser.Processing.Pipeline
{
    internal sealed class CallAssembler
    {
        private readonly PipelineContext _context;
        private readonly Pipeline.LegMerger _legMerger;
        private readonly Pipeline.TransferChainResolver _transferChainResolver;
        private readonly Pipeline.LegSuppressor _legSuppressor;
        private readonly Pipeline.CallFinalizer _callFinalizer;
        private readonly HashSet<string> _outputtedThreadIds;
        private readonly Action<ProcessedCall, ProcessingResult> _emitCall;

        public CallAssembler(
            PipelineContext context,
            Pipeline.LegMerger legMerger,
            Pipeline.TransferChainResolver transferChainResolver,
            Pipeline.LegSuppressor legSuppressor,
            Pipeline.CallFinalizer callFinalizer,
            HashSet<string> outputtedThreadIds,
            Action<ProcessedCall, ProcessingResult> emitCall)
        {
            _context = context;
            _legMerger = legMerger;
            _transferChainResolver = transferChainResolver;
            _legSuppressor = legSuppressor;
            _callFinalizer = callFinalizer;
            _outputtedThreadIds = outputtedThreadIds;
            _emitCall = emitCall;
        }


        public void AssembleCalls(ProcessingResult result)
        {
            // Build groups from cache (keyed by Thread ID or GID), filtering out HG-only legs
            var groups = new List<List<ProcessedLeg>>();
            foreach (var key in _context.Cache.GetAllGids())
            {
                // Skip calls already output via streaming
                if (_outputtedThreadIds.Contains(key)) continue;

                var allLegs = _context.Cache.GetPendingLegs(key);
                var hgOnlyLegs = allLegs.Where(l => l.IsHgOnly).ToList();
                var legs = allLegs.Where(l => !l.IsHgOnly).ToList();
                
                // Trace HG-only legs that have no matching CDR
                foreach (var hgLeg in hgOnlyLegs)
                {
                    _context.Tracer.TraceSuppressedLeg(
                        key,
                        string.Format("HG-only Leg GID={0} HG={1} DestExt={2} Line={3}",
                            hgLeg.GlobalCallId ?? "", hgLeg.HuntGroupNumber ?? "",
                            hgLeg.DestinationExt ?? "", hgLeg.SourceLine),
                        "HG-only leg with no matching CDR record (orphaned)");
                }
                
                if (legs.Count == 0) continue;
                groups.Add(legs);
            }

            // Build ProcessedCall objects
            foreach (var legs in groups)
            {
                var orderedLegs = legs
                    .OrderBy(l => l.InLegConnectTime ?? "")
                    .ThenBy(l => l.SourceLine)
                    .ToList();

                for (int i = 0; i < orderedLegs.Count; i++)
                {
                    orderedLegs[i].LegIndex = i + 1;
                }

                // Merge consecutive attempt+answer legs targeting same extension
                orderedLegs = _legMerger.MergeAttemptAnswerLegs(orderedLegs);

                // Direction aggregation at leg level: for each leg, if SIP endpoints indicate external, update direction
                foreach (var leg in orderedLegs)
                {
                    var ingressIsPstn = !string.IsNullOrEmpty(leg.IngressEndpoint) && _context.IsSipPstn(leg.IngressEndpoint);
                    var egressIsPstn = !string.IsNullOrEmpty(leg.EgressEndpoint) && _context.IsSipPstn(leg.EgressEndpoint);
                    var prevDir = leg.CallDirection;
                    
                    // Aggregate direction based on endpoints (most external wins)
                    if (ingressIsPstn && egressIsPstn && leg.CallDirection != CallDirection.TrunkToTrunk)
                        leg.CallDirection = CallDirection.TrunkToTrunk;
                    else if (egressIsPstn && leg.CallDirection == CallDirection.Internal)
                        leg.CallDirection = CallDirection.Outgoing;
                    else if (ingressIsPstn && leg.CallDirection == CallDirection.Internal)
                        leg.CallDirection = CallDirection.Incoming;
                    
                    if (leg.CallDirection != prevDir)
                    {
                        _context.Tracer.TraceDirectionDecision(
                            leg.ThreadId, leg.LegIndex, leg.CallDirection.ToString(),
                            string.Format("SIP PSTN override in AssembleCalls: {0}->{1} (Ingress PSTN={2}, Egress PSTN={3})",
                                prevDir, leg.CallDirection, ingressIsPstn, egressIsPstn),
                            !ingressIsPstn, !egressIsPstn,
                            leg.IngressEndpoint, leg.EgressEndpoint,
                            leg.OrigPartyId, leg.TermPartyId);
                    }
                }

                // Compute Transfer From / Transfer To for each leg
                _transferChainResolver.ComputeTransferChain(orderedLegs);

                //  Capture pre-suppression info for call-level fields 
                // OriginalDialedDigits: from the first leg with a DialedNumber
                // BEFORE CMS suppression and BEFORE "Internal: Dialed=DestExt" override.
                var preSuppressionFirstDialed = orderedLegs
                    .Select(l => l.DialedNumber)
                    .FirstOrDefault(d => !string.IsNullOrEmpty(d));
                // First leg's DestExt before suppression (for Incoming extension)
                var preSuppressionFirstDestExt = orderedLegs.First().DestinationExt;
                // Caller info before suppression (CMS legs may alter the first remaining leg)
                var preSuppressionCallerLeg = orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.CallerExternal))
                    ?? orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.CallerExtension))
                    ?? orderedLegs.First();
                // Capture direction before any modifications (routing number logic may change it)
                var preSuppressionCallerDirection = preSuppressionCallerLeg.CallDirection;
                // Direction aggregation: if ANY leg has external direction, use that
                // Priority: T2T > Outgoing > Incoming > Internal (most external wins)
                var externalLeg = orderedLegs.FirstOrDefault(l =>
                    l.CallDirection == CallDirection.TrunkToTrunk ||
                    l.CallDirection == CallDirection.Outgoing ||
                    l.CallDirection == CallDirection.Incoming);
                if (externalLeg != null)
                {
                    preSuppressionCallerDirection = externalLeg.CallDirection;
                }

                // Auto-detect CMS: numbers that appear as DestinationExt AND CallingNumber
                // in the same call are routing intermediaries (CMS receives call, then calls agent)
                var destinations = new HashSet<string>(
                    orderedLegs
                        .Where(l => !string.IsNullOrEmpty(l.DestinationExt))
                        .Select(l => l.DestinationExt),
                    StringComparer.OrdinalIgnoreCase);
                var callers = new HashSet<string>(
                    orderedLegs
                        .Select(l => l.CallingNumber)
                        .Where(c => !string.IsNullOrEmpty(c)),
                    StringComparer.OrdinalIgnoreCase);
                foreach (var num in destinations.Where(d => callers.Contains(d)))
                {
                    if (!_context.DetectedRoutingNumbers.Contains(num))
                    {
                        _context.DetectedRoutingNumbers.Add(num);
                        _context.Logger.Debug($"Auto-detected routing number (CMS pattern): {num}");
                        _context.Tracer.TraceSpecialNumber(num, "CMS", "CMS pattern: appears as both DestinationExt and CallingNumber in same call");
                    }
                }

                // Suppress routing-only legs (CMS, pilot  only in Transfer From/To)
                _legSuppressor.SuppressCmsLegs(orderedLegs);

                // After CMS suppression, legs whose TransferFrom is a routing number
                // represent internal routing steps (CMSagent)  direction=Internal
                foreach (var leg in orderedLegs)
                {
                    if (!string.IsNullOrEmpty(leg.TransferFrom) && _context.IsRoutingNumber(leg.TransferFrom)
                        && leg.CallDirection != CallDirection.Internal)
                    {
                        leg.CallDirection = CallDirection.Internal;
                    }
                }

                // General HG propagation: if any leg has HG, forward-propagate
                // to subsequent non-VM legs that don't have one.
                // This handles cases like HGHG2 where leg 2 should inherit parent HG.
                string propagatedHG = null;
                foreach (var leg in orderedLegs)
                {
                    if (!string.IsNullOrEmpty(leg.HuntGroupNumber))
                    {
                        propagatedHG = leg.HuntGroupNumber;
                    }
                    else if (propagatedHG != null && !_context.IsVmLeg(leg))
                    {
                        leg.HuntGroupNumber = propagatedHG;
                    }
                }

                // MLHG detection from PerCallFeatureExt bit 1024 (Call to MLHG)
                // Only apply if no HG was found via normal propagation
                if (propagatedHG == null)
                {
                    var mlhgPilot = orderedLegs
                        .Where(l => (l.PerCallFeatureExt & 1024) != 0 && !string.IsNullOrEmpty(l.CalledParty))
                        .Select(l => l.CalledParty)
                        .FirstOrDefault();
                    if (!string.IsNullOrEmpty(mlhgPilot))
                    {
                        foreach (var leg in orderedLegs.Where(l => string.IsNullOrEmpty(l.HuntGroupNumber)))
                        {
                            leg.HuntGroupNumber = mlhgPilot;
                        }
                    }
                }

                // VM legs: adjust DestinationExt based on whether VM actually answered
                foreach (var vmLeg in orderedLegs.Where(l => _context.IsVmLeg(l)))
                {
                    if (!vmLeg.IsAnswered && vmLeg.Duration == 0 && !string.IsNullOrEmpty(vmLeg.ForwardingParty))
                    {
                        // Unanswered VM leg = the forwarding extension that sent to VM
                        // DestinationExt = the forwarding extension (where the call rang)
                        vmLeg.DestinationExt = vmLeg.ForwardingParty;
                        vmLeg.CalledExtension = vmLeg.ForwardingParty;
                    }
                    else
                    {
                        // Answered VM leg: DestinationExt = VM code
                        var vmNum = _context.GetVoicemailNumber();
                        if (!string.IsNullOrEmpty(vmNum) && vmLeg.DestinationExt != vmNum)
                        {
                            vmLeg.DestinationExt = vmNum;
                            vmLeg.CalledExtension = vmNum;
                        }
                    }
                }

                // Rule: for Internal calls, Dialed = Dest Ext (always show destination extension)
                foreach (var leg in orderedLegs)
                {
                    if (leg.CallDirection == CallDirection.Internal && !string.IsNullOrEmpty(leg.DestinationExt))
                    {
                        leg.DialedNumber = leg.DestinationExt;
                    }
                }

                if (orderedLegs.Count == 0) continue; // All legs were routing-only

                var firstLeg = orderedLegs.First();
                var answeredLeg = orderedLegs.LastOrDefault(l => l.IsAnswered);
                var lastLeg = orderedLegs.Last();

                // Use pre-suppression caller info (CMS suppression may remove the original caller leg)
                var callerLeg = preSuppressionCallerLeg;

                // Find the best dialed number (post-suppression)
                var dialedLeg = orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.DialedNumber))
                    ?? firstLeg;

                var call = new ProcessedCall
                {
                    GlobalCallId = firstLeg.GlobalCallId,
                    TotalLegs = orderedLegs.Count,
                    IsAnswered = orderedLegs.Any(l => l.IsAnswered),
                    // Use the maximum duration among answered legs (not sum).
                    // CMS/HG scenarios have multiple "answered" routing legs;
                    // the primary leg has the longest duration.
                    TotalDuration = orderedLegs.Where(l => l.IsAnswered).DefaultIfEmpty()
                        .Max(l => l?.Duration ?? 0),
                    CallerExtension = callerLeg.CallerExtension,
                    CallerExternal = callerLeg.CallerExternal,
                    DialedNumber = dialedLeg.DialedNumber,
                    // Original Dialed Digits: from the first leg BEFORE CMS suppression
                    OriginalDialedDigits = preSuppressionFirstDialed,
                    HuntGroupNumber = orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.HuntGroupNumber))?.HuntGroupNumber,
                    ThreadId = firstLeg.ThreadId,
                    CallDirection = preSuppressionCallerDirection,
                    Legs = orderedLegs,
                };

                // Detect TrunkToTrunk at call level: external caller with external destination
                // (leg direction may be Incoming due to forwarding, but call is T2T)
                if (!string.IsNullOrEmpty(call.CallerExternal) && string.IsNullOrEmpty(call.CallerExtension))
                {
                    // Caller is external. Check if destination is also external.
                    var anyPstnToPstn = true && orderedLegs.Any(l =>
                        (_context.IsSipKnown(l.IngressEndpoint)
                            && _context.IsSipPstn(l.IngressEndpoint)
                            && _context.IsSipKnown(l.EgressEndpoint)
                            && _context.IsSipPstn(l.EgressEndpoint))
);

                    var anyInternalDest = orderedLegs.Any(l =>
                        (true
                            ? _context.IsInternalDestForEmptyRanges(l)
                            : (_context.IsInternalNumber(l.DestinationExt) || _context.IsInternalNumber(l.CalledExtension))));
                    if (anyPstnToPstn || !anyInternalDest)
                    {
                        call.CallDirection = CallDirection.TrunkToTrunk;
                    }
                }
                // Update call.DialedNumber after Internal override (it may have been empty before)
                if (string.IsNullOrEmpty(call.DialedNumber))
                {
                    var updatedDialedLeg = orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.DialedNumber));
                    if (updatedDialedLeg != null) call.DialedNumber = updatedDialedLeg.DialedNumber;
                }

                // Fallback: if OriginalDialedDigits is empty but DialedNumber is set, use DialedNumber
                if (string.IsNullOrEmpty(call.OriginalDialedDigits) && !string.IsNullOrEmpty(call.DialedNumber))
                {
                    call.OriginalDialedDigits = call.DialedNumber;
                }

                // Extension = "our extension" regardless of direction
                // Incoming: the first leg's DestExt (before suppression)  shows which
                //           extension the call was originally directed to
                // Outgoing: the extension that made the call (CallerExtension)
                // Internal: the originating extension (CallerExtension)
                // T2T: the extension that triggered the trunk-to-trunk (ForwardingParty or caller)
                switch (call.CallDirection)
                {
                    case CallDirection.Incoming:
                        // Use first leg's DestExt from before CMS suppression
                        call.Extension = preSuppressionFirstDestExt;
                        if (string.IsNullOrEmpty(call.Extension))
                            call.Extension = (answeredLeg ?? lastLeg).DestinationExt;
                        break;
                    case CallDirection.Outgoing:
                        call.Extension = call.CallerExtension;
                        break;
                    case CallDirection.Internal:
                        call.Extension = call.CallerExtension;
                        break;
                    case CallDirection.TrunkToTrunk:
                        // The extension that triggered the T2T (forwarding party or caller)
                        call.Extension = orderedLegs
                            .Select(l => l.ForwardingParty)
                            .FirstOrDefault(fp => !string.IsNullOrEmpty(fp))
                            ?? call.CallerExtension;
                        break;
                    default:
                        call.Extension = call.CallerExtension;
                        break;
                }

                // Apply shared leg post-processing (direction, DialedAni, Extension/DestExt)
                _callFinalizer.ApplyLegPostProcessing(call, orderedLegs);

                // NOTE: Feature code filtering (*44/#44) is now handled early in ProcessSingleRecord()
                // before leg creation. Any feature code calls that somehow reach here are unexpected.

                // === T2T splitting: split TrunkToTrunk calls into T2T-In + T2T-Out ===
                // Only split when there's an internal extension involved (ForwardingParty).
                // Pure T2T (external-to-external with no internal routing) stays as 1 call.
                HashSet<string> internalNums = null;
                if (true)
                {
                    internalNums = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    foreach (var l in orderedLegs)
                    {
                        if (l.OrigPartyId == 900 || l.TermPartyId == 902)
                        {
                            if (!string.IsNullOrEmpty(l.CallingNumber)) internalNums.Add(l.CallingNumber);
                            if (!string.IsNullOrEmpty(l.CalledParty)) internalNums.Add(l.CalledParty);
                            if (!string.IsNullOrEmpty(l.DestinationExt)) internalNums.Add(l.DestinationExt);
                            if (!string.IsNullOrEmpty(l.CalledExtension)) internalNums.Add(l.CalledExtension);
                            if (!string.IsNullOrEmpty(l.ForwardingParty)) internalNums.Add(l.ForwardingParty);
                        }
                    }
                }

                var pstnToPstn = true && orderedLegs.Any(l =>
                    _context.IsSipPstn(l.IngressEndpoint) && _context.IsSipPstn(l.EgressEndpoint));

                var forwardingExt = orderedLegs
                    .Select(l => l.ForwardingParty)
                    .FirstOrDefault(fp =>
                        !string.IsNullOrEmpty(fp) &&
                        (true
                            ? (internalNums != null && (internalNums.Contains(fp)
                                || (pstnToPstn && !_context.IsRoutingNumber(fp))))
                            : _context.IsInternalNumber(fp)));
                
                if (call.CallDirection == CallDirection.TrunkToTrunk && orderedLegs.Count > 0
                    && !string.IsNullOrEmpty(forwardingExt))
                {
                    var t2tLeg = orderedLegs[0];
                    var ext = forwardingExt;
                    var external1 = call.CallerExternal ?? t2tLeg.CallerExternal ?? "";
                    var t2tExternalDest = orderedLegs
                        .Select(l => l.CalledExternal)
                        .FirstOrDefault(e => !string.IsNullOrEmpty(e)) ?? "";
                    var external2 = !string.IsNullOrEmpty(t2tExternalDest) ? t2tExternalDest : (t2tLeg.DialedAni ?? "");
                    var transferFrom = ext;
                    var originalDnis = t2tLeg.DialedNumber ?? "";

                    _context.Logger.Debug($"Splitting T2T: {external1} -> {ext} -> {external2}");

                    // T2T-In: External caller -> Internal extension
                    var t2tInLeg = new ProcessedLeg
                    {
                        GlobalCallId = t2tLeg.GlobalCallId,
                        ThreadId = t2tLeg.ThreadId,
                        GidSequence = t2tLeg.GidSequence,
                        LegIndex = 1,
                        CallDirection = CallDirection.T2TIn,
                        CallerExternal = external1,
                        CallingNumber = external1,
                        CalledExtension = ext,
                        CalledParty = ext,
                        DestinationExt = "",
                        Extension = ext,
                        DialedNumber = ext,
                        DialedAni = external1,
                        TransferFrom = "", //incoming leg, no transfer from
                        TransferTo = "",
                        Duration = call.TotalDuration,
                        IsAnswered = call.IsAnswered,
                        IsForwarded = t2tLeg.IsForwarded,
                        HuntGroupNumber = t2tLeg.HuntGroupNumber,
                        CauseCode = t2tLeg.CauseCode,
                        CauseCodeText = t2tLeg.CauseCodeText,
                        PerCallFeature = t2tLeg.PerCallFeature,
                        PerCallFeatureText = t2tLeg.PerCallFeatureText,
                        AttemptIndicator = t2tLeg.AttemptIndicator,
                        AttemptIndicatorText = t2tLeg.AttemptIndicatorText,
                        InLegConnectTime = t2tLeg.InLegConnectTime,
                        CallAnswerTime = t2tLeg.CallAnswerTime,
                        SourceFile = t2tLeg.SourceFile,
                        SourceLine = t2tLeg.SourceLine,
                    };

                    var t2tInCall = new ProcessedCall
                    {
                        GlobalCallId = call.GlobalCallId,
                        CallDirection = CallDirection.T2TIn,
                        TotalLegs = 1,
                        IsAnswered = call.IsAnswered,
                        TotalDuration = call.TotalDuration,
                        CallerExternal = external1,
                        Extension = ext,
                        DialedNumber = ext,
                        OriginalDialedDigits = originalDnis,
                        HuntGroupNumber = call.HuntGroupNumber,
                        ThreadId = call.ThreadId,
                        Legs = new List<ProcessedLeg> { t2tInLeg },
                    };

                    // T2T-Out: Internal extension -> External destination
                    var t2tOutLeg = new ProcessedLeg
                    {
                        GlobalCallId = t2tLeg.GlobalCallId,
                        ThreadId = t2tLeg.ThreadId,
                        GidSequence = t2tLeg.GidSequence,
                        LegIndex = 2,
                        CallDirection = CallDirection.T2TOut,
                        CallerExtension = ext,
                        CallingNumber = ext,
                        CalledExternal = external2,
                        CalledParty = external2,
                        DestinationExt = "",
                        Extension = ext,
                        DialedNumber = external2,
                        DialedAni = external2,
                        TransferFrom = transferFrom,
                        TransferTo = "",
                        Duration = call.TotalDuration,
                        IsAnswered = call.IsAnswered,
                        IsForwarded = t2tLeg.IsForwarded,
                        HuntGroupNumber = t2tLeg.HuntGroupNumber,
                        CauseCode = t2tLeg.CauseCode,
                        CauseCodeText = t2tLeg.CauseCodeText,
                        PerCallFeature = t2tLeg.PerCallFeature,
                        PerCallFeatureText = t2tLeg.PerCallFeatureText,
                        AttemptIndicator = t2tLeg.AttemptIndicator,
                        AttemptIndicatorText = t2tLeg.AttemptIndicatorText,
                        InLegConnectTime = t2tLeg.InLegConnectTime,
                        CallAnswerTime = t2tLeg.CallAnswerTime,
                        SourceFile = t2tLeg.SourceFile,
                        SourceLine = t2tLeg.SourceLine,
                    };

                    var t2tOutCall = new ProcessedCall
                    {
                        GlobalCallId = call.GlobalCallId + "_out",
                        CallDirection = CallDirection.T2TOut,
                        TotalLegs = 1,
                        IsAnswered = call.IsAnswered,
                        TotalDuration = call.TotalDuration,
                        CallerExtension = ext,
                        Extension = ext,
                        DialedNumber = external2,
                        OriginalDialedDigits = originalDnis,
                        HuntGroupNumber = call.HuntGroupNumber,
                        ThreadId = call.ThreadId,
                        Legs = new List<ProcessedLeg> { t2tOutLeg },
                    };

                    _emitCall(t2tInCall, result);
                    _emitCall(t2tOutCall, result);
                    result.TotalCallsIdentified += 2;
                    continue; // Don't add the original T2T call
                }
                else if (call.CallDirection == CallDirection.TrunkToTrunk)
                {
                    var forwardingCandidates = JoinValues(
                        orderedLegs.Select(l => l.ForwardingParty).Distinct(StringComparer.OrdinalIgnoreCase));
                    var callingCandidates = JoinValues(
                        orderedLegs.Select(l => l.CallingNumber).Distinct(StringComparer.OrdinalIgnoreCase));
                    var destinationCandidates = JoinValues(
                        orderedLegs.Select(l => l.DestinationExt).Distinct(StringComparer.OrdinalIgnoreCase));
                    var calledPartyCandidates = JoinValues(
                        orderedLegs.Select(l => l.CalledParty).Distinct(StringComparer.OrdinalIgnoreCase));

                    _context.Logger.Info(
                        "T2T split skipped. ThreadId={0}, GlobalCallId={1}, LegsCount={2}, ForwardingExt={3}, ForwardingCandidates={4}, ExtensionRangeIsEmpty={5}, InternalNumsCount={6}, PstnToPstn={7}, CallerExternal={8}, CallerExtension={9}, CallingCandidates={10}, DestinationCandidates={11}, CalledPartyCandidates={12}",
                        call.ThreadId,
                        call.GlobalCallId,
                        orderedLegs.Count,
                        forwardingExt ?? string.Empty,
                        forwardingCandidates,
                        true,
                        internalNums?.Count ?? 0,
                        pstnToPstn,
                        call.CallerExternal ?? string.Empty,
                        call.CallerExtension ?? string.Empty,
                        callingCandidates,
                        destinationCandidates,
                        calledPartyCandidates);
                }

                _emitCall(call, result);
                result.TotalCallsIdentified++;
            }
        }

        public ProcessedCall AssembleSingleCall(List<ProcessedLeg> legs)
        {
            if (legs == null || legs.Count == 0) return null;

            // Filter out HG-only legs
            legs = legs.Where(l => !l.IsHgOnly).ToList();
            if (legs.Count == 0) return null;

            var orderedLegs = legs
                .OrderBy(l => l.InLegConnectTime ?? "")
                .ThenBy(l => l.SourceLine)
                .ToList();

            for (int i = 0; i < orderedLegs.Count; i++)
                orderedLegs[i].LegIndex = i + 1;

            // Apply standard transformations
            orderedLegs = _legMerger.MergeAttemptAnswerLegs(orderedLegs);

            // Direction aggregation at leg level: for each leg, if SIP endpoints indicate external, update direction
            foreach (var leg in orderedLegs)
            {
                var ingressIsPstn = !string.IsNullOrEmpty(leg.IngressEndpoint) && _context.IsSipPstn(leg.IngressEndpoint);
                var egressIsPstn = !string.IsNullOrEmpty(leg.EgressEndpoint) && _context.IsSipPstn(leg.EgressEndpoint);
                
                // Aggregate direction based on endpoints (most external wins)
                if (ingressIsPstn && egressIsPstn && leg.CallDirection != CallDirection.TrunkToTrunk)
                    leg.CallDirection = CallDirection.TrunkToTrunk;
                else if (egressIsPstn && leg.CallDirection == CallDirection.Internal)
                    leg.CallDirection = CallDirection.Outgoing;
                else if (ingressIsPstn && leg.CallDirection == CallDirection.Internal)
                    leg.CallDirection = CallDirection.Incoming;
            }

            _transferChainResolver.ComputeTransferChain(orderedLegs);

            var preSuppressionFirstDialed = orderedLegs
                .Select(l => l.DialedNumber)
                .FirstOrDefault(d => !string.IsNullOrEmpty(d));
            var preSuppressionFirstDestExt = orderedLegs.First().DestinationExt;
            var preSuppressionCallerLeg = orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.CallerExternal))
                ?? orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.CallerExtension))
                ?? orderedLegs.First();
            var preSuppressionCallerDirection = preSuppressionCallerLeg.CallDirection;
                // Direction aggregation: if ANY leg has external direction, use that
                // Priority: T2T > Outgoing > Incoming > Internal (most external wins)
                var externalLeg = orderedLegs.FirstOrDefault(l =>
                    l.CallDirection == CallDirection.TrunkToTrunk ||
                    l.CallDirection == CallDirection.Outgoing ||
                    l.CallDirection == CallDirection.Incoming);
                if (externalLeg != null)
                {
                    preSuppressionCallerDirection = externalLeg.CallDirection;
                }

            _legSuppressor.SuppressCmsLegs(orderedLegs);

            if (orderedLegs.Count == 0) return null;

            var firstLeg = orderedLegs.First();
            var answeredLeg = orderedLegs.LastOrDefault(l => l.IsAnswered);
            var callerLeg = preSuppressionCallerLeg;
            var dialedLeg = orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.DialedNumber)) ?? firstLeg;

            var call = new ProcessedCall
            {
                GlobalCallId = firstLeg.GlobalCallId,
                TotalLegs = orderedLegs.Count,
                IsAnswered = orderedLegs.Any(l => l.IsAnswered),
                TotalDuration = orderedLegs.Where(l => l.IsAnswered).DefaultIfEmpty().Max(l => l?.Duration ?? 0),
                CallerExtension = callerLeg.CallerExtension,
                CallerExternal = callerLeg.CallerExternal,
                DialedNumber = dialedLeg.DialedNumber,
                OriginalDialedDigits = preSuppressionFirstDialed,
                HuntGroupNumber = orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.HuntGroupNumber))?.HuntGroupNumber,
                ThreadId = firstLeg.ThreadId,
                CallDirection = preSuppressionCallerDirection,
                Legs = orderedLegs,
            };

            // T2T detection at call level
            if (!string.IsNullOrEmpty(call.CallerExternal) && string.IsNullOrEmpty(call.CallerExtension))
            {
                var anyInternalDest = orderedLegs.Any(l =>
                    _context.IsInternalNumber(l.DestinationExt) || _context.IsInternalNumber(l.CalledExtension));
                if (!anyInternalDest)
                    call.CallDirection = CallDirection.TrunkToTrunk;
            }

            // === POST-PROCESSING (shared with AssembleCalls) ===

            // VM legs: adjust DestinationExt based on whether VM actually answered
            foreach (var vmLeg in orderedLegs.Where(l => _context.IsVmLeg(l)))
            {
                if (!vmLeg.IsAnswered && vmLeg.Duration == 0 && !string.IsNullOrEmpty(vmLeg.ForwardingParty))
                {
                    vmLeg.DestinationExt = vmLeg.ForwardingParty;
                    vmLeg.CalledExtension = vmLeg.ForwardingParty;
                }
                else
                {
                    var vmNum = _context.GetVoicemailNumber();
                    if (!string.IsNullOrEmpty(vmNum) && vmLeg.DestinationExt != vmNum)
                    {
                        vmLeg.DestinationExt = vmNum;
                        vmLeg.CalledExtension = vmNum;
                    }
                }
            }

            // Rule: for Internal calls, Dialed = Dest Ext
            foreach (var leg in orderedLegs)
            {
                if (leg.CallDirection == CallDirection.Internal && !string.IsNullOrEmpty(leg.DestinationExt))
                {
                    leg.DialedNumber = leg.DestinationExt;
                }
            }
            // Update call.DialedNumber after Internal override (it may have been empty before)
            if (string.IsNullOrEmpty(call.DialedNumber))
            {
                var updatedDialedLeg = orderedLegs.FirstOrDefault(l => !string.IsNullOrEmpty(l.DialedNumber));
                if (updatedDialedLeg != null) call.DialedNumber = updatedDialedLeg.DialedNumber;
            }

            // Fallback: if OriginalDialedDigits is empty but DialedNumber is set, use DialedNumber
            if (string.IsNullOrEmpty(call.OriginalDialedDigits) && !string.IsNullOrEmpty(call.DialedNumber))
            {
                call.OriginalDialedDigits = call.DialedNumber;
            }

            // Set Extension based on direction
            switch (call.CallDirection)
            {
                case CallDirection.Incoming:
                    call.Extension = preSuppressionFirstDestExt;
                    if (string.IsNullOrEmpty(call.Extension))
                        call.Extension = (answeredLeg ?? orderedLegs.Last()).DestinationExt;
                    break;
                case CallDirection.Outgoing:
                case CallDirection.Internal:
                    call.Extension = call.CallerExtension;
                    break;
                case CallDirection.TrunkToTrunk:
                    call.Extension = orderedLegs
                        .Select(l => l.ForwardingParty)
                        .FirstOrDefault(fp => !string.IsNullOrEmpty(fp))
                        ?? call.CallerExtension;
                    break;
            }

            // Apply shared leg post-processing (direction, DialedAni, Extension/DestExt)
            _callFinalizer.ApplyLegPostProcessing(call, orderedLegs);

            return call;
        }

        private HashSet<string> GetCallers(List<ProcessedLeg> legs)
        {
            var callers = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var leg in legs)
            {
                if (!string.IsNullOrEmpty(leg.CallerExternal))
                    callers.Add(leg.CallerExternal);
                if (!string.IsNullOrEmpty(leg.CallerExtension))
                    callers.Add(leg.CallerExtension);
            }
            return callers;
        }

        private HashSet<string> GetDialedNumbers(List<ProcessedLeg> legs)
        {
            var dialed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var leg in legs)
            {
                if (!string.IsNullOrEmpty(leg.DialedNumber))
                    dialed.Add(leg.DialedNumber);
            }
            return dialed;
        }

        private bool AreWithinTimeWindow(List<ProcessedLeg> groupA, List<ProcessedLeg> groupB, int windowSeconds)
        {
            // Find earliest and latest setup times in each group
            foreach (var legA in groupA)
            {
                foreach (var legB in groupB)
                {
                    if (string.IsNullOrEmpty(legA.InLegConnectTime) || string.IsNullOrEmpty(legB.InLegConnectTime))
                        continue;

                    // Simple string comparison works for ISO timestamps
                    var diff = Math.Abs(StringTimeApproxDiffSeconds(legA.InLegConnectTime, legB.InLegConnectTime));
                    if (diff <= windowSeconds)
                        return true;
                }
            }
            return false;
        }

        private double StringTimeApproxDiffSeconds(string timeA, string timeB)
        {
            DateTime dtA;
            DateTime dtB;
            if (DateTime.TryParse(timeA, out dtA) && DateTime.TryParse(timeB, out dtB))
            {
                return (dtA - dtB).TotalSeconds;
            }
            return double.MaxValue;
        }
        private static string JoinValues(IEnumerable<string> values)
        {
            if (values == null) return string.Empty;
            return string.Join("|", values.Where(v => !string.IsNullOrWhiteSpace(v)));
        }
    }
}
