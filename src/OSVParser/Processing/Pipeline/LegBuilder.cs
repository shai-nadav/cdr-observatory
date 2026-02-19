using System;
using System.Collections.Generic;
using System.Linq;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser.Processing.Pipeline
{
    internal sealed class LegBuilder
    {
        private readonly PipelineContext _context;
        private readonly Func<string, string, int?> _calculateRingTime;
        private readonly Func<string, string> _getGidHex;
        private readonly Action<string, ProcessingResult> _checkStreamingOutput;
        public LegBuilder(
            PipelineContext context,
            Func<string, string, int?> calculateRingTime,
            Func<string, string> getGidHex,
            Action<string, ProcessingResult> checkStreamingOutput)
        {
            _context = context;
            _calculateRingTime = calculateRingTime;
            _getGidHex = getGidHex;
            _checkStreamingOutput = checkStreamingOutput;
        }

        public void ProcessFullCdr(RawCdrRecord raw, ProcessingResult result)
        {
            // Thread ID groups legs into a single logical call.
            // ThreadIdSequence is the unique per-call sequence number (spec field 125).
            // ThreadIdNode is the node name (spec field 124), not unique per call.
            var threadId = raw.ThreadIdSequence ?? raw.ThreadIdNode ?? raw.GlobalCallId;

            var leg = new ProcessedLeg
            {
                GlobalCallId = raw.GlobalCallId,
                ThreadId = threadId,
                GidSequence = raw.GidSequence,
                DialedNumber = raw.DialedNumber,
                DestinationExt = raw.DestinationExt,
                Duration = raw.Duration,
                CallAnswerTime = raw.CallAnswerTime,
                InLegConnectTime = raw.InLegConnectTime,
                OutLegReleaseTime = raw.OutLegReleaseTime,
                OutLegConnectTime = raw.OutLegConnectTime,
                CallReleaseTime = raw.CallReleaseTime,
                SourceFile = raw.SourceFile,
                SourceLine = raw.SourceLine,
                IngressEndpoint = raw.IngressEndpoint,
                EgressEndpoint = raw.EgressEndpoint,
            };

            // Trace field origins for all significant fields
            _context.Tracer.TraceFieldOrigin(threadId, 0, "DialedNumber", raw.DialedNumber, raw.SourceFile, raw.SourceLine, "spec101");
            _context.Tracer.TraceFieldOrigin(threadId, 0, "DestinationExt", raw.DestinationExt, raw.SourceFile, raw.SourceLine, "spec128");
            _context.Tracer.TraceFieldOrigin(threadId, 0, "IngressEndpoint", raw.IngressEndpoint, raw.SourceFile, raw.SourceLine, "spec126");
            _context.Tracer.TraceFieldOrigin(threadId, 0, "EgressEndpoint", raw.EgressEndpoint, raw.SourceFile, raw.SourceLine, "spec127");
            _context.Tracer.TraceFieldOrigin(threadId, 0, "CallingNumber", raw.CallingNumber, raw.SourceFile, raw.SourceLine, "spec12");
            _context.Tracer.TraceFieldOrigin(threadId, 0, "CalledParty", raw.CalledParty, raw.SourceFile, raw.SourceLine, "spec11");
            _context.Tracer.TraceFieldOrigin(threadId, 0, "ForwardingParty", raw.ForwardingParty, raw.SourceFile, raw.SourceLine, "spec65");

            // Debug: verify endpoint copying
            if (!string.IsNullOrEmpty(raw.IngressEndpoint) || !string.IsNullOrEmpty(raw.EgressEndpoint))
            {
                _context.Logger.Debug($"Created ProcessedLeg with Ingress={leg.IngressEndpoint}, Egress={leg.EgressEndpoint}, GidSequence={leg.GidSequence}");
            }

            // Store raw fields for transfer chain analysis
            leg.CalledParty = raw.CalledParty;
            leg.CallingNumber = raw.CallingNumber;

            // Determine if answered
            leg.IsAnswered = (raw.Duration > 0 && raw.CauseCode == 16)
                || raw.PerCallFeature == 8
                || (raw.MediaType == 1 && raw.CauseCode == 16);

            // Calculate RingTime (seconds between InLegConnectTime and CallAnswerTime)
            leg.RingTime = _calculateRingTime(raw.InLegConnectTime, raw.CallAnswerTime);

            // Store raw forwarding party for transfer chain
            leg.ForwardingParty = raw.ForwardingParty;

            // Determine if forwarded
            leg.IsForwarded = !string.IsNullOrEmpty(raw.ForwardingParty);

            // Determine if pickup
            leg.IsPickup = (raw.CallEventIndicator & 8192) == 8192;

            // Determine if voicemail (CF-to-Voicemail flag bit 64, or destination matches VM number)
            var vmNum = _context.GetVoicemailNumber();
            leg.IsVoicemail = (raw.PerCallFeatureExt & 64) == 64
                || (!string.IsNullOrEmpty(vmNum) && raw.CalledParty == vmNum);

            // Call direction rules:
            // 1) SIP endpoints (if known)
            // 2) PartyId fallback (OrigPartyId/TermPartyId)
            // 3) Thread context if both endpoints unknown
            bool callerIsInternal;
            bool destIsInternal;

            leg.CallDirection = _context.DirectionResolver.ResolveDirection(raw, threadId, out callerIsInternal, out destIsInternal);
            _context.DirectionResolver.AssignCallerCalledFields(leg, raw, leg.CallDirection, callerIsInternal, destIsInternal);

            _context.Tracer.TraceDirectionDecision(
                threadId, leg.LegIndex, leg.CallDirection.ToString(),
                string.Format("SIP/PartyId resolve: Ingress={0} Egress={1}", raw.IngressEndpoint ?? "null", raw.EgressEndpoint ?? "null"),
                callerIsInternal, destIsInternal,
                raw.IngressEndpoint, raw.EgressEndpoint,
                raw.OrigPartyId, raw.TermPartyId);

            if (!string.IsNullOrEmpty(leg.CallerExtension))
                _context.Tracer.TraceFieldOrigin(threadId, 0, "CallerExtension", leg.CallerExtension, raw.SourceFile, raw.SourceLine, "CallingNumber(internal)");
            if (!string.IsNullOrEmpty(leg.CallerExternal))
                _context.Tracer.TraceFieldOrigin(threadId, 0, "CallerExternal", leg.CallerExternal, raw.SourceFile, raw.SourceLine, "CallingNumber(external)");
            if (!string.IsNullOrEmpty(leg.CalledExtension))
                _context.Tracer.TraceFieldOrigin(threadId, 0, "CalledExtension", leg.CalledExtension, raw.SourceFile, raw.SourceLine, "DestinationExt(internal)");
            if (!string.IsNullOrEmpty(leg.CalledExternal))
                _context.Tracer.TraceFieldOrigin(threadId, 0, "CalledExternal", leg.CalledExternal, raw.SourceFile, raw.SourceLine, "DestinationExt(external)");

            // Forward from/to
            if (leg.IsForwarded)
            {
                leg.ForwardFromExt = raw.ForwardingParty;
                leg.ForwardToExt = raw.DestinationExt;
            }

            // Pickup
            if (leg.IsPickup)
            {
                leg.ForwardFromExt = raw.CalledParty;
                leg.ForwardToExt = raw.DestinationExt;
            }

            // Companion text fields
            leg.CauseCode = raw.CauseCode;
            leg.CauseCodeText = FieldMappings.GetReleaseCauseText(raw.CauseCode);
            leg.PerCallFeature = raw.PerCallFeature;
            leg.PerCallFeatureText = FieldMappings.GetPerCallFeatureText(raw.PerCallFeature);
            leg.AttemptIndicator = raw.AttemptIndicator;
            leg.AttemptIndicatorText = FieldMappings.GetAttemptIndicatorText(raw.AttemptIndicator);
            leg.PerCallFeatureExt = raw.PerCallFeatureExt;
            leg.PerCallFeatureExtText = FieldMappings.GetPerCallFeatureExtText(raw.PerCallFeatureExt);
            
            // Auto-detect voicemail number: if bit 64 is set (CF-to-Voicemail) and CalledParty is present
            if ((raw.PerCallFeatureExt & 64) != 0 && !string.IsNullOrEmpty(raw.CalledParty)
                && string.IsNullOrEmpty(_context.GetDetectedVoicemailNumber()))
            {
                _context.SetDetectedVoicemailNumber(raw.CalledParty);
                _context.Logger.Debug($"Auto-detected voicemail number: {_context.GetDetectedVoicemailNumber()}");
                _context.Tracer.TraceSpecialNumber(_context.GetDetectedVoicemailNumber(), "Voicemail", "PerCallFeatureExt bit 64 (CF-to-Voicemail)");
            }
            
            leg.CallEventIndicator = raw.CallEventIndicator;
            leg.CallEventIndicatorText = FieldMappings.GetCallEventIndicatorText(raw.CallEventIndicator);
            leg.OrigPartyId = raw.OrigPartyId;
            leg.OrigPartyIdText = FieldMappings.GetPartyIdText(raw.OrigPartyId);
            leg.TermPartyId = raw.TermPartyId;
            leg.TermPartyIdText = FieldMappings.GetPartyIdText(raw.TermPartyId);

            // Before storing, check if there are HG-only legs for this thread -- merge their info.
            // HG records are stored by GID, CDR legs by ThreadId. Check both keys.
            // GIDs may differ slightly in timestamp between HG and CDR records,
            // so also check by hex suffix.
            var existingLegs = _context.Cache.GetPendingLegs(threadId);
            var gid = raw.GlobalCallId;
            var gidHex = _getGidHex(gid);
            List<ProcessedLeg> gidLegs = new List<ProcessedLeg>();
            string hgOnlyGidKey = null; // track the cache key where HG-only legs are stored

            if (!string.IsNullOrEmpty(gid) && gid != threadId)
            {
                gidLegs = _context.Cache.GetPendingLegs(gid);
                if (gidLegs.Count > 0) hgOnlyGidKey = gid;
            }
            // If exact GID didn't find HG legs, try by hex suffix
            string fullGid;
            if (gidLegs.Count == 0 && !string.IsNullOrEmpty(gidHex)
                && _context.GidHexToFullGid.TryGetValue(gidHex, out fullGid)
                && fullGid != threadId)
            {
                gidLegs = _context.Cache.GetPendingLegs(fullGid);
                if (gidLegs.Count > 0) hgOnlyGidKey = fullGid;
            }

            foreach (var existing in existingLegs.Concat(gidLegs))
            {
                if (existing.IsHgOnly)
                {
                    if (string.IsNullOrEmpty(leg.HuntGroupNumber) && !string.IsNullOrEmpty(existing.HuntGroupNumber))
                    {
                        leg.HuntGroupNumber = existing.HuntGroupNumber;
                    }
                }
            }

            if (!string.IsNullOrEmpty(leg.HuntGroupNumber))
                _context.Tracer.TraceFieldOrigin(threadId, 0, "HuntGroupNumber", leg.HuntGroupNumber, raw.SourceFile, raw.SourceLine, "HG record merge");

            // Remove HG-only legs now that we have a real CDR for this thread
            foreach (var hgLeg in existingLegs.Where(l => l.IsHgOnly).ToList())
            {
                _context.Cache.RemovePendingLeg(threadId, hgLeg.InLegConnectTime);
            }
            if (hgOnlyGidKey != null)
            {
                foreach (var hgLeg in gidLegs.Where(l => l.IsHgOnly).ToList())
                {
                    _context.Cache.RemovePendingLeg(hgOnlyGidKey, hgLeg.InLegConnectTime);
                }
            }

            // Register GID hex  ThreadId mapping for HG records that arrive later
            if (!string.IsNullOrEmpty(gidHex))
            {
                _context.GidHexToThreadId[gidHex] = threadId;
            }

            // Store in cache keyed by Thread ID (groups all legs of the same call)
            _context.Cache.StorePendingLeg(threadId, leg);

            // Streaming: check for early output and enforce cache limit
            _checkStreamingOutput(threadId, result);

            // Candidate extension detection
        }

        public void ProcessHuntGroup(RawCdrRecord raw, ProcessingResult result)
        {
            // Auto-detect HG pilot numbers as routing numbers
            // Auto-detect HG pilot numbers as routing numbers
            if (!string.IsNullOrEmpty(raw.HuntGroupNumber))
            {
                _context.DetectedRoutingNumbers.Add(raw.HuntGroupNumber);
                _context.Tracer.TraceSpecialNumber(raw.HuntGroupNumber, "HuntGroup", "HG record pilot number (auto-detected routing)");
            }

            // HG records supplement the full CDR -- store in cache linked by GID
            var leg = new ProcessedLeg
            {
                GlobalCallId = raw.GlobalCallId,
                HuntGroupNumber = raw.HuntGroupNumber,
                CalledExtension = raw.RoutedToExtension,
                DestinationExt = raw.RoutedToExtension,
                CallDirection = CallDirection.Internal,
                InLegConnectTime = raw.HGStartTime ?? raw.Timestamp,
                SourceFile = raw.SourceFile,
                SourceLine = raw.SourceLine,
                IngressEndpoint = raw.IngressEndpoint,
                EgressEndpoint = raw.EgressEndpoint,
            };

            // Try to merge HG info into existing legs.
            // CDR legs are stored by ThreadId, but HG records only have GID.
            // Use the GID hex suffix  ThreadId mapping (timestamps can drift 0.1s).
            var gidHex = _getGidHex(raw.GlobalCallId);
            var existingLegs = _context.Cache.GetPendingLegs(raw.GlobalCallId);
            string threadId;
            if (existingLegs.Count == 0 && !string.IsNullOrEmpty(gidHex)
                && _context.GidHexToThreadId.TryGetValue(gidHex, out threadId))
            {
                existingLegs = _context.Cache.GetPendingLegs(threadId);
            }

            if (existingLegs.Count > 0)
            {
                foreach (var existing in existingLegs)
                {
                    if (string.IsNullOrEmpty(existing.HuntGroupNumber))
                    {
                        existing.HuntGroupNumber = raw.HuntGroupNumber;
                    }
                }
            }
            else
            {
                // HG record arrived before the full CDR -- store by GID, marked as HG-only
                leg.IsHgOnly = true;
                _context.Cache.StorePendingLeg(raw.GlobalCallId, leg);
                // Register hex mapping so CDRs with slightly different timestamps can find it.
                // Keep the FIRST mapping (parent HG)  don't overwrite with secondary HGs.
                if (!string.IsNullOrEmpty(gidHex) && !_context.GidHexToFullGid.ContainsKey(gidHex))
                {
                    _context.GidHexToFullGid[gidHex] = raw.GlobalCallId;
                }
            }
        }

        public void ProcessCallForward(RawCdrRecord raw, ProcessingResult result)
        {
            // CF records track forward activation -- create a minimal leg
            var leg = new ProcessedLeg
            {
                GlobalCallId = null, // CF records may not have GID
                CallerExtension = raw.OrigExtension,
                ForwardFromExt = raw.OrigExtension,
                ForwardToExt = raw.ForwardDestination,
                IsForwarded = true,
                InLegConnectTime = raw.Timestamp,
                SourceFile = raw.SourceFile,
                SourceLine = raw.SourceLine,
                IngressEndpoint = raw.IngressEndpoint,
                EgressEndpoint = raw.EgressEndpoint,
            };

            // Determine if forward destination is internal or external
            if (_context.IsInternalNumber(raw.ForwardDestination))
            {
                leg.CalledExtension = raw.ForwardDestination;
                leg.CallDirection = CallDirection.Internal;
            }
            else
            {
                leg.CalledExternal = raw.ForwardDestination;
                leg.CallDirection = CallDirection.TrunkToTrunk;
            }

            // CF records don't always have GID -- store only if linkable
            if (!string.IsNullOrEmpty(raw.GlobalCallId))
            {
                _context.Cache.StorePendingLeg(raw.GlobalCallId, leg);
            }
        }
    }
}
