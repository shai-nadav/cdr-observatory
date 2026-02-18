using System;
using System.Collections.Generic;
using System.Linq;
using Pipeline.Components.OSVParser.Cache;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser.Processing
{
    /// <summary>
    /// Direction resolver based on SIP endpoints and PartyId fallback.
    /// Used when extension ranges are not available.
    /// </summary>
    internal sealed class SipEndpointDirectionResolver : IDirectionResolver
    {
        private readonly ISipEndpointResolver _sipResolver;
        private readonly ICacheStore _cache;
        private readonly Func<string, bool> _isInternalNumber;
        private readonly Func<string> _getVoicemailNumber;
        private readonly IProcessorLogger _log;
        private readonly IProcessingTracer _tracer;

        // Track unknown SIP endpoints encountered during processing
        private readonly HashSet<string> _unknownEndpoints = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public SipEndpointDirectionResolver(
            ISipEndpointResolver sipResolver,
            ICacheStore cache,
            Func<string, bool> isInternalNumber,
            Func<string> getVoicemailNumber,
            IProcessorLogger log = null,
            IProcessingTracer tracer = null)
        {
            if (sipResolver == null) throw new ArgumentNullException(nameof(sipResolver));
            if (cache == null) throw new ArgumentNullException(nameof(cache));
            if (isInternalNumber == null) throw new ArgumentNullException(nameof(isInternalNumber));
            if (getVoicemailNumber == null) throw new ArgumentNullException(nameof(getVoicemailNumber));
            _sipResolver = sipResolver;
            _cache = cache;
            _isInternalNumber = isInternalNumber;
            _getVoicemailNumber = getVoicemailNumber;
            _log = log ?? new NullProcessorLogger();
            _tracer = tracer ?? NullProcessingTracer.Instance;
        }

        /// <summary>Unknown SIP endpoints encountered during processing.</summary>
        public IEnumerable<string> UnknownEndpoints => _unknownEndpoints;

        public CallDirection ResolveDirection(RawCdrRecord raw, string threadId, out bool callerIsInternal, out bool destIsInternal)
        {
            ResolveInternalityForEmptyRanges(raw, threadId, out callerIsInternal, out destIsInternal);

            // VM number always treated as internal
            var vmNumber = _getVoicemailNumber();
            var vmOverride = false;
            if ((raw.PerCallFeatureExt & 64) != 0 ||
                (!string.IsNullOrEmpty(vmNumber) &&
                 (raw.CalledParty == vmNumber || raw.DestinationExt == vmNumber)))
            {
                destIsInternal = true;
                vmOverride = true;
            }

            // Silent Monitor legs with empty DestExt and InLegConnectTime present
            var isSilentMonitor = (raw.PerCallFeatureExt & 16384) != 0;
            if (callerIsInternal && string.IsNullOrEmpty(raw.DestinationExt)
                && isSilentMonitor && !string.IsNullOrEmpty(raw.InLegConnectTime))
            {
                _log.Debug("Direction override: SilentMonitor -> Incoming");
                return CallDirection.Incoming;
            }

            CallDirection direction;
            if (callerIsInternal && destIsInternal)
                direction = CallDirection.Internal;
            else if (callerIsInternal && !destIsInternal)
                direction = CallDirection.Outgoing;
            else if (!callerIsInternal && destIsInternal)
                direction = CallDirection.Incoming;
            else if (!callerIsInternal && !destIsInternal)
                direction = CallDirection.TrunkToTrunk;
            else
                direction = CallDirection.Unknown;

            // OrigPartyId overrides
            if (direction == CallDirection.TrunkToTrunk && raw.OrigPartyId == 901
                && !string.IsNullOrEmpty(raw.ForwardingParty) && _isInternalNumber(raw.ForwardingParty))
            {
                direction = CallDirection.Incoming;
                _log.Debug("Direction override: OrigPartyId=901 + ForwardingParty internal -> Incoming");
            }
            else if (direction == CallDirection.Unknown)
            {
                if (raw.OrigPartyId == 901 && !callerIsInternal)
                {
                    direction = CallDirection.Incoming;
                    _log.Debug("Direction override: OrigPartyId=901 + external caller -> Incoming");
                }
                else if (raw.OrigPartyId == 900)
                {
                    direction = CallDirection.Internal;
                    _log.Debug("Direction override: OrigPartyId=900 -> Internal");
                }
            }

            _log.Debug($"Direction resolved: {direction}. CallerInternal={callerIsInternal}, DestInternal={destIsInternal}, VmOverride={vmOverride}, OrigPartyId={raw.OrigPartyId}, TermPartyId={raw.TermPartyId}, GidSequence={raw.GidSequence}");

            return direction;
        }

        public void AssignCallerCalledFields(ProcessedLeg leg, RawCdrRecord raw, CallDirection direction, bool callerIsInternal, bool destIsInternal)
        {
            if (callerIsInternal)
            {
                leg.CallerExtension = raw.CallingNumber;
            }
            else
            {
                leg.CallerExternal = raw.CallingNumber;
                if (direction == CallDirection.TrunkToTrunk &&
                    !string.IsNullOrEmpty(raw.ForwardingParty) &&
                    _isInternalNumber(raw.ForwardingParty))
                {
                    leg.CallerExtension = raw.ForwardingParty;
                    _log.Debug($"Caller extension overridden from ForwardingParty for T2T: {raw.ForwardingParty}");
                }
            }

            if (destIsInternal)
            {
                leg.CalledExtension = raw.DestinationExt;
            }
            else
            {
                leg.CalledExternal = !string.IsNullOrEmpty(raw.DestinationExt) ? raw.DestinationExt : raw.CalledParty;
            }
        }

        private string NormalizeEndpoint(string endpoint)
        {
            if (string.IsNullOrWhiteSpace(endpoint)) return null;
            var e = endpoint.Trim();
            if (e.Contains(","))
            {
                var parts = e.Split(',');
                e = parts[parts.Length - 1].Trim();
            }
            var lastColon = e.LastIndexOf(':');
            if (lastColon > -1 && lastColon < e.Length - 1)
            {
                var port = e.Substring(lastColon + 1);
                var isPort = port.All(char.IsDigit);
                if (isPort && e.Count(c => c == ':') == 1)
                {
                    e = e.Substring(0, lastColon);
                }
            }
            return e;
        }

        private bool IsSipPstn(string endpoint)
        {
            var e = NormalizeEndpoint(endpoint);
            if (string.IsNullOrEmpty(e)) return false;
            return _sipResolver.IsPstn(e);
        }

        private bool IsSipKnown(string endpoint)
        {
            var e = NormalizeEndpoint(endpoint);
            if (string.IsNullOrEmpty(e)) return false;
            var known = _sipResolver.IsKnown(e);
            if (!known && _sipResolver.IsLoaded)
                _unknownEndpoints.Add(e);
            return known;
        }

        private bool ResolveSideInternalFromSipOrParty(string endpoint, int partyId, bool isCaller)
        {
            if (IsSipKnown(endpoint))
                return !IsSipPstn(endpoint);

            if (isCaller)
            {
                if (partyId == 900) return true;  // OpenScape internal
                if (partyId == 901) return false; // PSTN
            }
            else
            {
                if (partyId == 902) return true;  // OpenScape internal
                if (partyId == 901) return false; // PSTN
            }

            return true; // conservative default
        }

        private void ResolveInternalityForEmptyRanges(RawCdrRecord raw, string threadId,
            out bool callerIsInternal, out bool destIsInternal)
        {
            var ingressKnown = IsSipKnown(raw.IngressEndpoint);
            var egressKnown = IsSipKnown(raw.EgressEndpoint);

            callerIsInternal = ResolveSideInternalFromSipOrParty(raw.IngressEndpoint, raw.OrigPartyId, isCaller: true);
            destIsInternal = ResolveSideInternalFromSipOrParty(raw.EgressEndpoint, raw.TermPartyId, isCaller: false);

            // Trace SIP classification for ingress
            if (!string.IsNullOrEmpty(raw.IngressEndpoint))
            {
                var ingressNorm = NormalizeEndpoint(raw.IngressEndpoint);
                _tracer.TraceSipClassification(
                    ingressNorm,
                    ingressKnown ? (IsSipPstn(raw.IngressEndpoint) ? "PSTN" : "Internal") : "Unknown",
                    ingressKnown ? "SipMapper" : string.Format("PartyId fallback (OrigPartyId={0})", raw.OrigPartyId));
            }
            
            // Trace SIP classification for egress
            if (!string.IsNullOrEmpty(raw.EgressEndpoint))
            {
                var egressNorm = NormalizeEndpoint(raw.EgressEndpoint);
                _tracer.TraceSipClassification(
                    egressNorm,
                    egressKnown ? (IsSipPstn(raw.EgressEndpoint) ? "PSTN" : "Internal") : "Unknown",
                    egressKnown ? "SipMapper" : string.Format("PartyId fallback (TermPartyId={0})", raw.TermPartyId));
            }

            // If both endpoints are unknown, try thread context
            if (!ingressKnown && !egressKnown)
            {
                var priorLegs = _cache.GetPendingLegs(threadId);
                if (priorLegs.Count > 0)
                {
                    if (priorLegs.Any(l => l.CallDirection == CallDirection.Incoming))
                    {
                        callerIsInternal = false;
                        destIsInternal = true;
                    }
                    else if (priorLegs.Any(l => l.CallDirection == CallDirection.Outgoing))
                    {
                        callerIsInternal = true;
                        destIsInternal = false;
                    }
                    else if (priorLegs.Any(l => l.CallDirection == CallDirection.Internal))
                    {
                        callerIsInternal = true;
                        destIsInternal = true;
                    }
                    else if (priorLegs.Any(l => l.CallDirection == CallDirection.TrunkToTrunk))
                    {
                        callerIsInternal = false;
                        destIsInternal = false;
                    }
                }

                _tracer.TraceSipClassification(
                    string.Format("{0}+{1}", raw.IngressEndpoint ?? "null", raw.EgressEndpoint ?? "null"),
                    string.Format("CallerInternal={0} DestInternal={1}", callerIsInternal, destIsInternal),
                    string.Format("Thread context from {0} prior legs", priorLegs.Count));
            }
        }
    }
}



