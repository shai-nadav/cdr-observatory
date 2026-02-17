using System;
using Pipeline.Components.OSVParser.Config;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser.Processing
{
    /// <summary>
    /// Direction resolver based on extension ranges.
    /// </summary>
    internal sealed class ExtensionRangeDirectionResolver : IDirectionResolver
    {
        private readonly ExtensionRangeParser _extensionRange;
        private readonly Func<string, bool> _isInternalNumber;
        private readonly Func<string> _getVoicemailNumber;
        private readonly IProcessorLogger _log;

        public ExtensionRangeDirectionResolver(
            ExtensionRangeParser extensionRange,
            Func<string, bool> isInternalNumber,
            Func<string> getVoicemailNumber,
            IProcessorLogger log = null)
        {
            if (extensionRange == null) throw new ArgumentNullException(nameof(extensionRange));
            if (isInternalNumber == null) throw new ArgumentNullException(nameof(isInternalNumber));
            if (getVoicemailNumber == null) throw new ArgumentNullException(nameof(getVoicemailNumber));
            _extensionRange = extensionRange;
            _isInternalNumber = isInternalNumber;
            _getVoicemailNumber = getVoicemailNumber;
            _log = log ?? new NullProcessorLogger();
        }

        public CallDirection ResolveDirection(RawCdrRecord raw, string threadId, out bool callerIsInternal, out bool destIsInternal)
        {
            if (_extensionRange.IsEmpty)
            {
                callerIsInternal = raw.OrigPartyId == 900;
                destIsInternal = raw.TermPartyId == 902;
            }
            else
            {
                callerIsInternal = _isInternalNumber(raw.CallingNumber);

                destIsInternal = _isInternalNumber(raw.DestinationExt) ||
                                 _isInternalNumber(raw.DialedNumber) ||
                                 _isInternalNumber(raw.CalledParty);
            }

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
    }
}
