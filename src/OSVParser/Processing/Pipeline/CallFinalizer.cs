using System;
using System.Collections.Generic;
using System.Linq;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser.Processing.Pipeline
{
    internal sealed class CallFinalizer
    {
        private readonly PipelineContext _context;

        public CallFinalizer(PipelineContext context)
        {
            _context = context;
        }

        public void ApplyLegPostProcessing(ProcessedCall call, List<ProcessedLeg> orderedLegs)
        {
            // Direction propagation: all legs inherit call-level direction for external calls
            if (call.CallDirection == CallDirection.Incoming
                || call.CallDirection == CallDirection.Outgoing
                || call.CallDirection == CallDirection.TrunkToTrunk)
            {
                foreach (var leg in orderedLegs)
                {
                    leg.CallDirection = call.CallDirection;
                }
            }

            // Compute DialedAni: the external number for the call
            HashSet<string> internalNumsForCaller = null;
            if (_context.ExtensionRange.IsEmpty)
            {
                internalNumsForCaller = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var l in orderedLegs)
                {
                    if (l.OrigPartyId == 900 && !string.IsNullOrEmpty(l.CallingNumber))
                    {
                        internalNumsForCaller.Add(l.CallingNumber);
                    }

                    if (l.TermPartyId == 902)
                    {
                        if (!string.IsNullOrEmpty(l.CalledParty)) internalNumsForCaller.Add(l.CalledParty);
                        if (!string.IsNullOrEmpty(l.DestinationExt)) internalNumsForCaller.Add(l.DestinationExt);
                        if (!string.IsNullOrEmpty(l.CalledExtension)) internalNumsForCaller.Add(l.CalledExtension);
                        if (!string.IsNullOrEmpty(l.ForwardingParty)) internalNumsForCaller.Add(l.ForwardingParty);
                    }
                }
            }

            var excludedCallerNums = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (internalNumsForCaller != null)
            {
                foreach (var n in internalNumsForCaller) excludedCallerNums.Add(n);
            }
            foreach (var l in orderedLegs)
            {
                if (!string.IsNullOrEmpty(l.DestinationExt)) excludedCallerNums.Add(l.DestinationExt);
                if (!string.IsNullOrEmpty(l.CalledParty)) excludedCallerNums.Add(l.CalledParty);
                if (!string.IsNullOrEmpty(l.ForwardingParty)) excludedCallerNums.Add(l.ForwardingParty);
            }

            var externalCaller = orderedLegs
                .Where(l => l.OrigPartyId == 901 && !string.IsNullOrEmpty(l.CallerExternal)
                    && !excludedCallerNums.Contains(l.CallerExternal)
                    && !_context.IsRoutingNumber(l.CallerExternal))
                .Select(l => l.CallerExternal)
                .FirstOrDefault()
                ?? orderedLegs.Select(l => l.CallerExternal)
                    .FirstOrDefault(e => !string.IsNullOrEmpty(e)
                        && !excludedCallerNums.Contains(e)
                        && !_context.IsRoutingNumber(e))
                    ?? "";
            var externalDest = orderedLegs
                .Select(l => l.CalledExternal)
                .FirstOrDefault(e => !string.IsNullOrEmpty(e)) ?? "";

            foreach (var leg in orderedLegs)
            {
                switch (call.CallDirection)
                {
                    case CallDirection.Incoming:
                        leg.DialedAni = externalCaller;
                        break;
                    case CallDirection.Outgoing:
                    case CallDirection.TrunkToTrunk:
                        leg.DialedAni = !string.IsNullOrEmpty(externalDest) ? externalDest : leg.DialedNumber;
                        break;
                    default:
                        leg.DialedAni = leg.DialedNumber;
                        break;
                }
            }

            // Extension/DestExt field handling
            foreach (var leg in orderedLegs)
            {
                if (call.CallDirection == CallDirection.Outgoing)
                {
                    leg.Extension = leg.CallerExtension;
                    leg.DestinationExt = "";
                }
                else
                if (call.CallDirection == CallDirection.Incoming
                    || call.CallDirection == CallDirection.Outgoing
                    || call.CallDirection == CallDirection.TrunkToTrunk
                    || call.CallDirection == CallDirection.T2TIn
                    || call.CallDirection == CallDirection.T2TOut)
                {
                    // Non-Internal: Extension = DestinationExt, then clear DestExt
                    leg.Extension = !string.IsNullOrEmpty(leg.DestinationExt)
                        ? leg.DestinationExt
                        : (leg.CalledParty ?? "");
                    leg.DestinationExt = "";
                }
                else
                {
                    // Internal: Extension = calling extension
                    leg.Extension = call.CallerExtension ?? "";
                    // Internal: DestExt should show destination - fallback to CalledParty if empty
                    if (string.IsNullOrEmpty(leg.DestinationExt) && !string.IsNullOrEmpty(leg.CalledParty))
                    {
                        leg.DestinationExt = leg.CalledParty;
                    }
                }

                if (leg.IsPickup && !string.IsNullOrEmpty(leg.TransferFrom) )
                {
                    _context.Logger.Debug($"Clear TransferFrom for pickup call");
                    leg.TransferFrom = "";
                }
            }

           
        }
    }
}
