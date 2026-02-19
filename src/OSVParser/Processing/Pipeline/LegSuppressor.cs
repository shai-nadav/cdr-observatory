using System;
using System.Collections.Generic;
using System.Linq;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser.Processing.Pipeline
{
    internal sealed class LegSuppressor
    {
        private readonly PipelineContext _context;

        public LegSuppressor(PipelineContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Suppress routing-only legs from the output.
        /// Routing numbers (CMS, pilot, etc.) that act as pure pass-throughs
        /// should not appear as call records. Info is preserved in Transfer From/To.
        ///
        /// Must be called AFTER ComputeTransferChain (so CMS numbers are already
        /// in the transfer fields) and BEFORE computing call-level fields.
        /// </summary>
        public void SuppressCmsLegs(List<ProcessedLeg> orderedLegs)
        {
            if (_context.RoutingNumbers.Count == 0 && _context.DetectedRoutingNumbers.Count == 0) return;

            // Identify CMS-routing leg indices
            var cmsIndices = new HashSet<int>();
            for (int i = 0; i < orderedLegs.Count; i++)
            {
                if (IsRoutingOnlyLeg(orderedLegs[i]))
                {
                    cmsIndices.Add(i);
                    var suppLeg = orderedLegs[i];
                    _context.Tracer.TraceSuppressedLeg(
                        suppLeg.ThreadId,
                        string.Format("Leg#{0} DestExt={1} CallingNum={2} Duration={3} Line={4}",
                            i + 1, suppLeg.DestinationExt ?? "", suppLeg.CallingNumber ?? "",
                            suppLeg.Duration, suppLeg.SourceLine),
                        string.Format("Routing-only: {0}",
                            _context.IsRoutingNumber(suppLeg.DestinationExt) ? "DestinationExt is routing number" : "CallingNumber is routing number + no forwarding"));
                }
            }

            // Answered CMS legs (dur>0) are real calls where user talked to CMS
            // -- keep them as-is with CMS as DestinationExt.

            if (cmsIndices.Count == 0) return;

            _context.Logger.Debug($"Suppressing {cmsIndices.Count} CMS-routing legs");

            // Update adjacent legs' Transfer From/To to bridge over suppressed CMS legs
            foreach (var idx in cmsIndices)
            {
                var cmsLeg = orderedLegs[idx];
                // CMS routing number: prefer DestinationExt, fall back to CallingNumber
                var cmsNumber = !string.IsNullOrEmpty(cmsLeg.DestinationExt)
                    ? cmsLeg.DestinationExt
                    : cmsLeg.CallingNumber;
                // The destination the CMS leg was routing to
                var cmsTarget = !string.IsNullOrEmpty(cmsLeg.CalledParty) && !_context.IsRoutingNumber(cmsLeg.CalledParty)
                    ? cmsLeg.CalledParty
                    : cmsLeg.DestinationExt;

                // Find previous non-CMS leg
                int prevIdx = -1;
                for (int i = idx - 1; i >= 0; i--)
                {
                    if (!cmsIndices.Contains(i)) { prevIdx = i; break; }
                }

                // Find next non-CMS leg
                int nextIdx = -1;
                for (int i = idx + 1; i < orderedLegs.Count; i++)
                {
                    if (!cmsIndices.Contains(i)) { nextIdx = i; break; }
                }

                // Previous leg's TransferTo: point to the destination the CMS was routing to
                if (prevIdx >= 0 && !string.IsNullOrEmpty(cmsTarget))
                {
                    orderedLegs[prevIdx].TransferTo = cmsTarget;
                }

                // Next leg's TransferFrom: set to CMS number if not already set
                // (shows "forwarded from CMS" in the routing chain)
                if (nextIdx >= 0)
                {
                    var nextLeg = orderedLegs[nextIdx];
                    if (string.IsNullOrEmpty(nextLeg.TransferFrom))
                    {
                        nextLeg.TransferFrom = cmsNumber;
                    }
                    // Propagate DialedNumber from suppressed CMS leg's target
                    if (string.IsNullOrEmpty(nextLeg.DialedNumber) && !string.IsNullOrEmpty(cmsTarget))
                    {
                        nextLeg.DialedNumber = cmsTarget;
                    }
                    // FIX: For Internal calls, also populate CalledExtension (Dest Ext) from CMS target
                    // Every Internal call must have Dest Ext populated
                    if (string.IsNullOrEmpty(nextLeg.CalledExtension) && !string.IsNullOrEmpty(cmsTarget)
                        && _context.IsInternalNumber(cmsTarget))
                    {
                        nextLeg.CalledExtension = cmsTarget;
                    }
                }
            }

            // Collect HG numbers from legs about to be suppressed
            var suppressedHGs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var idx in cmsIndices)
            {
                var hg = orderedLegs[idx].HuntGroupNumber;
                if (!string.IsNullOrEmpty(hg))
                    suppressedHGs.Add(hg);
            }

            // Capture original caller before suppression (first non-CMS leg's caller,
            // or first leg's caller if all are CMS)
            string originalCallerExt = null;
            for (int i = 0; i < orderedLegs.Count; i++)
            {
                if (!cmsIndices.Contains(i) && !string.IsNullOrEmpty(orderedLegs[i].CallerExtension)
                    && !_context.IsRoutingNumber(orderedLegs[i].CallerExtension))
                {
                    originalCallerExt = orderedLegs[i].CallerExtension;
                    break;
                }
            }
            // Fallback: if no non-CMS leg found, use first leg's caller if it's not a routing number
            if (string.IsNullOrEmpty(originalCallerExt) && orderedLegs.Count > 0
                && !string.IsNullOrEmpty(orderedLegs[0].CallerExtension)
                && !_context.IsRoutingNumber(orderedLegs[0].CallerExtension))
            {
                originalCallerExt = orderedLegs[0].CallerExtension;
            }

            // Direction aggregation: capture most external direction from legs about to be suppressed
            var dirPriority = new Dictionary<CallDirection, int>
            {
                { CallDirection.TrunkToTrunk, 4 },
                { CallDirection.Outgoing, 3 },
                { CallDirection.Incoming, 2 },
                { CallDirection.Internal, 1 },
                { CallDirection.Unknown, 0 }
            };
            CallDirection mostExternalDir = CallDirection.Internal;
            int maxPri = 1;
            foreach (var idx in cmsIndices)
            {
                var dir = orderedLegs[idx].CallDirection;
                int p;
                var pri = dirPriority.TryGetValue(dir, out p) ? p : 0;
                if (pri > maxPri) { maxPri = pri; mostExternalDir = dir; }
            }

            // Remove CMS legs (iterate in reverse to preserve indices)
            for (int i = orderedLegs.Count - 1; i >= 0; i--)
            {
                if (cmsIndices.Contains(i))
                    orderedLegs.RemoveAt(i);
            }

            // Propagate external direction from suppressed legs to remaining legs
            if (maxPri > 1)
            {
                foreach (var leg in orderedLegs)
                {
                    int lp;
                    var legPri = dirPriority.TryGetValue(leg.CallDirection, out lp) ? lp : 0;
                    if (legPri < maxPri)
                        leg.CallDirection = mostExternalDir;
                }
            }

            // Propagate HG numbers from suppressed CMS legs to remaining legs.
            // Do this BEFORE overwriting CallingNumber with original caller.
            if (suppressedHGs.Count > 0)
            {
                var hgNumber = suppressedHGs.First();
                foreach (var leg in orderedLegs)
                {
                    if (string.IsNullOrEmpty(leg.HuntGroupNumber)
                        && _context.IsRoutingNumber(leg.CallingNumber))
                        leg.HuntGroupNumber = hgNumber;
                }
            }

            // Propagate original caller to remaining legs that have routing numbers as caller
            if (!string.IsNullOrEmpty(originalCallerExt))
            {
                foreach (var leg in orderedLegs)
                {
                    if (_context.IsRoutingNumber(leg.CallerExtension) || _context.IsRoutingNumber(leg.CallingNumber))
                    {
                        leg.CallerExtension = originalCallerExt;
                        leg.CallingNumber = originalCallerExt;
                    }
                }
            }

            // Re-number remaining legs
            for (int i = 0; i < orderedLegs.Count; i++)
            {
                orderedLegs[i].LegIndex = i + 1;
            }

        }

        /// <summary>
        /// True when a leg is a routing pass-through that should be suppressed:
        ///   - Destination is a routing number AND duration = 0 (unanswered routing leg), OR
        ///   - CallingNumber is a routing number AND duration = 0 AND not answered
        ///     AND destination is also routing or empty with no forwarding
        ///     (CMS outgoing "setup" leg that initiates a transfer, NOT the CMS
        ///     agent's actual outbound call to a real extension/VM)
        /// Answered routing legs (dur>0) are real calls and are kept.
        /// </summary>
        private bool IsRoutingOnlyLeg(ProcessedLeg leg)
        {
            if (leg.Duration == 0)
            {
                if (_context.IsRoutingNumber(leg.DestinationExt))
                    return true;
                // CallingNumber is routing: only suppress if it's a pure setup leg
                // (e.g. CMSextension with no talk). Don't suppress CMS outbound calls
                // to real destinations (they have ForwardingParty or non-routing CalledParty
                // with actual routing info like DialedNumber).
                if (_context.IsRoutingNumber(leg.CallingNumber) && !leg.IsAnswered
                    && string.IsNullOrEmpty(leg.ForwardingParty)
                    && (string.IsNullOrEmpty(leg.DestinationExt) || _context.IsRoutingNumber(leg.DestinationExt)))
                    return true;
            }
            return false;
        }

        /// <summary>
        /// True when CalledParty is a routing intermediary (HG, queue, etc.) rather
        /// than the call endpoint.
        ///
        /// General field conditions (no hardcoded numbers):
        ///   - CalledParty is non-empty
        ///   - CalledParty differs from CallingNumber (it's not the caller looping back)
        ///   - CalledParty differs from DestinationExt (it's not the final endpoint)
        ///   - DestinationExt is non-empty (if DestExt is empty, CalledParty may BE the
        ///     destination  e.g. CMS agent legs where the PBX doesn't populate DestExt)
        ///   - CalledParty is not the VM code (VM is a service code, not a routing entity)
        /// </summary>
        private bool IsRoutingIntermediary(ProcessedLeg leg)
        {
            return !string.IsNullOrEmpty(leg.CalledParty)
                && !string.IsNullOrEmpty(leg.DestinationExt)
                && leg.CalledParty != leg.CallingNumber
                && leg.CalledParty != leg.DestinationExt
                && !_context.IsVmLeg(leg);
        }
    }
}
