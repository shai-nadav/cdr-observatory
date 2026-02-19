using System.Collections.Generic;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser.Processing.Pipeline
{
    /// <summary>
    /// Computes Transfer From / Transfer To for all legs in a call.
    /// </summary>
    internal class TransferChainResolver
    {
        private readonly PipelineContext _ctx;

        public TransferChainResolver(PipelineContext ctx)
        {
            _ctx = ctx;
        }

        /// <summary>
        /// True when CalledParty is a routing intermediary (HG, queue, etc.) rather
        /// than the call endpoint.
        /// </summary>
        private bool IsRoutingIntermediary(ProcessedLeg leg)
        {
            return !string.IsNullOrEmpty(leg.CalledParty)
                && !string.IsNullOrEmpty(leg.DestinationExt)
                && leg.CalledParty != leg.CallingNumber
                && leg.CalledParty != leg.DestinationExt
                && !_ctx.IsVmLeg(leg);
        }

        /// <summary>
        /// Compute Transfer From on a single leg (without inheritance).
        /// </summary>
        public string ComputeLegTransferFrom(ProcessedLeg leg, string origCaller)
        {
            // Rule 1: CalledParty is a routing intermediary
            if (IsRoutingIntermediary(leg))
                return leg.CalledParty;

            // Rule 2: Has forwarding party
            if (!string.IsNullOrEmpty(leg.ForwardingParty)
                && !(_ctx.IsVmLeg(leg) && !leg.IsAnswered && leg.Duration == 0))
                return leg.ForwardingParty;

            // Rule 3: CallingNumber differs from original caller
            if (!string.IsNullOrEmpty(leg.CallingNumber) &&
                !string.IsNullOrEmpty(origCaller) &&
                leg.CallingNumber != origCaller)
                return leg.CallingNumber;

            // Rule 4: CalledParty differs from DestinationExt and not VM
            if (!string.IsNullOrEmpty(leg.CalledParty) &&
                !string.IsNullOrEmpty(leg.DestinationExt) &&
                leg.CalledParty != leg.DestinationExt &&
                !_ctx.IsVmLeg(leg))
                return leg.CalledParty;

            return null;
        }

        /// <summary>
        /// Compute Transfer From / Transfer To for all legs in a call.
        /// </summary>
        public void ComputeTransferChain(List<ProcessedLeg> orderedLegs)
        {
            if (orderedLegs.Count == 0) return;

            var origCaller = orderedLegs[0].CallingNumber ?? "";
            string prevTransferFrom = null;

            for (int i = 0; i < orderedLegs.Count; i++)
            {
                var leg = orderedLegs[i];
                var nextLeg = (i + 1 < orderedLegs.Count) ? orderedLegs[i + 1] : null;

                // === Transfer From ===
                var xferFrom = ComputeLegTransferFrom(leg, origCaller);

                if (string.IsNullOrEmpty(xferFrom) && prevTransferFrom != null)
                {
                    xferFrom = prevTransferFrom;
                }

                // === Transfer To ===
                string xferTo = null;

                if (_ctx.IsVmLeg(leg) && leg.IsAnswered)
                {
                    xferTo = null;
                }
                else if (_ctx.IsVmLeg(leg) && !leg.IsAnswered && nextLeg != null && _ctx.IsVmLeg(nextLeg))
                {
                    xferTo = nextLeg.CalledParty;
                }
                else if (nextLeg != null)
                {
                    if (_ctx.IsVmLeg(nextLeg))
                    {
                        xferTo = nextLeg.CalledParty;
                    }
                    else
                    {
                        var nextXferFrom = ComputeLegTransferFrom(nextLeg, origCaller);

                        xferTo = !string.IsNullOrEmpty(nextXferFrom) ? nextXferFrom
                               : !string.IsNullOrEmpty(nextLeg.DestinationExt) ? nextLeg.DestinationExt
                               : nextLeg.CalledParty;

                        if (xferTo == xferFrom)
                        {
                            xferTo = !string.IsNullOrEmpty(nextLeg.DestinationExt) ? nextLeg.DestinationExt
                                   : nextLeg.CalledParty;
                        }

                        if (!string.IsNullOrEmpty(leg.DestinationExt) && xferTo == leg.DestinationExt)
                        {
                            xferTo = !string.IsNullOrEmpty(nextLeg.DestinationExt) ? nextLeg.DestinationExt
                                   : nextLeg.CalledParty;
                        }
                    }
                }

                leg.TransferFrom = xferFrom;
                leg.TransferTo = xferTo;
                prevTransferFrom = xferFrom;

                // Determine which rule produced TransferFrom
                string fromRule = "None";
                if (!string.IsNullOrEmpty(xferFrom))
                {
                    if (IsRoutingIntermediary(leg)) fromRule = "Rule1: CalledParty is routing intermediary";
                    else if (!string.IsNullOrEmpty(leg.ForwardingParty) && xferFrom == leg.ForwardingParty) fromRule = "Rule2: ForwardingParty";
                    else if (!string.IsNullOrEmpty(leg.CallingNumber) && xferFrom == leg.CallingNumber) fromRule = "Rule3: CallingNumber differs from origCaller";
                    else if (!string.IsNullOrEmpty(leg.CalledParty) && xferFrom == leg.CalledParty) fromRule = "Rule4: CalledParty differs from DestExt";
                    else fromRule = "Inherited from previous leg";
                }
                
                string toRule = "None";
                if (xferTo != null)
                {
                    if (_ctx.IsVmLeg(leg)) toRule = "VM leg";
                    else if (nextLeg != null && _ctx.IsVmLeg(nextLeg)) toRule = "Next leg is VM";
                    else if (nextLeg != null) toRule = "Look-ahead to next leg";
                }
                else if (nextLeg == null) toRule = "Last leg: null";
                
                _ctx.Tracer.TraceTransferChain(leg.ThreadId ?? "", leg.LegIndex, xferFrom, fromRule, xferTo, toRule);
            }
        }
    }
}
