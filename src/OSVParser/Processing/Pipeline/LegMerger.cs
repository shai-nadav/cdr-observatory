using System.Collections.Generic;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser.Processing.Pipeline
{
    /// <summary>
    /// Merges consecutive attempt+answer legs targeting the same extension.
    /// When a 0-duration unanswered leg is immediately followed by an answered leg
    /// to the same destination extension, they represent the same call attempt
    /// (ring then answer) and should be merged into a single leg.
    /// </summary>
    internal class LegMerger
    {
        private readonly PipelineContext _ctx;

        public LegMerger(PipelineContext ctx)
        {
            _ctx = ctx;
        }

        public List<ProcessedLeg> MergeAttemptAnswerLegs(List<ProcessedLeg> legs)
        {
            if (legs.Count <= 1) return legs;

            var merged = new List<ProcessedLeg>();
            int i = 0;
            while (i < legs.Count)
            {
                if (i + 1 < legs.Count)
                {
                    var current = legs[i];
                    var next = legs[i + 1];

                    // Merge condition: current is attempt (0s, not answered),
                    // next is answered, and both target the same destination extension
                    var currentDest = !string.IsNullOrEmpty(current.DestinationExt)
                        ? current.DestinationExt
                        : current.CalledExtension;
                    var nextDest = !string.IsNullOrEmpty(next.DestinationExt)
                        ? next.DestinationExt
                        : next.CalledExtension;

                    // Don't merge if next leg is VM or has a real forwarding party (not HG/routing)
                    var nextIsVM = _ctx.IsVmLeg(next);
                    var nextFwd = next.ForwardingParty ?? "";
                    // Forwarding from a routing/HG number is not real user forwarding
                    var hasFwdBetween = !string.IsNullOrEmpty(nextFwd)
                        && !_ctx.IsRoutingNumber(nextFwd)
                        && !_ctx.IsHuntGroupNumber(nextFwd);

                    if (current.Duration == 0
                        && !current.IsAnswered
                        && next.IsAnswered
                        && next.Duration > 0
                        && !string.IsNullOrEmpty(currentDest)
                        && !string.IsNullOrEmpty(nextDest)
                        && currentDest == nextDest
                        && !nextIsVM
                        && !hasFwdBetween)
                    {
                        // Merge: keep first leg as base, take answer data from second
                        current.Duration = next.Duration;
                        current.IsAnswered = next.IsAnswered;
                        current.CauseCode = next.CauseCode;
                        current.CauseCodeText = next.CauseCodeText;
                        current.CallAnswerTime = next.CallAnswerTime;
                        current.OutLegReleaseTime = next.OutLegReleaseTime;
                        current.OutLegConnectTime = next.OutLegConnectTime;
                        current.CallReleaseTime = next.CallReleaseTime;

                        // Carry over routing flags from answered leg
                        if (next.IsForwarded) current.IsForwarded = true;
                        if (!string.IsNullOrEmpty(next.ForwardFromExt))
                            current.ForwardFromExt = next.ForwardFromExt;
                        if (!string.IsNullOrEmpty(next.ForwardToExt))
                            current.ForwardToExt = next.ForwardToExt;
                        if (!string.IsNullOrEmpty(next.ForwardingParty))
                            current.ForwardingParty = next.ForwardingParty;
                        if (next.IsPickup) current.IsPickup = true;

                        // Combine source references for traceability
                        current.SourceFile = current.SourceFile == next.SourceFile
                            ? current.SourceFile
                            : $"{current.SourceFile}+{next.SourceFile}";
                        current.SourceLine = current.SourceLine; // keep first leg's line

                        _ctx.Tracer.TraceLegMerge(
                            current.ThreadId,
                            current.SourceLine,
                            next.SourceLine,
                            currentDest,
                            string.Format("Attempt(0s,unanswered)+Answer(dur={0}) to same DestExt={1}", next.Duration, currentDest));

                        // Direction aggregation when merging segments: most external wins
                        var dirPriority = new Dictionary<CallDirection, int>
                        {
                            { CallDirection.TrunkToTrunk, 4 },
                            { CallDirection.Outgoing, 3 },
                            { CallDirection.Incoming, 2 },
                            { CallDirection.Internal, 1 },
                            { CallDirection.Unknown, 0 }
                        };
                        int cp;
                        int np;
                        var currentPri = dirPriority.TryGetValue(current.CallDirection, out cp) ? cp : 0;
                        var nextPri = dirPriority.TryGetValue(next.CallDirection, out np) ? np : 0;
                        if (nextPri > currentPri)
                            current.CallDirection = next.CallDirection;

                        merged.Add(current);
                        i += 2; // skip both legs
                        continue;
                    }
                }

                merged.Add(legs[i]);
                i++;
            }

            // Re-number LegIndex sequentially
            for (int j = 0; j < merged.Count; j++)
            {
                merged[j].LegIndex = j + 1;
            }

            return merged;
        }
    }
}
