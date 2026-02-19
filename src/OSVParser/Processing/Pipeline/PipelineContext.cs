using System;
using System.Collections.Generic;
using Pipeline.Components.OSVParser.Cache;
using Pipeline.Components.OSVParser.Config;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser.Processing.Pipeline
{
    /// <summary>
    /// Shared context passed to pipeline stages. Holds references to engine state
    /// and helper methods without duplicating logic.
    /// </summary>
    internal class PipelineContext
    {
        public IProcessorLogger Logger { get; }
        public IProcessingTracer Tracer { get; }
        public ISipEndpointResolver SipResolver { get; }
        public DirectionResolver DirectionResolver { get; }
        public ICacheStore Cache { get; }

        // Number sets
        public HashSet<string> RoutingNumbers { get; }
        public HashSet<string> HuntGroupNumbers { get; }
        public HashSet<string> DetectedRoutingNumbers { get; }

        // GID mappings
        public Dictionary<string, string> GidHexToThreadId { get; }
        public Dictionary<string, string> GidHexToFullGid { get; }

        // Extension tracking
        public HashSet<string> UnknownSipEndpoints { get; }

        // Delegates to engine helper methods (avoids duplicating logic)
        public Func<string, bool> IsInternalNumber { get; }
        public Func<string> GetVoicemailNumber { get; }
        public Func<string, bool> IsRoutingNumber { get; }
        public Func<string, bool> IsHuntGroupNumber { get; }
        public Func<ProcessedLeg, bool> IsVmLeg { get; }
        public Func<string, bool> IsSipPstn { get; }
        public Func<string, bool> IsSipKnown { get; }
        public Func<string, string> NormalizeEndpoint { get; }
        public Func<ProcessedLeg, bool> IsInternalDestForEmptyRanges { get; }

        // Mutable voicemail number (detected at runtime)
        public Func<string> GetDetectedVoicemailNumber { get; }
        public Action<string> SetDetectedVoicemailNumber { get; }

        public PipelineContext(
            IProcessorLogger logger,
            IProcessingTracer tracer,
            ISipEndpointResolver sipResolver,
            DirectionResolver directionResolver,
            ICacheStore cache,
            HashSet<string> routingNumbers,
            HashSet<string> huntGroupNumbers,
            HashSet<string> detectedRoutingNumbers,
            Dictionary<string, string> gidHexToThreadId,
            Dictionary<string, string> gidHexToFullGid,
            HashSet<string> unknownSipEndpoints,
            Func<string, bool> isInternalNumber,
            Func<string> getVoicemailNumber,
            Func<string, bool> isRoutingNumber,
            Func<string, bool> isHuntGroupNumber,
            Func<ProcessedLeg, bool> isVmLeg,
            Func<string, bool> isSipPstn,
            Func<string, bool> isSipKnown,
            Func<string, string> normalizeEndpoint,
            Func<ProcessedLeg, bool> isInternalDestForEmptyRanges,
            Func<string> getDetectedVoicemailNumber,
            Action<string> setDetectedVoicemailNumber)
        {
            Logger = logger;
            Tracer = tracer;
            SipResolver = sipResolver;
            DirectionResolver = directionResolver;
            Cache = cache;
            RoutingNumbers = routingNumbers;
            HuntGroupNumbers = huntGroupNumbers;
            DetectedRoutingNumbers = detectedRoutingNumbers;
            GidHexToThreadId = gidHexToThreadId;
            GidHexToFullGid = gidHexToFullGid;
            UnknownSipEndpoints = unknownSipEndpoints;
            IsInternalNumber = isInternalNumber;
            GetVoicemailNumber = getVoicemailNumber;
            IsRoutingNumber = isRoutingNumber;
            IsHuntGroupNumber = isHuntGroupNumber;
            IsVmLeg = isVmLeg;
            IsSipPstn = isSipPstn;
            IsSipKnown = isSipKnown;
            NormalizeEndpoint = normalizeEndpoint;
            IsInternalDestForEmptyRanges = isInternalDestForEmptyRanges;
            GetDetectedVoicemailNumber = getDetectedVoicemailNumber;
            SetDetectedVoicemailNumber = setDetectedVoicemailNumber;
        }
    }
}
