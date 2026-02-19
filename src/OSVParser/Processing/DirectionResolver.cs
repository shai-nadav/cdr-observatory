using System;
using Pipeline.Components.OSVParser.Models;
using Pipeline.Components.OSVParser.Cache;

namespace Pipeline.Components.OSVParser.Processing
{
    /// <summary>
    /// Facade: delegates direction resolution to SIP endpoint strategy.
    /// </summary>
    internal sealed class DirectionResolver
    {
        private readonly IDirectionResolver _resolver;

        public DirectionResolver(
            ISipEndpointResolver sipResolver,
            ICacheStore cache,
            Func<string, bool> isInternalNumber,
            Func<string> getVoicemailNumber,
            IProcessorLogger log = null,
            IProcessingTracer tracer = null)
        {
            _resolver = new SipEndpointDirectionResolver(sipResolver, cache, isInternalNumber, getVoicemailNumber, log, tracer);
            log?.Info("DirectionResolver using SipEndpointDirectionResolver");
        }

        public CallDirection ResolveDirection(RawCdrRecord raw, string threadId, out bool callerIsInternal, out bool destIsInternal)
            => _resolver.ResolveDirection(raw, threadId, out callerIsInternal, out destIsInternal);

        public void AssignCallerCalledFields(ProcessedLeg leg, RawCdrRecord raw, CallDirection direction, bool callerIsInternal, bool destIsInternal)
            => _resolver.AssignCallerCalledFields(leg, raw, direction, callerIsInternal, destIsInternal);
    }
}
