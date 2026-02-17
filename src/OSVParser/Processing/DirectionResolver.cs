using System;
using Pipeline.Components.OSVParser.Config;
using Pipeline.Components.OSVParser.Models;
using Pipeline.Components.OSVParser.Cache;

namespace Pipeline.Components.OSVParser.Processing
{
    /// <summary>
    /// Facade: delegates direction resolution to the appropriate strategy.
    /// </summary>
    internal sealed class DirectionResolver
    {
        private readonly IDirectionResolver _resolver;

        public DirectionResolver(
            ExtensionRangeParser extensionRange,
            ISipEndpointResolver sipResolver,
            ICacheStore cache,
            Func<string, bool> isInternalNumber,
            Func<string> getVoicemailNumber,
            IProcessorLogger log = null,
            IProcessingTracer tracer = null)
        {
            if (extensionRange == null) throw new ArgumentNullException(nameof(extensionRange));

            _resolver = extensionRange.IsEmpty
                ? (IDirectionResolver)new SipEndpointDirectionResolver(sipResolver, cache, isInternalNumber, getVoicemailNumber, log, tracer)
                : new ExtensionRangeDirectionResolver(extensionRange, isInternalNumber, getVoicemailNumber, log);

            log?.Info(
                "DirectionResolver selected resolver: {0} (ExtensionRangeIsEmpty={1})",
                _resolver.GetType().Name,
                extensionRange.IsEmpty);
        }

        public CallDirection ResolveDirection(RawCdrRecord raw, string threadId, out bool callerIsInternal, out bool destIsInternal)
            => _resolver.ResolveDirection(raw, threadId, out callerIsInternal, out destIsInternal);

        public void AssignCallerCalledFields(ProcessedLeg leg, RawCdrRecord raw, CallDirection direction, bool callerIsInternal, bool destIsInternal)
            => _resolver.AssignCallerCalledFields(leg, raw, direction, callerIsInternal, destIsInternal);
    }
}


