using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser.Processing
{
    internal interface IDirectionResolver
    {
        CallDirection ResolveDirection(RawCdrRecord raw, string threadId, out bool callerIsInternal, out bool destIsInternal);
        void AssignCallerCalledFields(ProcessedLeg leg, RawCdrRecord raw, CallDirection direction, bool callerIsInternal, bool destIsInternal);
    }
}
