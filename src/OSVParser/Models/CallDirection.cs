namespace Pipeline.Components.OSVParser.Models
{
    public enum CallDirection
    {
        Unknown,
        Incoming,
        Outgoing,
        Internal,
        TrunkToTrunk,
        /// <summary>T2T incoming leg: external caller to internal extension.</summary>
        T2TIn,
        /// <summary>T2T outgoing leg: internal extension to external destination.</summary>
        T2TOut
    }
}
