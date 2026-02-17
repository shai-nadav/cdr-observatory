using System;
using System.Collections.Generic;

namespace Pipeline.Components.OSVParser
{
    /// <summary>
    /// Adapts an <see cref="ISipEndpointsProvider"/> (TEM-CA DB integration)
    /// into the unified <see cref="ISipEndpointResolver"/> interface.
    /// </summary>
    internal sealed class SipEndpointProviderAdapter : ISipEndpointResolver
    {
        private readonly ISipEndpointsProvider _provider;
        private IDictionary<string, string> _addressMap;

        public SipEndpointProviderAdapter(ISipEndpointsProvider provider)
        {
            if (provider == null) throw new ArgumentNullException(nameof(provider));
            _provider = provider;
        }

        private IDictionary<string, string> AddressMap
            => _addressMap ?? (_addressMap = _provider.LoadAddressToTrunkMap()
                              ?? (IDictionary<string, string>)new Dictionary<string, string>());

        public bool IsPstn(string endpoint)
        {
            if (string.IsNullOrEmpty(endpoint)) return false;
            return _provider.IsPstn(endpoint);
        }

        public bool IsKnown(string endpoint)
        {
            if (string.IsNullOrEmpty(endpoint)) return false;
            return AddressMap.ContainsKey(endpoint);
        }

        public bool IsEmpty => !_provider.IsLoaded || AddressMap.Count == 0;

        public bool IsLoaded => _provider.IsLoaded;

        public int PstnCount => _provider.PstnCount;
    }
}
