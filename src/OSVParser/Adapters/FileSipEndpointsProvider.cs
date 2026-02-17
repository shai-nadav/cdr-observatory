using System;
using System.Collections.Generic;
using Pipeline.Components.OSVParser.Config;

namespace Pipeline.Components.OSVParser
{
    /// <summary>
    /// File-based SIP endpoints provider (for standalone/config usage).
    /// Loads SipEndpoints XML and exposes PSTN gateway detection.
    /// </summary>
    public class FileSipEndpointsProvider : ISipEndpointsProvider
    {
        private readonly SipEndpointsLoader _loader = new SipEndpointsLoader();
        private readonly Dictionary<string, string> _addressToTrunk = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        private bool _loaded;

        public FileSipEndpointsProvider(string sipEndpointsFile)
        {
            LoadFromFile(sipEndpointsFile);
        }

        private void LoadFromFile(string sipEndpointsFile)
        {
            _loader.Load(sipEndpointsFile);
            _loaded = _loader.IsLoaded;
            _addressToTrunk.Clear();
            // We don't have trunk names in the XML; map address -> address as a safe default
            if (_loaded)
            {
                // Loader doesn't expose list; rely on IsPstn/IsLoaded for detection
                // Address-to-trunk map is optional in most flows
            }
        }

        public IDictionary<string, string> LoadAddressToTrunkMap()
        {
            return _addressToTrunk;
        }

        public bool IsPstn(string endpoint)
        {
            return _loader.IsPstn(endpoint);
        }

        public bool IsLoaded => _loaded;

        public int PstnCount => _loader.PstnCount;
    }
}
