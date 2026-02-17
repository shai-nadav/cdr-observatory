using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Linq;

namespace Pipeline.Components.OSVParser.Config
{
    /// <summary>
    /// Loads SIP endpoints from OpenScape XML export and provides PSTN gateway detection.
    /// </summary>
    public class SipEndpointsLoader
    {
        private readonly HashSet<string> _pstnEndpoints = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, string> _endpointNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        public bool IsLoaded { get; private set; }

        /// <summary>
        /// Load endpoints from SipEndpoints XML file.
        /// </summary>
        public void Load(string xmlPath)
        {
            if (string.IsNullOrEmpty(xmlPath) || !File.Exists(xmlPath))
            {
                IsLoaded = false;
                return;
            }

            try
            {
                var doc = XDocument.Load(xmlPath);
                var endpoints = doc.Descendants("SipEndpoint");

                foreach (var ep in endpoints)
                {
                    var ipFqdn = ep.Element("IpFqdn")?.Value;
                    var type = ep.Element("Type")?.Value;
                    var name = ep.Element("Name")?.Value;

                    if (string.IsNullOrEmpty(ipFqdn)) continue;

                    _endpointNames[ipFqdn] = name ?? "Unknown";

                    // NNITypePSTNGateway = PSTN gateway = external
                    if (type == "NNITypePSTNGateway")
                    {
                        _pstnEndpoints.Add(ipFqdn);
                    }
                }

                IsLoaded = true;
            }
            catch
            {
                IsLoaded = false;
            }
        }

        /// <summary>
        /// Check if an endpoint is a PSTN gateway (external).
        /// Empty/null endpoints are considered internal.
        /// </summary>
        public bool IsPstn(string endpoint)
        {
            if (string.IsNullOrEmpty(endpoint)) return false;
            return _pstnEndpoints.Contains(endpoint);
        }

        /// <summary>
        /// Get the name of an endpoint (for logging/debugging).
        /// </summary>
        public string GetName(string endpoint)
        {
            if (string.IsNullOrEmpty(endpoint)) return null;
            string name;
            return _endpointNames.TryGetValue(endpoint, out name) ? name : null;
        }

        public int PstnCount => _pstnEndpoints.Count;
        public int TotalCount => _endpointNames.Count;
    }
}

