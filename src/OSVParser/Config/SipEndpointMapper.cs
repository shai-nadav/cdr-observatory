using System;
using System.Collections.Generic;
using System.IO;
using System.Xml;

namespace Pipeline.Components.OSVParser.Config
{
    /// <summary>
    /// Parses SipEndpoints XML and determines if endpoints are PSTN gateways.
    /// Used for call direction detection based on Ingress/Egress endpoints.
    /// </summary>
    public class SipEndpointMapper : ISipEndpointResolver
    {
        private readonly HashSet<string> _pstnEndpoints = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _internalEndpoints = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public SipEndpointMapper()
        {
        }

        /// <summary>
        /// Load SIP endpoints from XML file.
        /// NNITypePSTNGateway = PSTN/External
        /// Everything else (NNITypeOther, NNITypeOpenScapeVoice) = Internal
        /// </summary>
        public void LoadFromFile(string xmlPath)
        {
            if (string.IsNullOrEmpty(xmlPath) || !File.Exists(xmlPath))
                return;

            var doc = new XmlDocument();
            doc.Load(xmlPath);

            var endpoints = doc.SelectNodes("//SipEndpoint");
            if (endpoints == null) return;

            foreach (XmlNode endpoint in endpoints)
            {
                var type = endpoint.SelectSingleNode("Type")?.InnerText;
                var name = endpoint.SelectSingleNode("Name")?.InnerText;
                var ipFqdn = endpoint.SelectSingleNode("IpFqdn")?.InnerText;

                if (type == "NNITypePSTNGateway")
                {
                    // PSTN gateway - external
                    if (!string.IsNullOrEmpty(name))
                        _pstnEndpoints.Add(name);
                    if (!string.IsNullOrEmpty(ipFqdn))
                        _pstnEndpoints.Add(ipFqdn);
                }
                else
                {
                    // Internal endpoint (NNITypeOther, NNITypeOpenScapeVoice, etc.)
                    if (!string.IsNullOrEmpty(name))
                        _internalEndpoints.Add(name);
                    if (!string.IsNullOrEmpty(ipFqdn))
                        _internalEndpoints.Add(ipFqdn);
                }
            }
        }

        /// <summary>
        /// Check if an endpoint is a PSTN gateway (external).
        /// Returns true if endpoint matches a known PSTN gateway.
        /// Returns false if endpoint is known internal OR unknown.
        /// </summary>
        public bool IsPstn(string endpoint)
        {
            if (string.IsNullOrEmpty(endpoint))
                return false;

            // Direct match against PSTN endpoints
            if (_pstnEndpoints.Contains(endpoint))
                return true;

            // If it matches a known internal endpoint, definitely not PSTN
            if (_internalEndpoints.Contains(endpoint))
                return false;

            // Unknown endpoint - assume internal (conservative)
            return false;
        }

        /// <summary>
        /// Check if an endpoint is known (either PSTN or internal).
        /// </summary>
        public bool IsKnown(string endpoint)
        {
            if (string.IsNullOrEmpty(endpoint))
                return false;
            return _pstnEndpoints.Contains(endpoint) || _internalEndpoints.Contains(endpoint);
        }

        /// <summary>
        /// Returns true if no endpoints are configured.
        /// When empty, direction detection falls back to extension ranges.
        /// </summary>
        /// <summary>
        /// Returns true if endpoint data was loaded successfully (non-empty).
        /// </summary>
        public bool IsLoaded => !IsEmpty;

        public bool IsEmpty => _pstnEndpoints.Count == 0 && _internalEndpoints.Count == 0;

        /// <summary>
        /// Number of PSTN endpoints loaded.
        /// </summary>
        public int PstnCount => _pstnEndpoints.Count;

        /// <summary>
        /// Number of internal endpoints loaded.
        /// </summary>
        public int InternalCount => _internalEndpoints.Count;
    }
}


