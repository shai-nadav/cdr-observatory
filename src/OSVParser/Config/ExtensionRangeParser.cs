using System;
using System.Collections.Generic;
using System.Linq;

namespace Pipeline.Components.OSVParser.Config
{
    /// <summary>
    /// Parses and evaluates extension ranges. Supports individual numbers and ranges.
    /// Thread-safe after construction.
    /// </summary>
    public class ExtensionRangeParser
    {
        private class ExtensionRange
        {
            public long Start { get; set; }
            public long End { get; set; }
            public int Length { get; set; }
        }

        private readonly HashSet<string> _individualNumbers = new HashSet<string>();
        private readonly List<ExtensionRange> _ranges = new List<ExtensionRange>();

        public ExtensionRangeParser(IEnumerable<string> ranges)
        {
            if (ranges == null) return;

            foreach (var range in ranges)
            {
                var trimmed = range?.Trim();
                if (string.IsNullOrEmpty(trimmed)) continue;

                if (trimmed.Contains("-"))
                {
                    var parts = trimmed.Split('-');
                    long start;
                    long end;
                    if (parts.Length == 2 &&
                        long.TryParse(parts[0].Trim(), out start) &&
                        long.TryParse(parts[1].Trim(), out end))
                    {
                        _ranges.Add(new ExtensionRange
                        {
                            Start = Math.Min(start, end),
                            End = Math.Max(start, end),
                            Length = parts[0].Trim().Length
                        });
                    }
                }
                else
                {
                    _individualNumbers.Add(trimmed);
                }
            }
        }

        /// <summary>
        /// Check if a number is within any configured extension range.
        /// Handles numbers with or without country code prefix (1).
        /// </summary>
        public bool IsExtension(string number)
        {
            if (string.IsNullOrEmpty(number)) return false;

            var trimmed = number.Trim();

            if (_individualNumbers.Contains(trimmed))
                return true;

            long numericValue;
            if (long.TryParse(trimmed, out numericValue))
            {
                foreach (var range in _ranges)
                {
                    if (numericValue >= range.Start && numericValue <= range.End)
                        return true;
                }
                
                // Try with leading "1" prefix if not already starting with 1
                // This handles cases where raw CDR has 10-digit numbers but range expects 11-digit with country code
                if (!trimmed.StartsWith("1") && trimmed.Length >= 10)
                {
                    var withPrefix = "1" + trimmed;
                    long prefixedValue;
                    if (long.TryParse(withPrefix, out prefixedValue))
                    {
                        foreach (var range in _ranges)
                        {
                            if (prefixedValue >= range.Start && prefixedValue <= range.End)
                                return true;
                        }
                    }
                }
                
                // Try without leading "1" prefix if it starts with 1
                // This handles cases where raw CDR has 11-digit numbers but range expects 10-digit without country code
                if (trimmed.StartsWith("1") && trimmed.Length >= 11)
                {
                    var withoutPrefix = trimmed.Substring(1);
                    long unprefixedValue;
                    if (long.TryParse(withoutPrefix, out unprefixedValue))
                    {
                        foreach (var range in _ranges)
                        {
                            if (unprefixedValue >= range.Start && unprefixedValue <= range.End)
                                return true;
                        }
                    }
                }
            }

            return false;
        }

        /// <summary>
        /// Returns true if no extension ranges are configured.
        /// Used for candidate extension discovery mode.
        /// </summary>
        public bool IsEmpty => _individualNumbers.Count == 0 && _ranges.Count == 0;

        /// <summary>
        /// Total count of ranges and individual numbers configured.
        /// </summary>
        public int Count => _individualNumbers.Count + _ranges.Count;
    }
}

