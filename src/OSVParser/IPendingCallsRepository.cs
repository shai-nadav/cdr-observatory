using System;
using System.Collections.Generic;
using Pipeline.Components.OSVParser.Models;

namespace Pipeline.Components.OSVParser
{
    /// <summary>
    /// Repository for pending calls between sessions.
    /// Memory during session, DB/file between sessions.
    /// Abstracted for testing and TEM-CA integration.
    /// </summary>
    public interface IPendingCallsRepository
    {
        /// <summary>
        /// Load pending calls from storage (don't delete yet - they may not match).
        /// </summary>
        /// <param name="instanceId">Instance identifier for scoping</param>
        /// <returns>List of pending call records</returns>
        IList<PendingCallRecord> Load(int instanceId);
        
        /// <summary>
        /// Sync changes at end of session.
        /// Deletes matched/orphaned calls, upserts remaining.
        /// </summary>
        /// <param name="instanceId">Instance identifier for scoping</param>
        /// <param name="deletedIds">CallIds that were matched or orphaned</param>
        /// <param name="upsertCalls">Pending calls to insert/update</param>
        void Sync(int instanceId, IList<string> deletedIds, IList<PendingCallRecord> upsertCalls);
        
        /// <summary>Get current record sequence (for orphan threshold check)</summary>
        long GetCurrentRecordSeq();
        
        /// <summary>Increment and return new record sequence</summary>
        long IncrementRecordSeq();
    }

    /// <summary>
    /// Pending call record (DB entity or in-memory).
    /// </summary>
    public class PendingCallRecord
    {
        /// <summary>Identity (DB) or generated ID (in-memory)</summary>
        public int PendingCallID { get; set; }
        
        /// <summary>Instance identifier for multi-instance support</summary>
        public int InstanceId { get; set; }
        
        /// <summary>Unique call identifier</summary>
        public string CallId { get; set; }
        
        /// <summary>Thread identifier (groups legs of same call)</summary>
        public string ThreadId { get; set; }
        
        /// <summary>Call start time</summary>
        public DateTime? StartTime { get; set; }
        
        /// <summary>Caller extension</summary>
        public string CallerExtension { get; set; }
        
        /// <summary>Called number</summary>
        public string CalledNumber { get; set; }
        
        /// <summary>When this record arrived (for age-based orphaning)</summary>
        public DateTime ArrivedDate { get; set; }
        
        /// <summary>Record sequence at arrival (for record-based orphaning)</summary>
        public long RecordSeq { get; set; }
        
        /// <summary>Serialized call data (legs, state)</summary>
        public string CallData { get; set; }
    }
}

