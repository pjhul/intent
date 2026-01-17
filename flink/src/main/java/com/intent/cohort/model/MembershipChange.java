package com.intent.cohort.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/**
 * Represents a change in cohort membership.
 */
public class MembershipChange implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int STATUS_OUT = -1;
    public static final int STATUS_IN = 1;

    @JsonProperty("cohort_id")
    private UUID cohortId;

    @JsonProperty("cohort_name")
    private String cohortName;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("prev_status")
    private int prevStatus;

    @JsonProperty("new_status")
    private int newStatus;

    @JsonProperty("changed_at")
    private Instant changedAt;

    @JsonProperty("trigger_event")
    private UUID triggerEvent;

    public MembershipChange() {}

    public MembershipChange(UUID cohortId, String cohortName, String userId,
                           int prevStatus, int newStatus, UUID triggerEvent) {
        this.cohortId = cohortId;
        this.cohortName = cohortName;
        this.userId = userId;
        this.prevStatus = prevStatus;
        this.newStatus = newStatus;
        this.changedAt = Instant.now();
        this.triggerEvent = triggerEvent;
    }

    public static MembershipChange entered(UUID cohortId, String cohortName,
                                           String userId, UUID triggerEvent) {
        return new MembershipChange(cohortId, cohortName, userId,
                                    STATUS_OUT, STATUS_IN, triggerEvent);
    }

    public static MembershipChange exited(UUID cohortId, String cohortName,
                                          String userId, UUID triggerEvent) {
        return new MembershipChange(cohortId, cohortName, userId,
                                    STATUS_IN, STATUS_OUT, triggerEvent);
    }

    public boolean isEntry() {
        return prevStatus == STATUS_OUT && newStatus == STATUS_IN;
    }

    public boolean isExit() {
        return prevStatus == STATUS_IN && newStatus == STATUS_OUT;
    }

    public UUID getCohortId() { return cohortId; }
    public void setCohortId(UUID cohortId) { this.cohortId = cohortId; }

    public String getCohortName() { return cohortName; }
    public void setCohortName(String cohortName) { this.cohortName = cohortName; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public int getPrevStatus() { return prevStatus; }
    public void setPrevStatus(int prevStatus) { this.prevStatus = prevStatus; }

    public int getNewStatus() { return newStatus; }
    public void setNewStatus(int newStatus) { this.newStatus = newStatus; }

    public Instant getChangedAt() { return changedAt; }
    public void setChangedAt(Instant changedAt) { this.changedAt = changedAt; }

    public UUID getTriggerEvent() { return triggerEvent; }
    public void setTriggerEvent(UUID triggerEvent) { this.triggerEvent = triggerEvent; }

    @Override
    public String toString() {
        return "MembershipChange{" +
                "cohortId=" + cohortId +
                ", userId='" + userId + '\'' +
                ", change=" + (isEntry() ? "ENTERED" : "EXITED") +
                '}';
    }
}
