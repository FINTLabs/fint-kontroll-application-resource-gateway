package no.fintlabs.resources.entity.properties;

import lombok.Data;

@Data
public class EntityRefreshConfiguration {
    private long intervalMs;
    private long topicRetentionTimeOffsetMs;

    public long getTopicRetentionTimeMs() {
        return intervalMs + topicRetentionTimeOffsetMs;
    }
}
