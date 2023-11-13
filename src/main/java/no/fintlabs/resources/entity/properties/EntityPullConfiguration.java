package no.fintlabs.resources.entity.properties;

import lombok.Data;

@Data
public class EntityPullConfiguration {
    private long initialDelayMs;
    private long fixedDelayMs;
}
