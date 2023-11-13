package no.fintlabs.resources.entity.properties;

import lombok.Data;

@Data
public class EntityPipelineConfiguration {
    private String resourceReference;
    private String kafkaTopic;
    private String fintEndpoint;
    private String selfLinkKeyFilter;
}
