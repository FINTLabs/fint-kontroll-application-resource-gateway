package no.fintlabs.resources.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters;

@Getter
@AllArgsConstructor
public class EntityPipeline {

    private EntityTopicNameParameters topicNameParameters;
    private String fintEndpoint;
    private String selfLinkKeyFilter;

}
