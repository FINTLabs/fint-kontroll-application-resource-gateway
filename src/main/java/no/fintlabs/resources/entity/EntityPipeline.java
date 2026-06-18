package no.fintlabs.resources.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import no.novari.kafka.topic.name.EntityTopicNameParameters;
@Getter
@AllArgsConstructor
public class EntityPipeline {

    private EntityTopicNameParameters topicNameParameters;
    private String fintEndpoint;
    private String selfLinkKeyFilter;

}
