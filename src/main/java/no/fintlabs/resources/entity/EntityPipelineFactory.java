package no.fintlabs.resources.entity;

import no.novari.kafka.topic.name.EntityTopicNameParameters;
import no.novari.kafka.topic.name.TopicNamePrefixParameters;
import no.fintlabs.resources.entity.properties.EntityPipelineConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

@Service
public class EntityPipelineFactory {

    public EntityPipeline create(EntityPipelineConfiguration entityPipelineConfiguration) {
        String resourceName = entityPipelineConfiguration.getResourceReference().replace(".","-");

        EntityTopicNameParameters topicNameParameters = EntityTopicNameParameters
                .builder()
                .topicNamePrefixParameters(TopicNamePrefixParameters
                        .stepBuilder()
                        .orgIdApplicationDefault()
                        .domainContextApplicationDefault()
                        .build())
                .resourceName(resourceName)
                .build();

        String fintEndpoint = StringUtils.isNotEmpty(entityPipelineConfiguration.getFintEndpoint())
                ? entityPipelineConfiguration.getFintEndpoint()
                : "/" + entityPipelineConfiguration.getResourceReference().replace(".", "/");

        String selfLinkKeyFilter = StringUtils.isNotEmpty(entityPipelineConfiguration.getSelfLinkKeyFilter())
                ? entityPipelineConfiguration.getSelfLinkKeyFilter()
                : "systemid";

        return new EntityPipeline(topicNameParameters, fintEndpoint, selfLinkKeyFilter);
    }

}
