package no.fintlabs.resources.entity;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.FintClient;
import no.fintlabs.kafka.entity.EntityProducer;
import no.fintlabs.kafka.entity.EntityProducerFactory;
import no.fintlabs.kafka.entity.EntityProducerRecord;
import no.fintlabs.kafka.entity.topic.EntityTopicService;
import no.fintlabs.resources.entity.properties.EntityConfiguration;
import no.fintlabs.resources.entity.properties.EntityPipelineConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClientException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Component
@ConditionalOnProperty("fint.resource-gateway.resources.entity.enabled")
public class EntityPublishingComponent {

    private final EntityTopicService entityTopicService;
    private final EntityProducer<Object> entityProducer;
    private final FintClient fintClient;
    private final List<EntityPipeline> entityPipelines;

    public EntityPublishingComponent(
            EntityTopicService entityTopicService,
            EntityConfiguration entityConfiguration,
            EntityPipelineFactory entityPipelineFactory,
            EntityProducerFactory entityProducerFactory,
            FintClient fintClient
    ) {
        this.entityTopicService = entityTopicService;
        this.entityProducer = entityProducerFactory.createProducer(Object.class);
        this.fintClient = fintClient;
        this.entityPipelines = this.createEntityPipelines(
                entityPipelineFactory,
                entityConfiguration.getEntityPipelines()
        );
        this.ensureTopics(entityPipelines, entityConfiguration.getRefresh().getTopicRetentionTimeMs());
    }

    private List<EntityPipeline> createEntityPipelines(
            EntityPipelineFactory entityPipelineFactory,
            List<EntityPipelineConfiguration> configs) {
        return configs.stream()
                .map(entityPipelineFactory::create)
                .collect(Collectors.toList());
    }

    private void ensureTopics(List<EntityPipeline> entityPipelines, long topicRetentionTime) {
        entityPipelines.forEach(entityPipeline -> this.entityTopicService.ensureTopic(
                entityPipeline.getTopicNameParameters(),
                topicRetentionTime
        ));
    }

    @Scheduled(fixedRateString = "#{entityConfiguration.refresh.intervalMs}")
    private void resetLastUpdatedTimestamps() {
        log.warn("Resetting resource last updated timestamps");
        this.fintClient.resetLastUpdatedTimestamps();
    }

    @Scheduled(
            initialDelayString = "#{entityConfiguration.pull.initialDelayMs}",
            fixedDelayString = "#{entityConfiguration.pull.fixedDelayMs}")
    private void pullAllUpdatedEntityResources() {
        log.info("Starting pulling resources");
        entityPipelines.forEach(this::pullUpdatedEntityResources);
        log.info("Completed pulling resources");
    }

    private void pullUpdatedEntityResources(EntityPipeline entityPipeline) {
        List<HashMap<String, Object>> resources = getUpdatedResources(entityPipeline.getFintEndpoint());
        for (HashMap<String, Object> resource : resources) {
            String key = getKey(resource, entityPipeline.getSelfLinkKeyFilter());
            entityProducer.send(
                    EntityProducerRecord.builder()
                            .topicNameParameters(entityPipeline.getTopicNameParameters())
                            .key(key)
                            .value(resource)
                            .build()
            );
        }
        log.info(resources.size() + " entities sent to " + entityPipeline.getTopicNameParameters());
    }

    private List<HashMap<String, Object>> getUpdatedResources(String endpointUrl) {
        try {
            return Objects.requireNonNull(fintClient.getResourcesLastUpdated(endpointUrl).block())
                    .stream()
                    .map(r -> ((HashMap<String, Object>) r))
                    .collect(Collectors.toList());
        } catch (WebClientException e) {
            log.error("Could not pull entities from endpoint=" + endpointUrl, e);
            return Collections.emptyList();
        }
    }

    // TODO: 19/11/2021 Handle exceptions (casting and no systemid)
    private String getKey(HashMap<String, Object> resource, String selfLinkKeyFilter) {
        HashMap<String, Object> links = (HashMap<String, Object>) resource.get("_links");
        List<HashMap<String, String>> selfLinks = (List<HashMap<String, String>>) links.get("self");
        return selfLinks.stream()
                .filter(o -> o.containsKey("href"))
                .map(o -> o.get("href"))
                .map(k -> k.replaceFirst("^https:/\\/.+\\.felleskomponent.no", ""))
                .filter(o -> o.toLowerCase().contains(selfLinkKeyFilter))
                .min(String::compareTo)
                .orElseThrow(() -> new IllegalStateException(String.format("No %s to generate key for resource=%s", selfLinkKeyFilter, resource)));
    }

}
