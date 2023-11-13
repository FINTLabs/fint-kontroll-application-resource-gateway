package no.fintlabs.resources.request;

import no.fintlabs.FintClient;
import no.fintlabs.kafka.common.ListenerBeanRegistrationService;
import no.fintlabs.kafka.common.topic.TopicCleanupPolicyParameters;
import no.fintlabs.kafka.requestreply.ReplyProducerRecord;
import no.fintlabs.kafka.requestreply.RequestConsumerFactoryService;
import no.fintlabs.kafka.requestreply.topic.RequestTopicService;
import no.fintlabs.resources.request.properties.RequestConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonLoggingErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

@Configuration
@ConditionalOnProperty("fint.resource-gateway.resources.request.enabled")
public class RequestConsumerConfiguration {

    public RequestConsumerConfiguration(
            RequestTopicService requestTopicService,
            FintClient fintClient,
            RequestConsumerFactoryService requestConsumerFactoryService,
            RequestPipelineFactory requestPipelineFactory,
            RequestConfiguration requestConfiguration,
            ListenerBeanRegistrationService listenerBeanRegistrationService
    ) {
        createRequestConsumers(
                requestTopicService,
                fintClient,
                requestConsumerFactoryService,
                requestPipelineFactory,
                requestConfiguration,
                listenerBeanRegistrationService
        );
    }

    private void createRequestConsumers(
            RequestTopicService requestTopicService,
            FintClient fintClient,
            RequestConsumerFactoryService requestConsumerFactoryService,
            RequestPipelineFactory requestPipelineFactory,
            RequestConfiguration requestConfiguration,
            ListenerBeanRegistrationService listenerBeanRegistrationService
    ) {
        requestConfiguration.getRequestPipelines()
                .stream()
                .map(requestPipelineFactory::create)
                .map(requestPipeline -> createResourceRequestConsumer(
                                requestTopicService,
                                fintClient,
                                requestConsumerFactoryService,
                                requestPipeline
                        )
                ).forEach(listenerBeanRegistrationService::registerBean);
    }

    public ConcurrentMessageListenerContainer<String, ResourceRequestParams> createResourceRequestConsumer(
            RequestTopicService requestTopicService,
            FintClient fintClient,
            RequestConsumerFactoryService requestConsumerFactoryService,
            RequestPipeline requestPipeline
    ) {
        requestTopicService.ensureTopic(requestPipeline.getTopicNameParameters(), 0, TopicCleanupPolicyParameters.builder().build());

        return requestConsumerFactoryService.createFactory(
                ResourceRequestParams.class,
                Object.class,
                (consumerRecord) -> ReplyProducerRecord.builder()
                        .value(
                                fintClient.getResource(
                                        formatEndpoint(consumerRecord.value(), requestPipeline),
                                        Object.class
                                ).block()
                        ).build(),
                new CommonLoggingErrorHandler()
        ).createContainer(requestPipeline.getTopicNameParameters());
    }

    private String formatEndpoint(ResourceRequestParams resourceRequestParams, RequestPipeline requestPipeline) {
        return String.format(
                requestPipeline.getFintEndpointFormat(),
                (Object[]) getParams(resourceRequestParams, requestPipeline)
        );
    }

    private String[] getParams(ResourceRequestParams resourceRequestParams, RequestPipeline requestPipeline) {
        String[] params = resourceRequestParams.getParams();
        if (params.length != requestPipeline.getExpectedNumberOfPathParams()) {
            throw new IllegalArgumentException(
                    "The number of provided request parameters (" +
                            params.length +
                            ") does not match the expected number of parameters (" +
                            requestPipeline.getExpectedNumberOfPathParams() +
                            ")"
            );
        }
        return params;
    }

}
