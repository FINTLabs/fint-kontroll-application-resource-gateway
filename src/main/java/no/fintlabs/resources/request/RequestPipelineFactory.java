package no.fintlabs.resources.request;

import no.fintlabs.kafka.requestreply.topic.RequestTopicNameParameters;
import no.fintlabs.resources.request.properties.RequestPipelineConfiguration;
import org.springframework.stereotype.Service;

@Service
public class RequestPipelineFactory {

    public RequestPipeline create(RequestPipelineConfiguration configuration) {
        RequestTopicNameParameters requestTopicNameParameters = RequestTopicNameParameters
                .builder()
                .resource(configuration.getResourceReference())
                .parameterName(String.join("-and-", configuration.getRequestParameterNames()))
                .build();

        return new RequestPipeline(
                requestTopicNameParameters,
                configuration.getFintEndpointFormat(),
                configuration.getRequestParameterNames().size()
        );
    }
}
