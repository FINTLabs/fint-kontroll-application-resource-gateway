package no.fintlabs.resources.request.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Collections;
import java.util.List;

@Data
@ConfigurationProperties("fint.resource-gateway.resources.request")
public class RequestConfiguration {
    private boolean enabled;
    private List<RequestPipelineConfiguration> requestPipelines = Collections.emptyList();
}
