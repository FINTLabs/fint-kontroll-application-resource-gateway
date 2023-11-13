package no.fintlabs.resources.request.properties;

import lombok.Data;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
public class RequestPipelineConfiguration {
    private String resourceReference;
    private String fintEndpointFormat;
    private List<String> requestParameterNames;
}
