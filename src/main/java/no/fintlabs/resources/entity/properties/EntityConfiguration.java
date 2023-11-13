package no.fintlabs.resources.entity.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
@ConfigurationProperties("fint.resource-gateway.resources.entity")
public class EntityConfiguration {
    private boolean enabled;
    private EntityRefreshConfiguration refresh;
    private EntityPullConfiguration pull;
    private List<EntityPipelineConfiguration> entityPipelines;
}
