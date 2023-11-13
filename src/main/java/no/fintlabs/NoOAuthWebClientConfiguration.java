package no.fintlabs;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Getter
@Setter
@Configuration
@ConditionalOnProperty(name = "fint.kontroll.datainput", havingValue = "mock")
@ConfigurationProperties(prefix = "fint.client")
@Slf4j
public class NoOAuthWebClientConfiguration {

    private String baseUrl;

    @Bean
    public WebClient webClient() {
        WebClient webClient = WebClient
                .builder()
                .baseUrl(baseUrl)
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(-1))
                .build();
        log.info("simple webclient without authentification created");
        return webClient;
    }

}
