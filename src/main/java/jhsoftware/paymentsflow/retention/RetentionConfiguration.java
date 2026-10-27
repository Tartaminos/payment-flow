package jhsoftware.paymentsflow.retention;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RetentionCleanupProperties.class)
class RetentionConfiguration {
}
