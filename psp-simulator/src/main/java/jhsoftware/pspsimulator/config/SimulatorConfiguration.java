package jhsoftware.pspsimulator.config;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class SimulatorConfiguration {

	@Bean
	Clock simulatorClock() {
		return Clock.systemUTC();
	}
}
