package jhsoftware.paymentsflow.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;

import org.junit.jupiter.api.Test;

class PaymentOperationRawValueDecoderTests {

	private final PaymentOperationRawValueDecoder decoder =
			new PaymentOperationRawValueDecoder(new RetryMessagingProperties(URI.create("mock://raw-decoder")));

	@Test
	void invalidConfluentFramingIsDefinitivePoisonWithoutRegistryLookup() {
		assertThatThrownBy(() -> decoder.decode("payment.operations.requested.v1", new byte[] {1, 2, 3}))
				.isInstanceOfSatisfying(PoisonCommandException.class,
						exception -> assertThat(exception.failureCode())
								.isEqualTo(PoisonFailureCode.CORRUPTED_WIRE_FORMAT));
	}
}
