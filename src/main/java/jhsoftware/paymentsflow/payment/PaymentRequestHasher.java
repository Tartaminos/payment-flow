package jhsoftware.paymentsflow.payment;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

import org.springframework.stereotype.Component;

@Component
class PaymentRequestHasher {

	String hash(PaymentAdmissionCommand command) {
		try {
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			try (DataOutputStream canonical = new DataOutputStream(bytes)) {
				canonical.writeByte(1);
				writeString(canonical, command.orderId());
				canonical.writeLong(command.amountMinor());
				writeString(canonical, command.currency());
			}
			return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
		}
		catch (IOException exception) {
			throw new IllegalStateException("Could not canonicalize payment request", exception);
		}
		catch (NoSuchAlgorithmException exception) {
			throw new IllegalStateException("SHA-256 is unavailable", exception);
		}
	}

	private void writeString(DataOutputStream output, String value) throws IOException {
		byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
		output.writeInt(encoded.length);
		output.write(encoded);
	}
}
