package jhsoftware.paymentsflow.processor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

final class PaymentOperationRetryIdentity {

	private static final UUID NAMESPACE = UUID.fromString("34b2fbac-4e10-5f90-a5e2-4dfe1304a51b");

	private PaymentOperationRetryIdentity() {
	}

	static UUID retryId(String sourceTopic, int sourcePartition, long sourceOffset) {
		String name = "retry|" + sourceTopic + "|" + sourcePartition + "|" + sourceOffset;
		return uuidV5(NAMESPACE, name);
	}

	private static UUID uuidV5(UUID namespace, String name) {
		byte[] namespaceBytes = toBytes(namespace);
		byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
		byte[] input = new byte[namespaceBytes.length + nameBytes.length];
		System.arraycopy(namespaceBytes, 0, input, 0, namespaceBytes.length);
		System.arraycopy(nameBytes, 0, input, namespaceBytes.length, nameBytes.length);

		byte[] hash = sha1(input);

		hash[6] &= 0x0F;
		hash[6] |= 0x50;
		hash[8] &= 0x3F;
		hash[8] |= 0x80;

		long mostSignificantBits = 0;
		for (int i = 0; i < 8; i++) {
			mostSignificantBits = (mostSignificantBits << 8) | (hash[i] & 0xFF);
		}
		long leastSignificantBits = 0;
		for (int i = 8; i < 16; i++) {
			leastSignificantBits = (leastSignificantBits << 8) | (hash[i] & 0xFF);
		}
		return new UUID(mostSignificantBits, leastSignificantBits);
	}

	private static byte[] sha1(byte[] input) {
		try {
			return MessageDigest.getInstance("SHA-1").digest(input);
		}
		catch (NoSuchAlgorithmException exception) {
			throw new IllegalStateException("SHA-1 is required for UUIDv5 generation", exception);
		}
	}

	private static byte[] toBytes(UUID uuid) {
		ByteBuffer buffer = ByteBuffer.allocate(16);
		buffer.putLong(uuid.getMostSignificantBits());
		buffer.putLong(uuid.getLeastSignificantBits());
		return buffer.array();
	}
}
