package jhsoftware.paymentsflow.processor;

import java.util.UUID;

record PspAuthorizeResponseBody(UUID pspOperationId, String result, String reasonCode) {
}
