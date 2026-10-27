package jhsoftware.paymentsflow.observability;

import java.io.IOException;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
class CorrelationRequestFilter extends OncePerRequestFilter {

	static final String TRACE_HEADER = "X-Trace-Id";

	@Override
	protected void doFilterInternal(
			HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
			throws ServletException, IOException {
		try {
			CorrelationContext.put(CorrelationContext.CORRELATION_ID, request.getHeader(TRACE_HEADER));
			filterChain.doFilter(request, response);
		}
		finally {
			CorrelationContext.clear();
		}
	}
}
