package com.linkin.apigateway.filter;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import javax.crypto.SecretKey;

/**
 * Flow:
 * 1. Extract JWT from Authorization header
 * 2. Validate JWT signature and expiry
 * 3. Extract userId from token claims
 * 4. Add userId to request header for the downstream service
 * 5. Forward request to the correct service
 * If JWT is invalid or missing -> 401 Unauthorized
 *
 * Usage in application.yml:
 *   filters:
 *     - JwtAuthFilter
 *
 * Requires property: jwt.secret (Base64-encoded HMAC key, at least 256 bits)
 */
@Component
@Slf4j
public class JwtAuthFilter extends AbstractGatewayFilterFactory<JwtAuthFilter.Config> {

    private static final String BEARER_PREFIX = "Bearer ";
    public static final String USER_ID_HEADER = "X-User-Id";

    private final SecretKey signingKey;

    public JwtAuthFilter(@Value("${jwt.secret}") String secret) {
        super(Config.class);
        this.signingKey = Keys.hmacShaKeyFor(Decoders.BASE64.decode(secret));
    }

    @Override
    public GatewayFilter apply(Config config) {
        return (exchange, chain) -> {
            String authHeader = exchange.getRequest()
                    .getHeaders()
                    .getFirst(HttpHeaders.AUTHORIZATION);

            if (authHeader == null || !authHeader.startsWith(BEARER_PREFIX)) {
                log.warn("Missing or invalid Authorization header");
                return unauthorized(exchange);
            }

            String token = authHeader.substring(BEARER_PREFIX.length()).trim();
            if (token.isEmpty()) {
                log.warn("Empty bearer token");
                return unauthorized(exchange);
            }

            String userId;
            try {
                // Verifies signature and expiry; throws JwtException if invalid
                Claims claims = Jwts.parser()
                        .verifyWith(signingKey)
                        .build()
                        .parseSignedClaims(token)
                        .getPayload();

                userId = claims.get("userId", String.class);
                if (userId == null) {
                    userId = claims.getSubject();
                }
            } catch (JwtException | IllegalArgumentException e) {
                log.warn("JWT validation failed: {}", e.getMessage());
                return unauthorized(exchange);
            }

            if (userId == null || userId.isBlank()) {
                log.warn("JWT has no userId or subject claim");
                return unauthorized(exchange);
            }

            // Remove any client-supplied X-User-Id first so it can't be spoofed
            ServerHttpRequest request = exchange.getRequest().mutate()
                    .headers(headers -> headers.remove(USER_ID_HEADER))
                    .header(USER_ID_HEADER, userId)
                    .build();

            return chain.filter(exchange.mutate().request(request).build());
        };
    }

    private Mono<Void> unauthorized(ServerWebExchange exchange) {
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        return exchange.getResponse().setComplete();
    }

    /** Placeholder for per-route filter configuration. */
    public static class Config {
    }
}