package com.minifms.security;

import com.minifms.common.web.InvalidRequestException;
import com.minifms.security.JwtTokenIssuer.IssuedToken;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 개발용 토큰 발급 API. fms.security.jwt.dev-token-enabled=true(local 프로필)일 때만 빈이 생기고 공개된다.
 * 운영 프로필에는 켜지 않는다 — 켜면 누구나 제어 권한 토큰을 만들 수 있다.
 */
@RestController
@RequestMapping("/api/v1/auth")
@ConditionalOnProperty(prefix = "fms.security.jwt", name = "dev-token-enabled", havingValue = "true")
public class DevTokenController {

    private final JwtTokenIssuer issuer;
    private final JwtProperties properties;

    public DevTokenController(JwtTokenIssuer issuer, JwtProperties properties) {
        this.issuer = issuer;
        this.properties = properties;
    }

    @PostMapping("/dev-token")
    public IssuedToken issue(@Valid @RequestBody DevTokenRequest request) {
        List<String> scopes = request.scopes() == null || request.scopes().isEmpty() ? Scopes.ALL : request.scopes();
        List<String> unknown = scopes.stream().filter(scope -> !Scopes.ALL.contains(scope)).toList();
        if (!unknown.isEmpty()) {
            throw new InvalidRequestException("알 수 없는 scope", unknown.stream().map(s -> s + " (가능: " + Scopes.ALL + ")").toList());
        }
        Duration ttl = Duration.ofMinutes(request.ttlMinutes() == null ? 60 : request.ttlMinutes());
        if (ttl.compareTo(properties.devTokenMaxTtl()) > 0) {
            ttl = properties.devTokenMaxTtl();
        }
        return issuer.issue(request.subject(), scopes, ttl);
    }

    public record DevTokenRequest(
            @NotBlank @Size(max = 64) String subject,
            List<String> scopes,
            @Min(1) @Max(720) Integer ttlMinutes) {
    }
}
