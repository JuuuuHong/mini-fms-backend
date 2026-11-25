package com.minifms.security;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * JWT 검증 설정(HS256 공유 비밀키).
 *
 * @param secret          서명 키. 환경 변수 FMS_JWT_SECRET 으로 넣는다. 32바이트(256비트) 미만이면 기동하지 않는다.
 * @param issuer          토큰의 iss 가 이 값이어야 한다.
 * @param devTokenEnabled 개발용 토큰 발급 API(/api/v1/auth/dev-token) 사용 여부. local 프로필에서만 켠다.
 * @param devTokenMaxTtl  개발용 토큰의 최대 유효 시간.
 */
@ConfigurationProperties(prefix = "fms.security.jwt")
public record JwtProperties(
        String secret,
        @DefaultValue("mini-fms") String issuer,
        @DefaultValue("false") boolean devTokenEnabled,
        @DefaultValue("12h") Duration devTokenMaxTtl) {

    private static final int MIN_SECRET_BYTES = 32;

    public JwtProperties {
        if (secret == null || secret.getBytes(StandardCharsets.UTF_8).length < MIN_SECRET_BYTES) {
            throw new IllegalStateException(
                    "fms.security.jwt.secret(FMS_JWT_SECRET)을 32바이트 이상으로 설정해야 합니다. 로컬은 local 프로필을 쓰세요.");
        }
    }

    public SecretKey secretKey() {
        return new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
    }

    @Override
    public String toString() {
        return "JwtProperties[issuer=" + issuer + ", devTokenEnabled=" + devTokenEnabled + "]";
    }
}
