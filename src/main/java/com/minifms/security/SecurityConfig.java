package com.minifms.security;

import static com.minifms.security.Scopes.authority;

import com.nimbusds.jose.jwk.source.ImmutableSecret;
import java.time.Instant;
import java.util.Objects;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;
import org.springframework.security.web.SecurityFilterChain;

/**
 * API 인증·인가. Spring Security OAuth2 Resource Server 의 Bearer 토큰 필터
 * (BearerTokenAuthenticationFilter)가 Authorization: Bearer &lt;JWT&gt; 를 꺼내 서명·만료·발급자를 검증한다.
 *
 * <pre>
 *   GET  /api/v1/**                      fleet:read        (관제 조회)
 *   POST /api/v1/vehicles/{id}/commands  vehicle:control   (원격 제어)
 *   POST /api/v1/telematics[/batch]      telematics:write  (단말 수집)
 *   POST /api/v1/incidents               incident:write    (RSU 보고)
 *   /actuator/health, /actuator/info     공개
 *   /actuator/**                         fleet:read
 * </pre>
 * 토큰이 없거나 잘못되면 401, 권한(scope)이 모자라면 403. 세션을 만들지 않는다(stateless).
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain apiSecurity(HttpSecurity http, JwtProperties properties) throws Exception {
        http
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> {
                    auth.requestMatchers("/error", "/actuator/health/**", "/actuator/info").permitAll();
                    if (properties.devTokenEnabled()) {
                        auth.requestMatchers(HttpMethod.POST, "/api/v1/auth/dev-token").permitAll();
                    }
                    auth.requestMatchers(HttpMethod.POST, "/api/v1/vehicles/*/commands").hasAuthority(authority(Scopes.VEHICLE_CONTROL))
                            .requestMatchers(HttpMethod.POST, "/api/v1/telematics", "/api/v1/telematics/batch")
                            .hasAuthority(authority(Scopes.TELEMATICS_WRITE))
                            .requestMatchers(HttpMethod.POST, "/api/v1/incidents").hasAuthority(authority(Scopes.INCIDENT_WRITE))
                            .requestMatchers(HttpMethod.GET, "/api/v1/**").hasAuthority(authority(Scopes.FLEET_READ))
                            .requestMatchers("/actuator/**").hasAuthority(authority(Scopes.FLEET_READ))
                            .anyRequest().denyAll();
                })
                .oauth2ResourceServer(oauth2 -> oauth2.jwt(Customizer.withDefaults()));
        return http.build();
    }

    /**
     * HS256 서명 검증 + exp/nbf(기본 60초 시계 오차 허용) + iss 검증 + exp 필수.
     * 기본 검증기는 exp 가 없으면 통과시키므로(영구 토큰), exp 가 있어야 한다는 검증을 더한다.
     */
    @Bean
    public JwtDecoder jwtDecoder(JwtProperties properties) {
        NimbusJwtDecoder decoder = NimbusJwtDecoder.withSecretKey(properties.secretKey())
                .macAlgorithm(MacAlgorithm.HS256)
                .build();
        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(
                JwtValidators.createDefaultWithIssuer(properties.issuer()),
                new JwtClaimValidator<Instant>(JwtClaimNames.EXP, Objects::nonNull)));
        return decoder;
    }

    /** 개발용 토큰 발급·테스트에서 쓴다. 운영에서는 외부 인증 서버(IdP)가 발급하고 이 앱은 검증만 한다. */
    @Bean
    public JwtEncoder jwtEncoder(JwtProperties properties) {
        return new NimbusJwtEncoder(new ImmutableSecret<>(properties.secretKey()));
    }
}
