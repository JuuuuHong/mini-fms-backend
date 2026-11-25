package com.minifms.security;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.minifms.support.IntegrationTestSupport;
import com.minifms.support.TestEvents;
import com.nimbusds.jose.jwk.source.ImmutableSecret;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;

/** 실제 서명된 JWT 로 Bearer 필터·인가 규칙을 확인한다. */
class SecurityIT extends IntegrationTestSupport {

    @Autowired
    JwtTokenIssuer issuer;

    @Test
    void 토큰이_없으면_401() throws Exception {
        mockMvc.perform(get("/api/v1/geofences"))
                .andExpect(status().isUnauthorized())
                .andExpect(header().string(HttpHeaders.WWW_AUTHENTICATE, org.hamcrest.Matchers.startsWith("Bearer")));
    }

    @Test
    void 조회_권한_토큰으로_조회는_되고_제어는_403() throws Exception {
        String token = issuer.issue("operator-1", List.of(Scopes.FLEET_READ), Duration.ofMinutes(5)).accessToken();

        mockMvc.perform(get("/api/v1/geofences").header(HttpHeaders.AUTHORIZATION, "Bearer " + token))
                .andExpect(status().isOk());
        mockMvc.perform(post("/api/v1/vehicles/V-001/commands").header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                        .contentType(MediaType.APPLICATION_JSON).content("{\"type\":\"DOOR_LOCK\"}"))
                .andExpect(status().isForbidden());
    }

    @Test
    void 제어_권한_토큰은_명령_API_를_통과한다() throws Exception {
        String token = issuer.issue("operator-2", List.of(Scopes.VEHICLE_CONTROL), Duration.ofMinutes(5)).accessToken();

        // 인증·인가를 통과해 컨트롤러까지 간다(텔레메트리가 없는 차량이라 404).
        mockMvc.perform(post("/api/v1/vehicles/NO-SUCH-VEHICLE/commands").header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                        .contentType(MediaType.APPLICATION_JSON).content("{\"type\":\"DOOR_LOCK\"}"))
                .andExpect(status().isNotFound());
    }

    @Test
    void 수집과_돌발상황_보고는_전용_scope_토큰만_할_수_있다() throws Exception {
        long now = System.currentTimeMillis();
        String telemetry = """
                {"vehicleId":"SEC-1","latitude":37.5,"longitude":127.0,"speed":10,
                 "batterySoc":50,"engineStatus":"ON","timestamp":%d}""".formatted(now);
        String incident = """
                {"incidentId":"%s","type":"ACCIDENT","latitude":37.5,"longitude":127.0,
                 "sourceId":"RSU-1","occurredAt":%d}""".formatted(TestEvents.uniqueId("SEC-INC"), now);
        String readOnly = issuer.issue("viewer", List.of(Scopes.FLEET_READ), Duration.ofMinutes(5)).accessToken();
        String controlOnly = issuer.issue("operator", List.of(Scopes.VEHICLE_CONTROL), Duration.ofMinutes(5)).accessToken();

        List<String[]> writes = List.of(
                new String[] {"/api/v1/telematics", telemetry},
                new String[] {"/api/v1/telematics/batch", "{\"events\":[" + telemetry + "]}"},
                new String[] {"/api/v1/incidents", incident});
        for (String[] write : writes) {
            // 토큰 없음 → 401
            mockMvc.perform(post(write[0]).contentType(MediaType.APPLICATION_JSON).content(write[1]))
                    .andExpect(status().isUnauthorized());
            // 다른 scope(조회·제어) 토큰 → 403
            for (String token : List.of(readOnly, controlOnly)) {
                mockMvc.perform(post(write[0]).header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                                .contentType(MediaType.APPLICATION_JSON).content(write[1]))
                        .andExpect(status().isForbidden());
            }
        }

        // 전용 scope 를 가진 토큰만 통과한다.
        String device = issuer.issue("gateway-1", List.of(Scopes.TELEMATICS_WRITE), Duration.ofMinutes(5)).accessToken();
        mockMvc.perform(post("/api/v1/telematics").header(HttpHeaders.AUTHORIZATION, "Bearer " + device)
                        .contentType(MediaType.APPLICATION_JSON).content(telemetry))
                .andExpect(status().isAccepted());
        String rsu = issuer.issue("rsu-1", List.of(Scopes.INCIDENT_WRITE), Duration.ofMinutes(5)).accessToken();
        mockMvc.perform(post("/api/v1/incidents").header(HttpHeaders.AUTHORIZATION, "Bearer " + rsu)
                        .contentType(MediaType.APPLICATION_JSON).content(incident))
                .andExpect(status().isAccepted());
    }

    @Test
    void 만료된_토큰_다른_키로_서명한_토큰_다른_발급자의_토큰은_401() throws Exception {
        String validSecret = "test-only-secret-0123456789-abcdefghijklmnop";
        Instant now = Instant.now();
        // 10분 전 발급, 5분 전 만료(검증기의 시계 오차 허용 60초를 넘김)
        String expired = sign(validSecret, "mini-fms", now.minusSeconds(600), now.minusSeconds(300));
        String forged = sign("another-secret-that-is-long-enough-0123456789", "mini-fms", now, now.plusSeconds(300));
        String wrongIssuer = sign(validSecret, "someone-else", now, now.plusSeconds(300));
        // 같은 키·같은 발급자로 만든 정상 토큰은 통과해야 위 세 경우가 "그 조건 때문에" 거절됐다고 말할 수 있다.
        String control = sign(validSecret, "mini-fms", now, now.plusSeconds(300));
        mockMvc.perform(get("/api/v1/geofences").header(HttpHeaders.AUTHORIZATION, "Bearer " + control))
                .andExpect(status().isOk());

        for (String token : List.of(expired, forged, wrongIssuer, "not-a-jwt")) {
            mockMvc.perform(get("/api/v1/geofences").header(HttpHeaders.AUTHORIZATION, "Bearer " + token))
                    .andExpect(status().isUnauthorized());
        }
    }

    @Test
    void 만료_시각_exp_가_없는_토큰은_401() throws Exception {
        NimbusJwtEncoder encoder = new NimbusJwtEncoder(new ImmutableSecret<>(new SecretKeySpec(
                "test-only-secret-0123456789-abcdefghijklmnop".getBytes(StandardCharsets.UTF_8), "HmacSHA256")));
        JwtClaimsSet claims = JwtClaimsSet.builder()
                .issuer("mini-fms").subject("forever").issuedAt(Instant.now())
                .claim("scope", Scopes.FLEET_READ)
                .build(); // exp 없음 → 영구 토큰
        String token = encoder.encode(JwtEncoderParameters.from(JwsHeader.with(MacAlgorithm.HS256).build(), claims)).getTokenValue();

        mockMvc.perform(get("/api/v1/geofences").header(HttpHeaders.AUTHORIZATION, "Bearer " + token))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void 헬스체크는_공개이고_개발용_토큰_발급은_테스트_프로필에서_꺼져_있다() throws Exception {
        mockMvc.perform(get("/actuator/health")).andExpect(status().isOk());
        mockMvc.perform(post("/api/v1/auth/dev-token").contentType(MediaType.APPLICATION_JSON).content("{\"subject\":\"x\"}"))
                .andExpect(status().isUnauthorized());
    }

    private static String sign(String secret, String issuer, Instant issuedAt, Instant expiresAt) {
        NimbusJwtEncoder encoder = new NimbusJwtEncoder(new ImmutableSecret<>(
                new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256")));
        JwtClaimsSet claims = JwtClaimsSet.builder()
                .issuer(issuer).subject("tester").issuedAt(issuedAt).expiresAt(expiresAt)
                .claim("scope", Scopes.FLEET_READ)
                .build();
        return encoder.encode(JwtEncoderParameters.from(JwsHeader.with(MacAlgorithm.HS256).build(), claims)).getTokenValue();
    }
}
