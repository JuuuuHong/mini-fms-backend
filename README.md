# Mini FMS Backend

차량 관제(FMS, Fleet Management System)와 자율협력주행(C-ITS)용 백엔드 코어 파이프라인.
텔레매틱스 수집 → 실시간 위치(Redis GEO)·과거 궤적(ClickHouse) 분리 저장, 메시지 멱등 처리, 지오펜스 진입·이탈 감지,
원격 제어 명령 사가(보상 트랜잭션), C-ITS 돌발상황 전파와 관제 이력 검색(Elasticsearch), JWT 인증,
지오펜스(다각형) 반납 구역 판정과 라이드 요금 정산(분산 락·결제 요청 트랜잭셔널 아웃박스)을 담았다.

| 영역 | 기술 |
|---|---|
| 언어·프레임워크 | Java 21, Spring Boot 3.5, Spring Kafka, Spring Data Redis(Lettuce), Spring Security(OAuth2 Resource Server) |
| 메시지 브로커 | Apache Kafka 3.9 (KRaft, ZooKeeper 없음) |
| 실시간 상태 | Redis 7.4 (GEO, Lua 스크립트) |
| 시계열 | ClickHouse 24.8 (ReplacingMergeTree) |
| 관제 이력 검색 | Elasticsearch 8.18 (Java API Client, external 버전) |
| 라이드·정산 | PostgreSQL 17 (Flyway, 결제 요청 아웃박스), Redisson 3.52 (분산 락), JTS 1.20 (다각형·STRtree) |
| 빌드·테스트 | Gradle(Kotlin DSL), JUnit 5, Testcontainers |

## 3분 데모

필요한 것: JDK 21, Docker, `jq`. 포트 9092·6379·8123·9200·5432·8080 을 쓴다(이미 쓰는 포트가 있으면 [로컬 실행](#로컬-실행)처럼 바꿔 띄운다).
아래 블록을 순서대로 한 터미널에 붙여 넣으면 된다.

```bash
# 0) 인프라(Kafka·Redis·ClickHouse·Elasticsearch·PostgreSQL) + 앱(시뮬레이터 차량 10대가 1초마다 위치를 보낸다)
docker compose up -d --wait
./gradlew bootRun > /tmp/mini-fms.log 2>&1 &
until curl -sf localhost:8080/actuator/health > /dev/null; do sleep 2; done; sleep 3

# 개발용 토큰(local 프로필에서만 열리는 발급 API)
TOKEN=$(curl -s -X POST localhost:8080/api/v1/auth/dev-token -H 'Content-Type: application/json' -d '{"subject":"demo"}' | jq -r .accessToken)
AUTH="Authorization: Bearer $TOKEN"
```

**① 최신 위치 조회** (Redis GEO + 최신 상태 해시)

```bash
curl -s localhost:8080/api/v1/vehicles/V-010 -H "$AUTH" | jq -c '{vehicleId: .latest.vehicleId, latitude: .latest.latitude, longitude: .latest.longitude, speed: .latest.speed, measuredAt: .latest.timestamp, controlState}'
# {"vehicleId":"V-010","latitude":37.57491,"longitude":126.96747,"speed":59.1,"measuredAt":"2026-11-25T07:41:40.099Z","controlState":{"door":"UNLOCKED","engine":"ON"}}
```

**② 응답하지 않는 차량에 원격 제어 → TIMEOUT 과 롤백** (V-010 은 시뮬레이터에서 무응답 차량)

```bash
CID=$(curl -s -X POST localhost:8080/api/v1/vehicles/V-010/commands -H "$AUTH" -H 'Content-Type: application/json' -d '{"type":"DOOR_LOCK"}' | jq -r .commandId)
curl -s localhost:8080/api/v1/vehicles/V-010 -H "$AUTH" | jq -c .controlState
# {"door":"LOCKED","engine":"ON"}        ← 요청 즉시 목표 값을 선반영
sleep 6
curl -s localhost:8080/api/v1/vehicles/V-010/commands/$CID -H "$AUTH" | jq -c '{status, failureReason, rolledBack}'
# {"status":"TIMEOUT","failureReason":"NO_ACK_WITHIN_5000MS","rolledBack":true}
curl -s localhost:8080/api/v1/vehicles/V-010 -H "$AUTH" | jq -c .controlState
# {"door":"UNLOCKED","engine":"ON"}      ← 5초 안에 응답이 없어 이전 값으로 롤백
```

**③ 보상 이벤트 확인** (vehicle-command-compensations)

```bash
docker exec fms-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic vehicle-command-compensations --from-beginning --timeout-ms 5000 2>/dev/null | grep "$CID" | jq -c '{commandId, failedStatus, reason, field, restoredValue, revertedValue}'
# {"commandId":"cc5250b0-…","failedStatus":"TIMEOUT","reason":"NO_ACK_WITHIN_5000MS","field":"door","restoredValue":"UNLOCKED","revertedValue":"LOCKED"}
```

**④ 돌발상황 접수 → 반경 2km 경고 → 관제 이력 조회** (Redis GEO → Kafka → Elasticsearch)

```bash
INCIDENT_ID=INC-$(date +%s)
curl -s -X POST localhost:8080/api/v1/incidents -H "$AUTH" -H 'Content-Type: application/json' \
  -d "{\"incidentId\":\"$INCIDENT_ID\",\"type\":\"ROAD_DEBRIS\",\"latitude\":37.5665,\"longitude\":126.9780,\"sourceId\":\"RSU-SEOUL-017\",\"occurredAt\":$(date +%s)000,\"description\":\"2차로 낙하물\"}" | jq -c
# {"incidentId":"INC-1795592512","duplicate":false,"recipients":10,"vehicleIds":["V-006","V-009", …]}

docker exec fms-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic c-its-incident-alerts --from-beginning --timeout-ms 5000 2>/dev/null | grep "$INCIDENT_ID" | head -2 | jq -c '{vehicleId, distanceMeters, message}'
# {"vehicleId":"V-009","distanceMeters":611,"message":"도로 낙하물 발생: 약 611m 앞 구간 주의 운전"}

sleep 2   # 처리 결과 → Kafka → 색인 소비자 → Elasticsearch (비동기)
curl -s localhost:8080/api/v1/incidents/$INCIDENT_ID -H "$AUTH" | jq -c
# {"incidentId":"INC-1795592512","type":"ROAD_DEBRIS","sourceId":"RSU-SEOUL-017","latitude":37.5665,"longitude":126.978,
#  "occurredAt":"2026-11-25T07:41:52Z","status":"COMPLETED","targetCount":10,"confirmedCount":10,"failedCount":0, …}
curl -s "localhost:8080/api/v1/incidents?from=$(date -u +%Y-%m-%dT00:00:00Z)&to=$(date -u +%Y-%m-%dT23:59:59Z)&type=ROAD_DEBRIS" -H "$AUTH" \
  | jq -c '{totalElements, totalPages, first: .content[0] | {incidentId, status, targetCount, confirmedCount}}'
# {"totalElements":1,"totalPages":1,"first":{"incidentId":"INC-1795592512","status":"COMPLETED","targetCount":10,"confirmedCount":10}}
```

**⑤ Elasticsearch 를 멈춰도 접수·경고는 계속되고, 복구 뒤 이력이 채워진다**

```bash
docker compose stop elasticsearch
INCIDENT_ID2=INC-$(date +%s)-ES
curl -s -X POST localhost:8080/api/v1/incidents -H "$AUTH" -H 'Content-Type: application/json' \
  -d "{\"incidentId\":\"$INCIDENT_ID2\",\"type\":\"ACCIDENT\",\"latitude\":37.5665,\"longitude\":126.9780,\"sourceId\":\"RSU-SEOUL-017\",\"occurredAt\":$(date +%s)000}" | jq -c '{incidentId, recipients}'
# {"incidentId":"INC-1795592522-ES","recipients":10}       ← 접수·경고 발행 정상
curl -s -o /dev/null -w '이력 조회: %{http_code}\n' localhost:8080/api/v1/incidents/$INCIDENT_ID2 -H "$AUTH"
# 이력 조회: 503                                            ← 검색만 장애
docker compose up -d --wait elasticsearch
until [ "$(curl -s -o /dev/null -w '%{http_code}' localhost:8080/api/v1/incidents/$INCIDENT_ID2 -H "$AUTH")" = 200 ]; do sleep 2; done
curl -s localhost:8080/api/v1/incidents/$INCIDENT_ID2 -H "$AUTH" | jq -c '{incidentId, status, targetCount, confirmedCount}'
# {"incidentId":"INC-1795592522-ES","status":"COMPLETED","targetCount":10,"confirmedCount":10}   ← 밀린 결과 색인
```

**⑥ 킥보드 대여 → 벌금 구역 반납 → 결제 요청 → 원격 잠금** (PostgreSQL 트랜잭셔널 아웃박스 → Kafka, 명령 사가)

```bash
# 기기 등록 + 단말 위치 보고(광화문 횡단보도 = 벌금 구역). 위치는 Kafka → Redis 로 비동기 반영된다.
curl -s -X PUT localhost:8080/api/v1/devices/KB-001 -H "$AUTH" | jq -c '{deviceId, state}'
# {"deviceId":"KB-001","state":"AVAILABLE"}
curl -s -o /dev/null -X POST localhost:8080/api/v1/telematics -H "$AUTH" -H 'Content-Type: application/json' \
  -d "{\"vehicleId\":\"KB-001\",\"latitude\":37.5706,\"longitude\":126.9770,\"speed\":0,\"batterySoc\":71,\"engineStatus\":\"OFF\",\"timestamp\":$(date +%s)000}"
sleep 2

RIDE=$(curl -s -X POST localhost:8080/api/v1/rides -H "$AUTH" -H 'Content-Type: application/json' -d '{"deviceId":"KB-001"}' | jq -r .rideId)
# 반납은 대여한 뒤에 측정한, 정차한(3km/h 이하) 단말 보고로만 판정한다. 단말이 세운 자리를 보고한다(측정 시각이 초 단위라 1초 뒤).
sleep 1
curl -s -o /dev/null -X POST localhost:8080/api/v1/telematics -H "$AUTH" -H 'Content-Type: application/json' \
  -d "{\"vehicleId\":\"KB-001\",\"latitude\":37.5706,\"longitude\":126.9770,\"speed\":0,\"batterySoc\":71,\"engineStatus\":\"OFF\",\"timestamp\":$(date +%s)000}"
sleep 2
curl -s -X POST localhost:8080/api/v1/rides/$RIDE/return -H "$AUTH" | jq -c '{status, zoneType: .returnInfo.zoneType, fare}'
# {"status":"COMPLETED","zoneType":"PENALTY_ZONE","fare":{"unlockFee":1000,"perMinuteFee":150,"ridingMinutes":1,"ridingFee":150,
#  "zoneAdjustment":10000,"subscriptionDiscount":0,"total":11150}}
curl -s localhost:8080/api/v1/devices/KB-001 -H "$AUTH" | jq -c .state     # "LOCKED"     ← 반납 확정, 잠금 확인 전
sleep 3
curl -s localhost:8080/api/v1/devices/KB-001 -H "$AUTH" | jq -c .state     # "AVAILABLE"  ← DOOR_LOCK 명령 ACK

docker exec fms-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic ride-payment-requests --from-beginning --timeout-ms 5000 2>/dev/null | grep "$RIDE" | jq -c '{amount, currency, idempotencyKey}'
# {"amount":11150,"currency":"KRW","idempotencyKey":"ride-payment:ffdbdccf-…"}

# 반납 불가 구역(청계천)으로 옮겨 반납하면 422 이고 라이드는 계속된다.
RIDE2=$(curl -s -X POST localhost:8080/api/v1/rides -H "$AUTH" -H 'Content-Type: application/json' -d '{"deviceId":"KB-001"}' | jq -r .rideId)
sleep 1
curl -s -o /dev/null -X POST localhost:8080/api/v1/telematics -H "$AUTH" -H 'Content-Type: application/json' \
  -d "{\"vehicleId\":\"KB-001\",\"latitude\":37.5690,\"longitude\":126.9850,\"speed\":0,\"batterySoc\":70,\"engineStatus\":\"OFF\",\"timestamp\":$(date +%s)000}"
sleep 2
curl -s -X POST localhost:8080/api/v1/rides/$RIDE2/return -H "$AUTH" | jq -c '{status, reason, zoneId}'
# {"status":422,"reason":"NO_RETURN_ZONE","zoneId":"CHEONGGYECHEON"}
```

구독권은 `subscriptions` 테이블에 넣고(판매는 범위 밖), 대여할 때 쓸 구독권을 지정한다(지정하지 않으면 정가):
`docker exec fms-postgres psql -U fms -d fms -c "INSERT INTO subscriptions (user_id, plan_code, starts_at, ends_at) VALUES ('demo', 'PASS_30', now(), now() + interval '30 days')"`
→ `-d '{"deviceId":"KB-001","subscriptionPlan":"PASS_30"}'`. 합계가 0원인 라이드는 결제 요청을 만들지 않는다.

정리: `kill $(lsof -t -iTCP:8080 -sTCP:LISTEN)` 후 `docker compose down -v`.

## 목차

1. [디렉터리 구조](#디렉터리-구조)
2. [아키텍처](#아키텍처)
3. [핵심 설계](#핵심-설계)
4. [데이터 모델](#데이터-모델)
5. [API 명세](#api-명세)
6. [로컬 실행](#로컬-실행)
7. [테스트](#테스트)
8. [남아 있는 장애 시나리오와 확장 포인트](#남아-있는-장애-시나리오와-확장-포인트)
9. [후속 요구사항 — 돌발상황 관제 이력](#후속-요구사항--돌발상황-관제-이력)
10. [후속 요구사항 — 반납 구역 판정과 라이드 요금 정산](#후속-요구사항--반납-구역-판정과-라이드-요금-정산)

## 디렉터리 구조

```
mini-fms-backend
├── build.gradle.kts / settings.gradle.kts / gradlew
├── docker-compose.yml                 # Kafka(KRaft)·Redis·ClickHouse·Elasticsearch·PostgreSQL (+ app, kafka-ui 프로필)
├── Dockerfile                         # 멀티 스테이지 빌드
└── src
    ├── main/java/com/minifms
    │   ├── MiniFmsApplication.java
    │   ├── common
    │   │   ├── config                 # Kafka 토픽·리스너 팩토리, DataSource 2개(ClickHouse·PostgreSQL), Redisson, Clock, @ConfigurationProperties
    │   │   ├── idempotency            # IdempotencyGuard: Redis 2단계(선점→완료) 멱등 처리
    │   │   ├── lock                   # DistributedLock: Redisson RLock(watchdog) 래퍼
    │   │   ├── outbox                 # Redis 아웃박스 → Kafka 릴레이
    │   │   ├── kafka · redis · geo · metrics · web(ProblemDetail 오류 응답)
    │   ├── telematics
    │   │   ├── api                    # POST /api/v1/telematics[/batch]
    │   │   ├── ingest                 # 검증 → telematics-raw-events 발행
    │   │   ├── consumer               # RealtimeLocationRouter(Redis), TelemetryHistoryWriter(ClickHouse)
    │   │   └── history                # ClickHouse 저장소
    │   ├── vehicle                    # Redis GEO 저장소(위치+지오펜스 원자 갱신), 반경·상세·궤적 조회 API
    │   ├── geofence                   # 진입·이탈 판정(히스테리시스)
    │   ├── command                    # 원격 제어 사가: API, 저장소(Lua), ACK 소비자, 타임아웃 스케줄러
    │   ├── incident                   # C-ITS 돌발상황: 반경 2km 경고 브로드캐스트, 처리 결과 기록
    │   │   └── history                # Elasticsearch 색인 소비자·저장소·검색 서비스
    │   ├── ride                       # 대여·반납 오케스트레이션(RideService), 라이드 트랜잭션, 저장소
    │   │   ├── zone                   # 반납 구역 판정(JTS 다각형 + STRtree)
    │   │   ├── fare                   # 요금 계산(요금표 스냅샷·구독권·구역 조정)
    │   │   ├── device                 # 기기 상태 머신, 반납 뒤 원격 잠금 조정기
    │   │   ├── payment                # 결제 요청 아웃박스(PostgreSQL) → Kafka 릴레이
    │   │   └── api                    # /api/v1/rides, /api/v1/devices
    │   ├── security                   # JWT 검증 필터 체인, scope, 개발용 토큰 발급
    │   └── simulator                  # 가상 차량 10대(텔레메트리) + 가상 TCU(명령 응답)
    ├── main/resources
    │   ├── application.yml / application-local.yml
    │   ├── clickhouse/schema.sql
    │   ├── db/migration/*.sql         # PostgreSQL 스키마(Flyway): 기기·라이드·구독권·결제 아웃박스
    │   └── scripts/*.lua              # 원자적 상태 변경 10개
    └── test
        ├── java/com/minifms           # Testcontainers 통합 테스트 + 단위 테스트
        └── resources/application-test.yml
```

## 아키텍처

```mermaid
flowchart LR
    subgraph Edge["차량·도로 (시뮬레이터)"]
        SIM["가상 차량 10대<br/>1초 주기 텔레메트리"]
        TCU["가상 TCU<br/>명령 응답"]
        RSU["RSU·도로 센서"]
        RIDER["이용자 앱"]
    end

    subgraph App["Mini FMS Backend"]
        SEC["JWT 필터<br/>(Bearer, scope)"]
        ING["수집 API·서비스"]
        RT["RealtimeLocationRouter<br/>group: fms-realtime-router"]
        HW["TelemetryHistoryWriter<br/>group: fms-history-writer"]
        CMD["VehicleCommandService<br/>(사가 오케스트레이터)"]
        TO["타임아웃 스케줄러"]
        INC["IncidentBroadcastService"]
        IDX["IncidentResultIndexer<br/>group: fms-incident-indexer"]
        RELAY["OutboxRelay"]
        Q["조회 API"]
        RIDE["RideService<br/>(기기 락·구역 판정·요금)"]
        PREL["PaymentOutboxRelay"]
        LREC["DeviceLockReconciler"]
    end

    subgraph Kafka
        RAW[(telematics-raw-events)]
        DLT[(telematics-raw-events.DLT)]
        GA[(vehicle-geofence-alerts)]
        VC[(vehicle-commands)]
        ACK[(vehicle-command-acks)]
        COMP[(vehicle-command-compensations)]
        IA[(c-its-incident-alerts)]
        IR[(c-its-incident-results)]
        PAY[(ride-payment-requests)]
    end

    REDIS[("Redis<br/>GEO·최신 상태·지오펜스·멱등키<br/>명령 상태·아웃박스·분산 락")]
    CH[("ClickHouse<br/>telematics_history")]
    ES[("Elasticsearch<br/>c-its-incidents")]
    PG[("PostgreSQL<br/>기기·라이드·구독권<br/>결제 아웃박스")]

    SIM --> ING --> RAW
    RAW --> RT
    RAW --> HW
    RT -->|"멱등 선점 · 위치+지오펜스+알림 적재(Lua 1회)"| REDIS
    HW -->|"멱등 선점"| REDIS
    HW -->|"배치 INSERT"| CH
    HW -->|"무효 메시지"| DLT
    RELAY -->|"아웃박스 꺼내기·확인 후 삭제"| REDIS
    RELAY --> GA
    RELAY --> COMP
    RELAY --> IR
    SEC --> CMD
    CMD -->|"잠금·스냅샷·선반영(Lua)"| REDIS
    CMD --> VC --> TCU --> ACK --> CMD
    TO -->|"마감 초과 → 롤백 + 보상(Lua)"| REDIS
    RSU --> SEC --> INC
    INC -->|"GEOSEARCH 2km · 처리 결과 적재"| REDIS
    INC --> IA
    IR --> IDX -->|"bulk, external 버전"| ES
    SEC --> Q
    Q --> REDIS
    Q --> CH
    Q --> ES
    RIDER --> SEC --> RIDE
    RIDE -->|"기기 락(Redisson) · 단말 최신 위치"| REDIS
    RIDE -->|"TX: 라이드 확정 · 기기 LOCKED · 결제 요청 적재"| PG
    PREL -->|"FOR UPDATE SKIP LOCKED"| PG
    PREL --> PAY
    LREC -->|"LOCKED 기기 → AVAILABLE"| PG
    LREC -->|"DOOR_LOCK 요청"| CMD
```

- **수집 경로는 가볍게**: API·시뮬레이터는 검증만 하고 `telematics-raw-events` 에 싣는다(키 = vehicleId → 같은 차량은 같은 파티션에서 순서 유지).
- **소비처별 consumer group**: 실시간(Redis)·이력(ClickHouse)·관제 이력 색인(Elasticsearch)이 서로 다른 group 이다. 저장소 하나의 장애가 다른 경로를 막지 않는다.
- **조회는 CQRS 의 조회 측**: 현재 상태는 Redis, 과거 궤적은 ClickHouse, 돌발상황 이력은 Elasticsearch 에서 읽는다.
- **돈이 걸린 상태는 PostgreSQL**: 기기 상태 전이·라이드 확정·결제 요청 적재는 한 ACID 트랜잭션이어야 해서 관계형 DB 에 둔다.

## 핵심 설계

### 1. 메시지 멱등성 — Redis 2단계 선점

멱등키 = `vehicleId:timestamp(ms)`. 키 하나는 아래 상태를 오간다(`IdempotencyGuard`, `scripts/idempotency_*.lua`).

```mermaid
stateDiagram-v2
    [*] --> 선점: claim (SET NX PX lease, 값=처리 토큰)
    선점 --> 완료: complete (내 토큰일 때만 SET DONE PX window)
    선점 --> [*]: release (내 토큰일 때만 DEL) / lease 만료
    완료 --> [*]: window(기본 10분) 만료
```

| claim 결과 | 의미 | 소비자 동작 |
|---|---|---|
| `CLAIMED` | 이번 시도가 선점 | 처리 후 complete, 실패 시 release |
| `DUPLICATE` | 이미 DONE, 또는 같은 배치 안에서 앞서 나온 메시지 | 버림(지표 `fms.telematics.duplicates`) |
| `IN_FLIGHT` | 다른 시도가 선점 중(리밸런스 직후 등) | 버리지 않고 재시도(리스가 풀리면 처리) |

- "처리 전에 DONE 표시"만 하는 흔한 방식은 처리가 실패해 재전달된 메시지를 중복으로 오인해 **잃는다**. 그래서 선점과 완료를 나눴다.
- 배치 전체를 Lua 한 번(Redis 왕복 1회)으로 선점한다. complete·release 는 토큰을 비교해, 리스가 만료돼 다른 시도가 다시 선점한 키를 건드리지 않는다.
- 같은 배치 안의 중복은 "이번 실행에서 이미 본 키"로 가린다. Redis 클라이언트(Lettuce)가 재연결하며 선점 스크립트를 다시 보내도 자기 선점을 중복으로 오인해 메시지를 버리지 않는다.
- 소비처마다 namespace 를 따로 둔다("완료"는 그 소비처가 반영했다는 뜻이므로).
- **2중 방어**: 윈도우(10분) 밖의 늦은 재전송은 ClickHouse `ReplacingMergeTree(ORDER BY vehicle_id, event_time)` 가 병합 때 합친다. 궤적 조회는 `FINAL` 로 한다.

**측정 시각 범위와 이력 저장**

- 받을 수 있는 측정 시각은 **과거 7일 ~ 미래 30초**다(`fms.telematics.max-past-age`·`max-future-skew`). 각각 단말의 오프라인 버퍼 기간과 시계 오차에 대한 가정이며 설정으로 바꾼다.
  범위가 없으면 날짜가 100일 넘게 퍼진 묶음 하나를 ClickHouse 가 거부(`Too many partitions for single INSERT block`)하고, 이력 소비자가 같은 배치를 무기한 재시도해 그 파티션의 이력 기록이 멈춘다.
- 이력은 **UTC 날짜(= ClickHouse 파티션)별로 INSERT** 하고, 저장한 날짜의 멱등키는 바로 DONE 으로 확정한다. 뒤 날짜가 실패해 배치가 재처리돼도 앞 날짜는 다시 저장하지 않는다(병합 전에도 중복 행 없음).
- DB 오류는 DLT 로 보내지 않고 재시도한다. 검증을 통과한 레코드는 타입·범위가 맞아 레코드 때문에 INSERT 가 결정적으로 실패할 근거가 없기 때문이다. DLT 에는 해석·검증에 실패한 레코드만 보낸다.
- DLT 발행이 확인되지 않으면 예외로 **배치의 오프셋을 확정하지 않는다**. 재처리 때 정상 레코드는 이미 DONE 이라 다시 저장되지 않고 무효 레코드만 다시 DLT 로 간다.

### 2. 실시간 위치와 지오펜스 — 한 번의 원자 연산

`scripts/location_update.lua` 하나가 아래를 함께 한다.

1. 저장된 `timestamp` 보다 **최신일 때만** 진행(늦게 도착한 과거 좌표는 위치도 지오펜스도 바꾸지 않는다).
2. `GEOADD` + 최신 상태 해시 + `last-seen` 갱신.
3. 지오펜스별 전이 후보를 `SADD`/`SREM` 으로 적용하고, 실제로 바뀐 것만 알림을 아웃박스에 넣는다.

위치 저장과 지오펜스 판정을 따로 하면 그 사이 실패했을 때 Kafka 재전달 메시지는 "이미 반영된 시각"이라 판정을 건너뛰고,
**위치만 갱신된 채 진입 알림이 사라진다**. 한 스크립트로 합쳐 둘 다 반영되거나 둘 다 반영되지 않게 했다
(회귀 테스트 `RealtimeRedeliveryIT`: 위치 저장 직후 한 번 실패 → 재전달 → ENTER 정확히 1건).

지오펜스 판정 규칙(히스테리시스):

```
거리 ≤ radius                → 안 (ENTER 후보)
거리 > radius + hysteresis   → 밖 (EXIT 후보)
그 사이(기본 30m 완충 구간)   → 후보 없음 — 경계에서 GPS 가 흔들려 알림이 반복(flapping)되는 것을 막는다
```

반경 조회: `GEOSEARCH FROMLONLAT … BYRADIUS … WITHDIST ASC` → `last-seen`(ZMSCORE) 으로 **활성 차량**(기본 60초 안에 수신)만 남김 → 최신 상태를 파이프라인 한 번으로 읽는다.

### 3. 아웃박스 → Kafka 릴레이

지오펜스 알림·명령 보상·돌발상황 처리 결과는 상태 변경과 같은 Lua 스크립트(또는 같은 요청) 안에서 Redis 아웃박스(`fms:outbox`)에 넣고, `OutboxRelay` 가 Kafka 로 at-least-once 발행한다.

- 잠금을 가진 인스턴스 하나만 릴레이한다. 앞에서부터 발행이 확인된 항목만 지운다.
- **삭제는 `outbox_ack.lua` 로만 한다**: "아직 잠금 소유자인가 + 목록 앞부분이 내가 발행한 항목(SHA-1)과 같은가"를 원자적으로 확인한 뒤 같은 만큼만 `LTRIM`.
  발행 확인을 기다리는 사이 잠금이 만료돼 다른 릴레이가 같은 항목을 처리·삭제한 경우, "읽은 건수만큼 LTRIM" 은 **그다음의 미발행 항목을 지운다**
  (회귀 테스트 `OutboxRelayIT`).
- 중복 발행은 가능하다(at-least-once). 소비자는 `alertId`·`eventId`·`incidentId + version` 으로 거른다.

### 4. 원격 제어 명령 — 사가(오케스트레이션)와 보상

```mermaid
stateDiagram-v2
    [*] --> REQUESTED: POST /commands (잠금·스냅샷·목표값 선반영)
    REQUESTED --> TRANSMITTED: vehicle-commands 발행 확인(acks=all)
    REQUESTED --> FAILED: 기록되지 않은 것이 확실한 전송 실패 → 롤백·보상
    REQUESTED --> ACKNOWLEDGED: 발행 확인이 늦어도(결과 모름) 차량 ACK 가 오면
    REQUESTED --> TIMEOUT: 결과 모름 + 마감까지 ACK 없음 → 롤백·보상
    TRANSMITTED --> ACKNOWLEDGED: 차량 ACK(SUCCESS)
    TRANSMITTED --> FAILED: 차량 거절(REJECTED) → 롤백·보상
    TRANSMITTED --> TIMEOUT: 마감(5초) 초과 → 롤백·보상
    ACKNOWLEDGED --> [*]
    FAILED --> [*]
    TIMEOUT --> [*]
```

```mermaid
sequenceDiagram
    autonumber
    participant C as 관제 클라이언트
    participant API as VehicleCommandService
    participant R as Redis (Lua)
    participant K as Kafka
    participant T as 차량 TCU
    participant S as 타임아웃 스케줄러

    C->>API: POST /api/v1/vehicles/V-010/commands {DOOR_LOCK}
    API->>R: command_create.lua<br/>잠금 SET NX · 이전값 스냅샷 · door=LOCKED 선반영 · 마감 ZADD
    API->>K: vehicle-commands (expiresAt 포함)
    API->>R: REQUESTED → TRANSMITTED (CAS)
    API-->>C: 202 Accepted + Location
    K-->>T: 명령 전달 (응답 없음)
    loop 500ms 마다
        S->>R: 마감 지난 명령 조회 (ZRANGEBYSCORE)
    end
    S->>R: command_transition.lua<br/>TRANSMITTED → TIMEOUT (CAS) · door=UNLOCKED 롤백 · 잠금 해제 · 보상 이벤트 아웃박스 적재
    R-->>K: OutboxRelay → vehicle-command-compensations
    C->>API: GET /commands/{id} → TIMEOUT, rolledBack=true
```

- **로컬 트랜잭션 = Lua 스크립트**: 생성(잠금·스냅샷·선반영·마감 등록)과 보상(상태 전이·롤백·잠금 해제·보상 이벤트 적재)이 각각 원자적이다.
- **compare-and-set 전이**: ACK 와 타임아웃이 동시에 와도 종료 상태는 한 번만 확정된다(`VehicleCommandUncertaintyIT`: 경합 30건에서 종료 확정·제어 상태·보상 이벤트가 명령마다 일치).
- **차량당 진행 중 명령 1개**(409 Conflict): 롤백할 "이전 값"이 다른 명령에 의해 흐트러지지 않게 한다. 잠금 TTL(마감 + 10초)로 영구 잠김을 막는다.
- **전송 결과를 모르면 보상하지 않는다**: 발행 확인 시간 초과·재시도 가능한 Kafka 오류는 "실패"가 아니라 "브로커가 받았는지 모름"이다.
  프로듀서는 계속 재전송하므로 이미 차량에 전달됐을 수 있다. 명령을 진행 중(REQUESTED)으로 두고 ACK 면 ACKNOWLEDGED, 마감까지 없으면 스케줄러가 TIMEOUT 으로 보상한다.
  기록되지 않은 것이 확실한 실패(직렬화·크기·권한 오류)만 바로 FAILED 로 보상한다(`fms.command.transmit_unknown` 지표).
- **차량 실행 마감(expiresAt) = 서버 마감 − 1.5초**(`fms.command.device-execution-margin`, ACK 전달 지연에 대한 가정): 마감 직전에 실행한 명령의 ACK 가 서버 마감 전에 도착할 여유다.
- **ACK 소비**: Redis 연결 장애·시간 초과처럼 복구될 수 있는 오류만 무기한 재시도한다(그동안 ACK 를 버리지 않는다). 그 밖의 오류와 해석할 수 없는 메시지는 로그를 남기고 넘어가 뒤따르는 ACK 를 막지 않는다.
- **보상의 의미**: 보상은 서버가 기록한 제어 상태(shadow)를 이전 값으로 되돌리는 것이다. 실제 차량 상태는 차량이 보고해야 확인된다.
  TIMEOUT 확정 뒤 성공 ACK 가 늦게 오면 서버 기록과 실제 차량이 다를 수 있어, `fms.command.late_acks` 지표와 경고 로그로 확인을 요청한다(자동 보정은 후속 과제).

### 5. C-ITS 돌발상황 전파

`POST /api/v1/incidents` (RSU·도로 센서 게이트웨이, `incident:write`) → `IncidentBroadcastService`

1. `incidentId` 로 멱등 선점 — RSU 재전송 시 경고를 다시 보내지 않는다(응답 `duplicate: true`).
2. Redis `GEOSEARCH` 로 돌발 지점 **반경 2km** 의 활성 차량을 찾고 시동 꺼짐(`OFF`)은 뺀다. 신호 대기(`IDLE`)는 위험 구간에 있으므로 포함한다.
3. 차량마다 경고 한 건씩 `c-its-incident-alerts` 로 발행(키 = vehicleId)하고, **건별로 브로커 확인(acks=all)을 기다려 센다**.
   확인 건수는 "발행 마감(`publish-timeout`, 3초) 안에 브로커가 수신을 확인한 수"다. 마감은 보내기 전에 잡고, 브로커 장애로 send 가 즉시 실패하면
   남은 차량에는 보내지 않는다(브로커 장애 때 요청 하나가 "대상 수 × max.block.ms" 동안 스레드를 붙잡지 않게). 레코드 한 건만의 거절이면 나머지는 계속 보낸다.
4. 처리 결과(대상 수·확인 건수·실패 건수·상태)를 아웃박스에 남긴다 → `c-its-incident-results`. 이때 **선점을 아직 가지고 있는지** 같은 Lua 안에서 확인한다.
   처리가 오래 걸려 선점이 만료되고 RSU 재시도가 이어받았다면, 늦게 끝난 처리는 결과를 남기지 않고 409 를 돌려준다(이어받은 처리의 결과가 최신).
5. 하나라도 확인되지 않으면 선점을 풀고 503 → RSU 가 재시도한다. 재시도는 더 큰 버전의 새 결과를 남긴다(이미 받은 차량은 `incidentId` 로 중복 표시를 거른다).

| 처리 상태 | 조건 | 접수 API |
|---|---|---|
| `NO_TARGETS` | 대상 0대 | 202 (`recipients: 0`) |
| `COMPLETED` | 대상 전부 발행 확인 | 202 |
| `PARTIALLY_FAILED` | 일부만 확인 | 503(재시도 요청) |
| `FAILED` | 하나도 확인 안 됨(브로커 장애) | 503(재시도 요청) |

### 6. 돌발상황 관제 이력 — Kafka → Elasticsearch 비동기 색인

```mermaid
sequenceDiagram
    autonumber
    participant RSU as RSU 게이트웨이
    participant INC as IncidentBroadcastService
    participant R as Redis
    participant K as Kafka
    participant IDX as IncidentResultIndexer
    participant ES as Elasticsearch
    participant OP as 관제 운영자

    RSU->>INC: POST /api/v1/incidents
    INC->>R: 멱등 선점 · GEOSEARCH 2km
    INC->>K: c-its-incident-alerts × N (건별 확인)
    INC->>R: 선점 확인 + 처리 결과 버전 발급 + 아웃박스 적재(Lua 1회)
    INC-->>RSU: 202 (또는 일부 실패 시 503)
    R-->>K: OutboxRelay → c-its-incident-results (키 = incidentId)
    K-->>IDX: 배치 소비
    IDX->>ES: bulk index (id = incidentId, version = 결과 버전, version_type = external)
    Note over IDX,ES: 409 version_conflict = 재전달·늦은 도착 → 건너뜀
    OP->>ES: GET /api/v1/incidents?from&to&type (fleet:read)
```

- **문서 ID = `incidentId`, external 버전 = 처리 결과 버전**: Elasticsearch 는 저장된 것보다 큰 버전만 받는다.
  같은 결과의 재전달은 문서를 늘리지 않고, **늦게 도착한 이전 결과는 최신 결과를 덮어쓰지 않는다**. 소비자 쪽 순서 보장이나 잠금이 필요 없다.
- **버전 = max(현재 시각 ms, 마지막 버전 + 1)**, 선점 확인·버전 발급·아웃박스 적재를 한 Lua 로 한다(`incident_record_result.lua`).
  Redis 가 발급 순서를 직렬화하므로 같은 밀리초에도 앞선 결과보다 크고, 키가 사라져도 시각 기반이라 과거로 돌아가지 않는다.
  버전 순서가 처리 순서와 어긋날 수 있는 경우(선점이 만료된 처리가 늦게 끝나 기록)는 선점 확인으로 막는다.
- **처리 결과를 Kafka 에 직접 보내지 않고 아웃박스를 거친다**: 브로커 장애로 경고가 전부 실패(`FAILED`)한 처리도 이력에 남아야 하는데, 그 순간 Kafka 는 받지 못한다.
- **별칭 색인**: 검색·쓰기는 별칭 `c-its-incidents` 로, 실제 색인은 `c-its-incidents-v1`(strict 매핑)이다. 쓰기는 `require_alias` 로 해서
  색인이 사라지면(볼륨 없는 재생성·운영자 삭제) Elasticsearch 가 동적 매핑으로 자동 생성하지 않고 실패하게 한다. 색인 소비자는 그때 매핑을 지정해 다시 만든다.
  매핑을 바꿀 때는 `-v2` 로 재색인한 뒤 별칭만 옮긴다.
- **Elasticsearch 장애**
  - 접수·경고 발행 경로는 Elasticsearch 를 거치지 않는다 → 계속 동작.
  - 색인 소비자는 bulk 실패 시 오프셋을 커밋하지 않고 무기한 재시도(간격 최대 5초). 복구되면 밀린 결과를 처리한다(`c-its-incident-results` 보존 7일).
  - 검색·상세 API 는 503 + `Retry-After: 5`(연결 실패·시간 초과·429·5xx). 그 밖의 Elasticsearch 거절(4xx)은 다시 해도 같으므로 500 이다.
  - Elasticsearch 헬스 지표는 앱 전체 상태(`/actuator/health`)에서 뺐다. 검색 저장소 장애로 앱이 트래픽에서 빠지면 접수까지 멈추기 때문이다.
  - 색인은 첫 색인 때 매핑(`dynamic: strict`)을 지정해 만든다. 앱 기동은 Elasticsearch 에 의존하지 않는다.
- **검색**: 발생 시각 `[from, to)` 필터 + 선택 유형(`term`), 정렬 `occurredAt desc, incidentId asc`(같은 시각에도 순서 고정), `page`·`size` 페이지.
  기간 최대 31일, `size` 최대 100, `page × size` 는 10,000 이하(Elasticsearch `max_result_window`).
  ID 조회는 바로 반영되고, 검색은 refresh(기본 1초) 뒤에 보인다.

### 7. API 인증 — JWT (Spring Security Resource Server)

- `BearerTokenAuthenticationFilter` 가 `Authorization: Bearer <JWT>` 를 꺼내 **HS256 서명·만료(exp/nbf, 시계 오차 60초)·발급자(iss)** 를 검증한다. 세션은 만들지 않는다.
- 권한은 토큰의 `scope` 클레임(공백 구분)으로 준다.

| scope | 허용 |
|---|---|
| `fleet:read` | `GET /api/v1/**`(관제 이력 검색 포함), `/actuator/**`(health·info 제외) |
| `vehicle:control` | `POST /api/v1/vehicles/{id}/commands`, `PUT /api/v1/devices/{id}` |
| `telematics:write` | `POST /api/v1/telematics[/batch]` |
| `incident:write` | `POST /api/v1/incidents` |
| `ride:write` | `POST /api/v1/rides`, `POST /api/v1/rides/{id}/return`, 본인 `GET /api/v1/rides/{id}` |

- 토큰 없음·서명 불일치·만료·`exp` 없음(영구 토큰)·발급자 불일치 → **401**, scope 부족 → **403**, 그 밖의 경로는 거부(deny by default).
- 서명 키는 `FMS_JWT_SECRET`(32바이트 이상). 비어 있으면 **기동하지 않는다**. `local` 프로필만 개발용 키와 토큰 발급 API 를 켠다.

### 8. 라이드 반납·정산 — 구역 판정, 분산 락, 결제 요청 아웃박스

```mermaid
stateDiagram-v2
    [*] --> AVAILABLE: PUT /devices/{id}
    AVAILABLE --> IN_USE: 대여(CAS)
    IN_USE --> LOCKED: 반납 확정(라이드 COMPLETED·결제 요청 적재와 한 트랜잭션)
    LOCKED --> AVAILABLE: DOOR_LOCK 명령 ACK
    LOCKED --> LOCKED: 잠금 거절·무응답 → 백오프 뒤 다시 요청
```

```mermaid
sequenceDiagram
    autonumber
    participant U as 이용자 앱
    participant S as RideService
    participant R as Redis
    participant DB as PostgreSQL
    participant RL as PaymentOutboxRelay
    participant K as Kafka
    participant RC as DeviceLockReconciler

    U->>S: POST /api/v1/rides/{id}/return
    S->>R: 기기 락 fms:lock:device:{id} (최대 3초 대기, watchdog 연장)
    S->>R: 단말 최신 위치(측정 30초 이내 · 대여 뒤 측정 · 정차)
    S->>S: 구역 판정(STRtree → 다각형) · 요금 계산
    S->>DB: TX{ 라이드 IN_PROGRESS→COMPLETED · 기기 IN_USE→LOCKED · payment_outbox INSERT(합계 > 0) }
    S-->>U: 200 확정 요금 (락 해제)
    loop 200ms
        RL->>DB: 미발행 행 FOR UPDATE SKIP LOCKED
        RL->>K: ride-payment-requests (acks=all, 키 = rideId, 확인은 delivery.timeout 까지 대기)
        RL->>DB: 확인된 행 published_at · COMMIT
    end
    loop 500ms
        RC->>DB: 차례가 된 LOCKED 기기
        RC->>R: DOOR_LOCK 명령(기존 명령 사가)
        RC->>DB: ACKNOWLEDGED 면 LOCKED → AVAILABLE
    end
```

**구역 판정** (`ReturnZoneIndex`)

- 구역은 WKT 다각형(`POLYGON`·`MULTIPOLYGON`, "경도 위도" 순서)으로 설정한다. 기동할 때 읽어 **STRtree**(경계 사각형 R-tree)에 넣고,
  판정은 "좌표를 포함할 수 있는 후보(사각형) → 실제 다각형 포함 검사" 두 단계로 한다. 구역이 많아져도 후보만 정밀 검사한다.
- 겹친 구역은 **더 제한적인 정책 하나**: `NO_RETURN_ZONE > PENALTY_ZONE > DISCOUNT_ZONE > SERVICE_ZONE`(같은 정책끼리는 ID 순).
  어느 구역에도 속하지 않으면 서비스 지역 밖이라 반납을 받지 않는다.
- **경계 위의 점은 구역 안**이다. 좌표는 경도·위도를 평면 좌표로 쓴다(도시 규모에서 측지선과의 차이는 GPS 오차보다 훨씬 작다).
- 자기 교차 다각형, 다각형이 아닌 도형, 중복 ID, **위도·경도를 뒤바꾼 WKT**(위도 범위를 벗어남)는 기동할 때 거부한다.
- Uber H3 육각 셀 근사는 쓰지 않았다. 셀 경계가 실제 구역 경계와 달라, 구역 밖에 세운 이용자에게 10,000원 벌금을 매길 수 있기 때문이다.
- **판정 좌표는 요청 본문이 아니라 단말이 보낸 최신 텔레메트리**(Redis)다. 클라이언트가 좌표를 꾸밀 수 없다. 그 보고가 아래를 모두 만족할 때만 판정하고,
  아니면 422 로 거절한다(라이드는 계속, 단말이 새 위치를 보내면 풀린다).
  - 측정 시각이 30초(`fms.ride.max-location-age`) 안 — 아니면 `LOCATION_STALE`, 위치가 없으면 `LOCATION_UNAVAILABLE`
  - **대여한 뒤에 측정** — 아니면 `LOCATION_BEFORE_RIDE_START`. 대여 전 보고는 이번 라이드가 끝난 자리를 말해 주지 않는다(짧은 라이드에서 생긴다).
  - **정차 중**(속도 ≤ `fms.ride.return-max-speed-kmh` 3) — 아니면 `DEVICE_MOVING`. 달리는 중의 보고는 다음 보고 때 이미 다른 자리라,
    서비스 구역을 지나며 보낸 보고로 바로 옆 벌금·반납 불가 구역의 반납을 판정하게 된다(벌금 회피 또는 억울한 벌금). 3km/h 는 정차 중 GPS 속도 흔들림을 넘는 값이다.
- 같은 기준을 **대여에도** 건다. 위치를 보고하지 않는 기기를 빌려주면 반납할 수 없는 라이드가 생기고 그동안 주행 요금이 쌓이기 때문이다
  (단말은 대여 가능 상태에서도 30초 안에 위치를 보고한다고 가정한다).

**요금** (`FareCalculator`, 원 단위 정수)

```
합계 = 잠금해제료 + 주행시간(분, 시작한 분 올림) × 분당요금 + 구역 조정 − 구독권 할인
구역 조정   = 벌금 구역 +10,000 | 주차 권장 구역 −500(남은 금액까지만) | 서비스 구역 0
구독권 할인 = (잠금해제료 면제면 잠금해제료) + 주행요금 × 할인율 / 100   (원 미만 버림)
```

- **구독권은 이용자가 대여할 때 지정한 것 하나만** 쓴다(`subscriptionPlan`). 서버가 가진 구독권 중에서 고르지 않으며, 지정하지 않으면 정가다.
  요금표에 없으면 422 `SUBSCRIPTION_PLAN_UNKNOWN`, 지금 유효하지 않으면 422 `SUBSCRIPTION_NOT_ACTIVE` — 고른 할인을 조용히 빼고 정가로 대여하지 않는다.
- 잠금해제료·분당요금·구독권은 **대여할 때 라이드에 스냅샷**으로 남겨 반납 때 그 값으로 정산한다. 이용 중에 요금표가 바뀌거나 구독권이 끝나도 대여할 때의 조건을 지킨다.
  구역 할인·벌금은 반납 시점의 설정을 쓴다.
- 주행 시간은 나노초 단위로 분을 올림한다(60.0004초 → 2분). 대여·반납 시각은 DB 정밀도(마이크로초)로 잘라, 응답·결제 요청·DB 의 시각이 같다.
- 구독권은 이용요금(잠금해제료 + 주행요금)만 깎고 **벌금은 깎지 않는다**. 주차 권장 할인은 남은 금액보다 크면 남은 금액까지만 적용한다(합계 ≥ 0, 환불을 만들지 않음).
- 항목의 합이 합계와 같다는 것을 `Fare` 생성자와 DB `CHECK` 가 함께 지킨다.

**동시성 — 분산 락은 1차, DB 가 최종**

- 같은 기기에 대한 대여·반납은 Redisson 락 `fms:lock:device:{deviceId}` 로 줄 세운다. 임대 시간을 주지 않아 **watchdog 이 보유 중 연장**하고,
  프로세스가 죽으면 30초 뒤 풀린다. `lock-wait`(3초) 안에 얻지 못하면 409(`REQUEST_IN_PROGRESS`).
- redisson-spring-boot-starter 는 쓰지 않았다. 스타터는 `RedisConnectionFactory` 를 Redisson 구현으로 바꿔 기존 Lettuce 기반 Lua 스크립트와 예외 변환(503 판정)까지 바꾼다.
  락에 필요한 `RedissonClient` 만 `spring.data.redis` 설정으로 따로 만든다.
- **락은 정합성을 보장하지 않는다**: 긴 GC 정지나 Redis 장애 조치로 락이 풀린 사이 다른 인스턴스가 들어올 수 있다. 그래서 상태 전이는 모두 조건부 갱신이다.
  - `UPDATE devices … WHERE state = 'AVAILABLE'`, `UPDATE rides … WHERE status = 'IN_PROGRESS'`, `UPDATE devices … WHERE state = 'IN_USE' AND current_ride_id = ?`
  - 부분 유니크 인덱스: 기기당·사용자당 `IN_PROGRESS` 라이드 하나, `payment_outbox.ride_id` 유니크(라이드당 결제 요청 하나)
  - 회귀 테스트 `RideConcurrencyIT`: 락 없이 반납 트랜잭션 16건 동시 → 확정 1건·결제 요청 1건.
- **반납은 멱등**: 이미 확정된 라이드는 저장된 결과를 그대로 돌려준다(중복 클릭·재시도에도 결제 요청은 한 건).
- 같은 사용자가 서로 다른 기기를 동시에 빌리면 기기 락이 달라 함께 통과하지만, 사용자 유니크 인덱스가 하나만 받는다(나머지 409 `ACTIVE_RIDE_EXISTS`).
- **대여 응답을 잃고 다시 보내면** 409 `ACTIVE_RIDE_EXISTS` + `activeRideId` 를 받는다. 이용자의 진행 중 라이드를 기기 상태보다 먼저 보기 때문이다
  (기기 상태를 먼저 보면 rideId 없는 `DEVICE_NOT_AVAILABLE` 이 나가 이용자가 자기 라이드를 반납할 길이 없다).
- 락을 잃은 사이 다른 인스턴스가 먼저 확정했으면, 이 요청은 자기 계산(다른 시각·구역)이 아니라 **저장된 결과**를 돌려준다.

**결제 요청 — 트랜잭셔널 아웃박스** (`PaymentOutboxRelay`)

- 반납 API 는 외부 결제 API 를 부르지도, Kafka 에 직접 보내지도 않는다. 라이드 확정과 `payment_outbox` INSERT 가 **한 트랜잭션**이라
  "요금은 확정됐는데 결제 요청이 없음"·"결제 요청은 나갔는데 라이드는 롤백됨"이 생기지 않는다. Kafka 가 멈춰도 반납은 성공하고, 결제 요청은 복구 뒤 나간다.
- 릴레이 한 주기 = 한 트랜잭션: 미발행 행을 `FOR UPDATE SKIP LOCKED` 로 잠가 모두 보낸 뒤, 브로커 확인(acks=all)이 온 행만 `published_at` 을 찍고 커밋한다.
  여러 인스턴스가 함께 돌아도 잠긴 행은 서로 건너뛰므로 같은 행을 동시에 보내지 않는다(별도 잠금 불필요).
- **확인은 프로듀서가 성패를 알려 줄 때까지 기다린다**: `send-timeout`(35초) > `delivery.timeout.ms`(30초)를 기동할 때 확인한다. 더 일찍 포기하면
  프로듀서가 아직 재전송 중인 행의 잠금을 풀어 다음 주기(200ms)가 같은 결제 요청을 또 보낸다(브로커가 잠시 느릴 때마다 사본이 쌓인다).
- at-least-once: 보낸 뒤 확인 전에 죽거나, 프로듀서가 끝내 실패를 알린 레코드가 사실은 기록됐으면 다시 보낸다.
  결제 서비스는 `idempotencyKey`(`ride-payment:{rideId}`, PG 멱등키로 그대로 쓸 수 있다)로 중복 청구를 막는다.
- **합계 0원이면 결제 요청을 만들지 않는다**(PG 는 0원 승인을 받지 않는다). 라이드 기록의 `total_fare = 0` 으로만 남는다.
- 확인된 행은 7일 뒤 지운다(`fms.payment-outbox.retention`).
- Kafka 를 택한 이유: 이미 쓰는 브로커이고, 키(rideId) 단위 보존·재처리가 된다. SQS 로 바꾸려면 릴레이의 send 만 바꾸면 된다.

**반납 뒤 원격 잠금** (`DeviceLockReconciler`)

- 반납 트랜잭션은 기기를 `LOCKED` 로 바꾸기만 한다. 조정기가 주기마다 차례가 된 `LOCKED` 기기에 기존 명령 사가로 `DOOR_LOCK` 을 보내고,
  `ACKNOWLEDGED` 면 `AVAILABLE` 로 돌린다. 할 일을 DB 의 기기 상태로 들고 있어, 반납 직후 프로세스가 죽거나 Redis 가 잠시 멈춰도 다음 주기에 이어서 한다.
- 거절·무응답(`FAILED`·`TIMEOUT`)이면 명령을 비우고 지수 백오프(5초에서 두 배씩, 최대 5분) 뒤 다시 보낸다. 그동안 기기는 `LOCKED` — **잠김이 확인되지 않은 기기는 빌려주지 않는다**.
- **선점 후 발송**: 명령 ID 를 먼저 만들어 기기 행에 조건부로 기록(`LOCKED` 이고 진행 중 명령 없음)한 뒤에 보낸다. 주기 시작 때 읽은 목록은 스냅샷이라,
  조정기 락을 잃은 사이 다른 조정기가 그 기기를 잠금 확인 → 대여(`IN_USE`)까지 진행했을 수 있다. 선점이 실패하면 보내지 않아 **주행 중인 기기를 잠그지 않는다**.
  선점 뒤 발송 전에 죽으면 명령 기록이 없으므로 다음 확인 때 실패로 보고 다시 보낸다.
- 다른 명령(관제의 `ENGINE_STOP` 등)이 진행 중이면(409) 선점을 풀고 끝나기를 기다렸다 보낸다.
- 명령 전송 확인이 오지 않으면(브로커 장애 추정) 그 주기의 남은 기기는 다음 주기로 넘긴다. 기기마다 전송 대기(최대 3초)로 스케줄러 스레드를 오래 붙잡지 않기 위해서다.
  같은 이유로 스케줄러 풀을 8개로 늘렸다(브로커 장애 때 막히는 작업이 스레드를 다 잡아도 Redis 만 쓰는 명령 타임아웃 보상은 돈다).

**장애 격리**: `db` 헬스는 ClickHouse 만 본다. 라이드 DB 장애로 앱 전체가 트래픽에서 빠지면 텔레매틱스 수집·돌발상황 경고까지 멈추기 때문이다.
떠 있는 인스턴스에서 라이드 DB·Redis 연결 실패는 라이드 API 의 503(`Retry-After`)으로만 드러난다. Redisson 은 첫 사용 때 연결한다(지연 초기화).
**커밋 중 연결이 끊긴 경우도 503**이다. Spring 은 커밋이 실패하면 롤백을 시도하는데, HikariCP 가 이미 닫은 연결은 SQLState 없는 예외를 던져
롤백 실패(`TransactionSystemException`)가 원래 예외를 덮는다. 커밋됐는지 모르는 상태라 재시도를 알린다(반납은 멱등, 대여 재시도는 위의 409 로 라이드 ID 를 돌려받는다).
단, **기동 때는 Flyway 마이그레이션이 PostgreSQL 연결을 요구**하므로 PostgreSQL 이 멈춘 동안 재기동한 인스턴스는 뜨지 않는다(ClickHouse 스키마 초기화와 같은 성질).

## 데이터 모델

### Kafka 토픽 (파티션 6)

| 토픽 | 키 | 생산자 → 소비자 | 보존 |
|---|---|---|---|
| `telematics-raw-events` | vehicleId | 수집 API·시뮬레이터 → 실시간 라우터, 이력 기록기 | 3일 |
| `telematics-raw-events.DLT` | vehicleId | 이력 기록기(무효 메시지 원본 + 사유 헤더) | 14일 |
| `vehicle-geofence-alerts` | vehicleId | 아웃박스 릴레이 → (알림 서비스) | 7일 |
| `vehicle-commands` | vehicleId | 명령 서비스 → 차량 TCU | 1일 |
| `vehicle-command-acks` | vehicleId | 차량 TCU → 명령 ACK 소비자 | 1일 |
| `vehicle-command-compensations` | vehicleId | 아웃박스 릴레이 → (고객 앱 푸시·감사 로그) | 7일 |
| `c-its-incident-alerts` | vehicleId | 돌발상황 서비스 → (V2X 푸시 게이트웨이) | 1일 |
| `c-its-incident-results` | incidentId | 아웃박스 릴레이 → 이력 색인 소비자 | 7일 |
| `ride-payment-requests` | rideId | 결제 요청 아웃박스 릴레이 → (결제 서비스) | 7일 |

### Redis 키

| 키 | 타입 | 내용 |
|---|---|---|
| `fms:vehicles:geo` | GEO(zset) | 차량 최신 위치 인덱스 |
| `fms:vehicles:last-seen` | zset | 마지막 수신 시각(score) — 활성 판정·정리 |
| `fms:vehicle:{id}:latest` | hash | 최신 텔레메트리(TTL 7일) |
| `fms:vehicle:{id}:shadow` | hash | 제어 상태(door, engine) |
| `fms:vehicle:{id}:command-lock` | string | 진행 중 commandId(TTL) |
| `fms:command:{id}` | hash | 명령 상태(TTL 24시간) |
| `fms:command:deadlines` | zset | 응답 마감(score) |
| `fms:geofence:{id}:inside` | set | 지오펜스 안 차량 |
| `fms:incident:{id}:result-version` | string | 돌발상황 처리 결과의 마지막 버전(TTL 30일) |
| `fms:dedup:{namespace}:{key}` | string | 멱등키(토큰 또는 DONE) |
| `fms:outbox` / `fms:outbox:dead` | list | 발행 대기 이벤트 / 해석 불가 항목 |
| `fms:lock:device:{id}` | hash(Redisson) | 기기 대여·반납 분산 락(watchdog 30초) |
| `fms:lock:device-lock-reconciler` | hash(Redisson) | 잠금 조정기를 한 인스턴스만 돌리는 락 |

### PostgreSQL (Flyway `db/migration/V1__ride_settlement.sql`)

| 테이블 | 내용 | 지키는 것 |
|---|---|---|
| `devices` | 기기 상태(`AVAILABLE`·`IN_USE`·`LOCKED`), 진행 중 라이드, 잠금 명령 진행(명령 ID·시도 수·다음 확인 시각) | `IN_USE` ⇔ 진행 중 라이드 있음 |
| `rides` | 대여 시점 요금표·구독권 스냅샷, 반납 위치·측정 시각·구역, 요금 항목·합계 | 기기당·사용자당 `IN_PROGRESS` 하나(부분 유니크), 항목 합 = 합계 |
| `subscriptions` | 구독권(사용자·코드·기간). 판매·결제는 범위 밖, 읽기만 한다 | `ends_at > starts_at` |
| `payment_outbox` | 결제 요청(JSON), 토픽·키, 발행 확인 시각 | 라이드당 한 건(`ride_id` 유니크) |

### ClickHouse

```sql
CREATE TABLE IF NOT EXISTS telematics_history
(
    vehicle_id    LowCardinality(String),
    event_time    DateTime64(3, 'UTC'),
    latitude      Float64,
    longitude     Float64,
    speed         Float32,
    battery_soc   Float32,
    engine_status LowCardinality(String),
    ingested_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)   -- 같은 (vehicle_id, event_time) 은 병합 때 하나로
PARTITION BY toYYYYMMDD(event_time)         -- 보존 기간이 지난 파티션을 통째로 삭제
ORDER BY (vehicle_id, event_time)           -- 차량별 시간 범위 조회에 맞춘 정렬 키
TTL toDateTime(event_time) + INTERVAL 90 DAY;
```

### Elasticsearch 색인 — 별칭 `c-its-incidents` → `c-its-incidents-v1` (문서 ID = incidentId)

| 필드 | 타입 | 내용 |
|---|---|---|
| `incidentId`, `type`, `sourceId`, `status` | keyword | 유형·출처·처리 상태 필터 |
| `occurredAt`, `processedAt` | date | 발생 시각(검색·정렬 기준), 처리 시각 |
| `location` | geo_point | 돌발 지점(공간 검색 확장용) |
| `targetCount`, `confirmedCount`, `failedCount` | integer | 대상 수, 브로커 발행 확인 수, 미확인 수 |
| `version` | long | 처리 결과 버전(= 문서 external 버전) |
| `description` | text | 보고 내용 |

## API 명세

오류 응답은 RFC 9457 Problem Details(`application/problem+json`)다.

| 메서드·경로 | scope | 설명 | 주요 응답 |
|---|---|---|---|
| `POST /api/v1/telematics` | `telematics:write` | 텔레메트리 1건 수집 | 202 `{accepted}`, 400 |
| `POST /api/v1/telematics/batch` | `telematics:write` | 최대 1,000건 묶음 수집 | 202, 400 |
| `GET /api/v1/vehicles/nearby?lat&lng&radiusKm[&limit]` | `fleet:read` | 반경 안 활성 차량(가까운 순) | 200, 400 |
| `GET /api/v1/vehicles/{id}` | `fleet:read` | 최신 상태 + 활성 여부 + 제어 상태 | 200, 404 |
| `GET /api/v1/vehicles/{id}/track[?from&to&limit]` | `fleet:read` | 과거 궤적(기본 최근 1시간, 최대 7일) | 200, 400 |
| `GET /api/v1/geofences` | `fleet:read` | 지오펜스와 안에 있는 차량 | 200 |
| `POST /api/v1/vehicles/{id}/commands` | `vehicle:control` | 원격 제어(`DOOR_LOCK`·`DOOR_UNLOCK`·`ENGINE_STOP`) | 202 + Location, 404, 409 |
| `GET /api/v1/vehicles/{id}/commands/{commandId}` | `fleet:read` | 명령 상태 | 200, 404 |
| `POST /api/v1/incidents` | `incident:write` | 돌발상황 보고 → 반경 2km 경고 전파 | 202 `{incidentId, duplicate, recipients, vehicleIds}`, 409(처리 중·선점 만료), 503(발행 미확인·저장소 장애) |
| `GET /api/v1/incidents?from&to[&type&page&size]` | `fleet:read` | 관제 이력 검색(발생 `[from,to)`, 최신순) | 200 `{content, page, size, totalElements, totalPages}`, 400, 503 |
| `GET /api/v1/incidents/{incidentId}` | `fleet:read` | 돌발상황 처리 결과 | 200, 404(색인 전), 503 |
| `PUT /api/v1/devices/{deviceId}` | `vehicle:control` | 대여 기기 등록(ID = 텔레매틱스 vehicleId, 멱등) | 201(새로), 200(이미 있음) |
| `GET /api/v1/devices/{deviceId}` | `fleet:read` | 기기 상태·잠금 진행 | 200, 404 |
| `POST /api/v1/rides` `{deviceId, subscriptionPlan?}` | `ride:write` | 대여(이용자 = 토큰 subject, 구독권은 지정한 것만) | 201 + Location, 404(미등록), 409(`ACTIVE_RIDE_EXISTS` + `activeRideId`·`DEVICE_NOT_AVAILABLE`·`REQUEST_IN_PROGRESS`), 422(`SUBSCRIPTION_PLAN_UNKNOWN`·`SUBSCRIPTION_NOT_ACTIVE`·`LOCATION_STALE`·`LOCATION_UNAVAILABLE`), 503 |
| `POST /api/v1/rides/{rideId}/return` | `ride:write` | 반납: 구역 판정·요금 확정·결제 요청 적재(합계 > 0, 멱등) | 200, 404(남의 라이드 포함), 409, 422(`NO_RETURN_ZONE`·`OUT_OF_SERVICE_AREA`·`LOCATION_STALE`·`LOCATION_UNAVAILABLE`·`LOCATION_BEFORE_RIDE_START`·`DEVICE_MOVING`), 503 |
| `GET /api/v1/rides/{rideId}` | `ride:write`(본인) · `fleet:read`(전체) | 라이드·요금 | 200, 404 |
| `POST /api/v1/auth/dev-token` | 없음(`local` 프로필만) | 개발용 토큰 발급 | 200 |
| `GET /actuator/health` | 없음 | 헬스 체크(Elasticsearch 제외) | 200 |
| `GET /actuator/metrics/{name}` | `fleet:read` | 지표 | 200 |

관제 이력 한 건:

```json
{
  "incidentId": "INC-1795592512",
  "type": "ROAD_DEBRIS",
  "sourceId": "RSU-SEOUL-017",
  "latitude": 37.5665,
  "longitude": 126.978,
  "occurredAt": "2026-11-25T07:41:52Z",
  "description": "2차로 낙하물",
  "status": "COMPLETED",
  "targetCount": 10,
  "confirmedCount": 10,
  "failedCount": 0,
  "version": 1795592512827,
  "processedAt": "2026-11-25T07:41:52.827138Z"
}
```

텔레메트리 본문(Kafka 메시지와 같은 모양): `{"vehicleId":"V-001","latitude":37.5665,"longitude":126.9780,"speed":42.5,"batterySoc":87.2,"engineStatus":"ON","timestamp":1795590000000}`.
`vehicleId` 는 `[A-Za-z0-9_-]`, 위도는 Redis GEO 범위(±85.05112878), `timestamp` 는 epoch ms 이며 과거 7일 ~ 미래 30초 밖이면 거부한다.
Redis·ClickHouse 연결 실패·시간 초과는 503(`Retry-After`), 처리하지 못한 예외는 Problem Details 형식의 500 이다.

## 로컬 실행

```bash
docker compose up -d --wait   # Kafka·Redis·ClickHouse·Elasticsearch
./gradlew bootRun             # local 프로필(개발용 JWT 키·토큰 발급 API), 시뮬레이터 켜짐
```

- 포트가 이미 쓰이고 있으면 바꿔 띄운다: `REDIS_PORT=6380 docker compose up -d --wait` 후 `REDIS_PORT=6380 ./gradlew bootRun`
  (`KAFKA_PORT`, `CLICKHOUSE_PORT`, `ELASTICSEARCH_PORT`, `POSTGRES_PORT`, `APP_PORT` 도 같다. Elasticsearch 는 `ELASTICSEARCH_URIS=http://localhost:<포트>` 도 함께 준다).
- 앱까지 컨테이너로: `docker compose --profile app up -d --build`
- Kafka UI: `docker compose --profile tools up -d kafka-ui` → http://localhost:8090
- 시뮬레이터 끄기: `FMS_SIMULATOR_ENABLED=false ./gradlew bootRun`
- 정리: `docker compose down -v`

시뮬레이터 차량은 서울 물류센터(37.5665, 126.9780) 주변 원형 경로를 돌며 반경 1km 지오펜스를 넘나든다(데모용으로 이동 거리 5배 가속).
`V-009` 는 명령을 거절하고, `V-010` 은 응답하지 않는다(보상 시연용).

### 더 많은 curl 예시

[3분 데모](#3분-데모)의 `TOKEN`·`AUTH` 를 그대로 쓴다.

```bash
# 반경 2km 안 활성 차량(가까운 순)
curl -s "localhost:8080/api/v1/vehicles/nearby?lat=37.5665&lng=126.9780&radiusKm=2&limit=3" -H "$AUTH" | jq

# 지오펜스 안 차량 / 진입·이탈 알림
curl -s localhost:8080/api/v1/geofences -H "$AUTH" | jq
docker exec fms-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic vehicle-geofence-alerts --from-beginning --max-messages 2

# 텔레메트리 직접 수집
curl -s -X POST localhost:8080/api/v1/telematics -H "$AUTH" -H 'Content-Type: application/json' \
  -d "{\"vehicleId\":\"V-100\",\"latitude\":37.5651,\"longitude\":126.9895,\"speed\":30.5,\"batterySoc\":77.0,\"engineStatus\":\"ON\",\"timestamp\":$(date +%s)000}"

# 원격 제어: 정상(ACKNOWLEDGED) / 거절(FAILED)
CID_OK=$(curl -s -X POST localhost:8080/api/v1/vehicles/V-001/commands -H "$AUTH" -H 'Content-Type: application/json' -d '{"type":"DOOR_LOCK"}' | jq -r .commandId)
CID_NG=$(curl -s -X POST localhost:8080/api/v1/vehicles/V-009/commands -H "$AUTH" -H 'Content-Type: application/json' -d '{"type":"DOOR_LOCK"}' | jq -r .commandId)
sleep 3
curl -s localhost:8080/api/v1/vehicles/V-001/commands/$CID_OK -H "$AUTH" | jq -c '{status, rolledBack}'   # ACKNOWLEDGED, false
curl -s localhost:8080/api/v1/vehicles/V-009/commands/$CID_NG -H "$AUTH" | jq -c '{status, rolledBack}'   # FAILED, true

# 과거 궤적(ClickHouse) — 최근 1시간 기본
curl -s "localhost:8080/api/v1/vehicles/V-001/track" -H "$AUTH" | jq '.count, .points[-1]'

# 멱등 처리: 시뮬레이터가 5% 확률로 보낸 중복이 걸러진다(raw = uniq)
curl -s "localhost:8080/actuator/metrics/fms.telematics.duplicates?tag=sink:history" -H "$AUTH" | jq .measurements
docker exec fms-clickhouse clickhouse-client --user fms --password fms_local -d fms \
  -q "SELECT count() AS raw, uniqExact(vehicle_id, event_time) AS uniq FROM telematics_history"

# 이력 색인 지표(반영·버린 결과 수)
curl -s "localhost:8080/actuator/metrics/fms.incident.history.indexed" -H "$AUTH" | jq .measurements
```

## 테스트

```bash
./gradlew test
```

Testcontainers 가 Kafka·Redis·ClickHouse·Elasticsearch·PostgreSQL 을 한 번 띄우고 모든 통합 테스트가 같은 Spring 컨텍스트를 쓴다(Docker 필요, 약 1분 10초).

| 테스트 | 확인하는 것 |
|---|---|
| `VehicleLocationIT` | GEO 반경 조회(1km·3km, 가까운 순, 거리), 비활성 제외, 과거 좌표가 최신 위치를 덮지 않음, 정리 작업, 반경 API·검증(400) |
| `TelematicsPipelineIT` | Kafka → Redis·ClickHouse 반영, **중복 메시지가 두 저장소에 한 번만**(병합을 멈춘 채 행 수 확인), 깨진·무효 메시지가 파이프라인을 막지 않음, 지연 이벤트, 수집 API, 궤적 시각 왕복, 측정 시각 범위(8일 전·1분 뒤 400, 6일 전·10초 뒤 202) |
| `TelemetryHistoryRetryIT` | **사흘치 중 둘째 날 저장 실패 → 재처리 → 병합 전에도 6건·궤적 6건**, **DLT 발행 미확인 → 배치 재처리 → 정상 1건·DLT 1건** |
| `RealtimeRedeliveryIT` | 위치 저장 직후 실패 → 재전달 → ENTER 알림 정확히 1건(위치·지오펜스가 한 스크립트라 그 사이 실패는 구조상 없다) |
| `IdempotencyGuardIT` | 배치 내 중복, 윈도우 만료, IN_FLIGHT, 남의 토큰으로 해제·확정 불가, 리스 만료 후 재선점, **선점 스크립트 재실행(클라이언트 재전송)** |
| `OutboxRelayIT` | 확인된 만큼만 삭제, 발행 대기 중 잠금 만료 → 다른 릴레이 처리 → 미발행 항목 유지, **잠금은 가졌지만 목록 앞부분이 달라진 경우** |
| `GeofenceDetectionIT` | ENTER·EXIT 각 1회, 완충 구간·재전송·지연 좌표 무시 |
| `VehicleCommandSagaIT` | ACKNOWLEDGED / TIMEOUT(롤백·보상 이벤트·잠금 해제) / FAILED(거절) / 409 / 404 / 400 |
| `VehicleCommandUncertaintyIT` | **브로커는 받았지만 발행 확인이 늦은 명령 → 보상 없이 ACK 로 완료**, **성공 ACK·마감 경합 30건 → 종료 확정 1회·제어 상태·보상 이벤트 일치** |
| `IncidentBroadcastIT` | 반경 2km 주행 차량만(IDLE 포함, OFF·반경 밖 제외), 재보고 시 재전파 없음, scope 403 |
| `IncidentResilienceIT` | **브로커 장애 → 발행 1회로 멈춤·FAILED 5/0/5**, **처리 중 선점을 잃으면 409·결과 미기록**, 대상 상한은 시동 꺼짐 제외 뒤 적용 |
| `IncidentHistoryIT` | 기간·유형 필터, 최신순·같은 시각 incidentId 순·페이지, 401·403·400(page 오버플로 포함), 대상 0대(NO_TARGETS), 일부 발행 실패(PARTIALLY_FAILED 3/2/1) → 재시도 결과가 이력 대체, **Elasticsearch 15초 정지 중 접수·경고 계속·검색 503·색인 없음 → 복구 후 이력 조회**, 재전달·늦은 이전 결과가 최신을 덮지 않음, **운영 중 색인 삭제 → strict 매핑으로 재생성** |
| `SecurityIT` | 토큰 없음·만료·위조 서명·다른 발급자·**exp 없음** 401, scope 부족 403, 수집·돌발상황 API 는 전용 scope 만, 헬스 공개 |
| `RideSettlementIT` | **벌금 구역 반납 → 11분·12,650원 → 결제 요청 Kafka 정확히 1건(첫 건 뒤 3초 더 읽음, 멱등키) → DOOR_LOCK ACK → AVAILABLE → 재대여**, 주차 권장 구역 + 지정한 구독권(대여 뒤 만료돼도 대여 시점 조건) 1,025원, **구독권 지정**(둘 있어도 미지정이면 정가, 만료·미보유·미판매 422, 형식 400), **0원 라이드는 결제 요청 없음**, **대여 전 위치·주행 중 보고는 반납 422 → 정차 보고로 반납**, 반납 불가 구역 422·라이드 유지·결제 요청 없음 → 옮긴 뒤 반납, 서비스 지역 밖·대여 뒤 보고 끊김(2분 전) 반납 422, 위치 없음·오래된 기기 대여 422, 반납 재요청 같은 응답·결제 요청 1건, **대여 응답을 잃고 재요청하면 409 + activeRideId**, 대여 규칙·남의 라이드 404·scope 403, **잠금 무응답 기기는 첫 명령 TIMEOUT 뒤에만 재발송·LOCKED 유지·대여 409**, **잠금 거절 기기는 FAILED → 백오프 뒤 재발송** |
| `RideConcurrencyIT` | 같은 기기 동시 대여 16건 → 1건, 같은 라이드 동시 반납 16건 → 같은 요금·결제 요청 1건, **락 없이 반납 트랜잭션 16건 동시 → 확정 1건**, **락을 잃은 사이 다른 인스턴스가 먼저 확정하면 저장된 결과를 돌려줌**, 락 없이 같은 기기 대여 트랜잭션 동시 → 1건, 같은 사용자 다른 기기 16대 동시 → 1건(나머지 `ACTIVE_RIDE_EXISTS`), 다른 요청이 락을 쥐면 1초 기다린 뒤 409 |
| `DeviceStateGuardIT` | 진행 중 라이드 없는 `LOCKED` 기기는 대여 트랜잭션을 받지 않음(유니크 인덱스와 구분), 잠금 명령 선점은 `LOCKED`·진행 중 명령 없음일 때만(PostgreSQL) |
| `RideTransactionFailureIT` | **커밋 중 DB 백엔드를 끊으면 503 + Retry-After**(GlobalExceptionHandler 처리기를 MVC 규칙으로 해석) |
| `PaymentOutboxSettingsTest` | 확인 대기 ≤ `delivery.timeout.ms`(설정값·Kafka 기본 120초)면 기동 거부, batch-size·주기 0 이하 거부 |
| `PaymentOutboxRelayIT` | 확인된 행만 표시하고 실패한 행만 다시 보냄, send 즉시 실패 시 중단·앞선 것만 표시, 확인 마감 초과 시 표시 안 함, **두 릴레이 동시 → 잠긴 행은 건너뜀(같은 행 동시 발행 없음)**, 보존 기간 지난 발행 행만 삭제 |
| `GeoDistanceTest`, `SimulatedFleetTest`, `IncidentDispatchStatusTest` | 거리 계산, 시뮬레이터 경로가 지오펜스 경계를 넘나듦, 처리 상태 판정 |
| `DeviceLockReconcilerTest` | **기기 행 선점에 실패하면(스냅샷 뒤 상태 변경) 잠금 명령을 보내지 않음**, 선점한 ID 로 발송, 전송 확인이 없으면 남은 기기는 다음 주기로, 다른 명령 진행 중이면 선점 해제, 선점 뒤 발송 예외면 선점한 ID 로 백오프 |
| `ReturnZoneIndexTest`, `FareCalculatorTest` | 구역 우선순위(겹침, 같은 정책은 ID 순)·경계 포함·서비스 지역 밖·**경도=x·위도=y(동서로 긴 구역)**·잘못된 WKT(뒤바뀐 위경도·자기 교차·중복 ID) 거부, 분 올림(밀리초 아래 포함)·지정한 구독권(면제·할인율·원 미만 버림)·벌금 비할인·할인 상한·잠금 재시도 백오프 |

주요 테스트는 고친 코드를 일부러 되돌려(변이) 실패하는지 확인했다: 결과 모름도 즉시 보상, CAS 깨기, 날짜별 확정 제거,
결과 기록의 선점 확인 제거, `require_alias` 제거, Elasticsearch external 버전 제거.

## 남아 있는 장애 시나리오와 확장 포인트

**원격 제어**

- **늦은 성공 ACK 의 자동 보정 없음**: 서버가 TIMEOUT 을 확정한 뒤 성공 ACK 가 오면 서버 기록(이전 값)과 실제 차량(목표 값)이 다를 수 있다. 지표·로그만 남기고, 차량 상태 재조회나 보정은 후속 과제다.
- **잠금 TTL 만료 뒤 늦은 보상**: 스케줄러가 잠금 TTL(마감 + 10초)보다 오래 멈추면 다음 명령이 시작된 뒤 이전 명령의 보상이 돌 수 있다. 이때 롤백은 건너뛰지만 `rolledBack`·보상 이벤트는 되돌린 것으로 기록된다.
- **ACK 처리 정지**: Redis 장애 동안 ACK 소비는 재시도하며 멈춘다. 복구 직후 마감 스케줄러가 먼저 TIMEOUT 을 확정하면 위 "늦은 성공 ACK" 가 된다.

**텔레매틱스**

- **측정 시각 범위는 가정이다**: 7일보다 오래 오프라인이던 단말의 데이터나 30초 넘게 시계가 틀린 단말의 데이터는 거부된다(값은 설정).
- **활성 판정은 단말 시각 기준**: `last-seen` 은 측정 시각이다. 미래 30초까지 허용하므로, 앞선 시각 한 건이 들어오면 그 시각까지 해당 차량의 최신 위치 갱신이 멈춘다.
- **멱등 윈도우**는 10분이다. 더 늦은 재전송은 ReplacingMergeTree 가 최종적으로 합치고, 조회는 FINAL 로 정확하다.
- **배치 재시도 중 리밸런스**: 무기한 재시도 중 파티션이 다른 소비자로 넘어갈 때의 오프셋 동작은 검증하지 않았다(중복이 생겨도 FINAL·멱등키가 흡수한다).

**돌발상황 접수·이력**

- **경고 발행 후 결과 기록 전에 프로세스가 죽으면**: 경고는 나갔지만 처리 결과가 없다. 선점은 리스(15초) 뒤 풀리므로 RSU 가 재시도하면 새 결과가 남지만, 재시도가 없으면 이력이 빠진다.
- **Redis 장애**: 멱등 선점·반경 조회·처리 결과 아웃박스가 모두 Redis 라 접수 자체가 실패한다(503). 이력 결과도 남지 않는다.
- **Kafka 장애**: 경고 발행이 `FAILED` 로 끝나고 503(재시도 요청). 처리 결과는 아웃박스에 남아 브로커 복구 뒤 이력에 반영된다.
- **Elasticsearch 장애가 7일을 넘기면**: `c-its-incident-results` 보존 기간을 넘긴 결과는 색인되지 못하고 사라진다.
- **별칭 자리에 일반 색인이 있으면**(예: 별칭 도입 전에 만든 로컬 볼륨) 색인 소비자가 매핑을 보장할 수 없어 쓰지 않고 오류를 남긴다. 로컬은 `docker compose down -v` 로 정리한다.
- **형식·매핑 오류 결과**는 로그와 `fms.incident.history.rejected` 지표만 남기고 건너뛴다(DLT 없음).
- **부분 실패 재시도**: 이미 경고를 받은 차량에 같은 경고가 다시 간다. 단말이 `incidentId` 로 중복 표시를 걸러야 한다.
- **발행 확인 = 마감 안의 브로커 수신**: 확인이 3초보다 늦으면 실제로 전달됐어도 실패로 세고 재전파된다. 차량 단말이 실제로 받았는지는 연동 범위 밖이다.
- **깊은 페이지**: `page × size ≤ 10,000`. 그 이상은 기간을 좁히거나 `search_after` 커서로 바꾼다.

**라이드 반납·정산**

- **GPS 오차와 구역 경계**: 경계에 닿으면 구역 안으로 본다. 벌금 구역 경계 몇 m 안에서는 GPS 오차(수~수십 m)에 따라 판정이 갈릴 수 있다.
  단말이 위치 정확도(HDOP 등)를 보내면 "불확실하면 벌금 대신 사진 확인 요청" 같은 완충 규칙을 더할 수 있다(지금 텔레메트리에는 정확도가 없다).
- **위치 신선도·대여 뒤 측정은 단말 시각 기준**: 측정 시각을 서버 시각(대여 시각·지금)과 비교한다. 단말 시계가 앞서 있으면(최대 30초 허용) 실제보다 신선하게 보이고,
  늦으면 대여 직후 반납이 그만큼 늦게 풀린다. GPS 시각을 쓰는 단말을 전제로 한다.
- **정차 판정의 남은 틈**: 반납은 정차 보고로만 판정하지만, 정차 보고 직후 움직여 다음 보고 전에 반납하면 그 사이 이동은 모른다. 틈은 단말의 보고 주기만큼이다
  (주행 중 1초 주기면 수 m). 더 줄이려면 반납 요청 뒤에 측정한 새 보고를 기다리거나 단말이 잠금 직전 위치를 보내게 한다.
- **대여 때 잠금 해제 명령은 보내지 않는다**(요구 범위 밖): 대여는 상태만 바꾼다. 실제 서비스는 `DOOR_UNLOCK` 명령과, 실패하면 대여를 취소(무료)하는 보상이 필요하다.
- **잠금이 끝내 확인되지 않는 기기**: `LOCKED` 로 남아 최대 5분 간격으로 계속 다시 보낸다(대여 불가). 물리적으로 잠기지 않은 채 세워져 있을 수 있으므로
  `fms.ride.device_lock{result=FAILED}` 증가에 경보를 걸어 현장 확인해야 한다. 늦게 온 성공 ACK(TIMEOUT 확정 뒤)는 명령 사가와 같이 자동 보정하지 않는다.
- **결제 결과는 범위 밖**: 결제 실패·미수금·환불·분쟁은 결제 서비스가 한다. 이 서비스는 확정 요금과 결제 요청 발행까지 책임진다.
- **결제 요청 중복 가능**(at-least-once): 확인 전에 죽거나 프로듀서가 실패를 알린 레코드가 사실은 기록됐을 때다. 결제 서비스가 `idempotencyKey` 로 거르지 않으면 이중 청구된다.
  이 계약을 결제 서비스와 맞춰야 한다.
- **릴레이 트랜잭션이 브로커 확인을 기다리는 동안**(최대 35초) 행 잠금과 DB 연결 하나, 스케줄러 스레드 하나를 쥔다. 브로커 장애가 길면 주기마다 그만큼 붙잡는다.
- **토픽이 없으면 결제 요청이 멈춘다**: 기동 때 브로커에 닿지 않아 `ride-payment-requests` 를 만들지 못하면(브로커 자동 생성 꺼짐) 재기동 전까지 발행되지 않는다(유실은 아님).
  가장 오래된 미발행 행의 나이를 지표로 내 경보를 거는 것이 다음 단계다. 다른 토픽도 같은 성질이다.
- **구역·요금표는 설정 파일**: 바꾸려면 재기동한다. 라이드에는 구역 ID·정책만 남고 구역 다각형의 변경 이력은 없다(분쟁 시 당시 구역 모양을 복원할 수 없다).
- **구독권 목록 API 가 없다**: 이용자는 대여할 때 구독권을 지정하지만, 가진 구독권 목록은 구독권 판매 서비스(범위 밖)가 앱에 알려 준다고 가정한다.
- **0원 라이드의 결제 기록**: 결제 요청을 만들지 않으므로 결제 서비스에는 기록이 없다. 정산·통계는 라이드 기록(`total_fare = 0`)으로 본다.
- **주행 중 단말 통신이 끊기면 반납할 수 없다**: 반납은 30초 안의 단말 위치가 있어야 받는다. 통신이 돌아올 때까지 라이드가 진행 중으로 남고 주행 요금이 쌓인다.
  종료 정책(마지막 보고 시각을 종료 시각으로 쓰기, 관제의 강제 종료 API 등)은 정하지 않았다.
- **Redis 장애**: 분산 락과 단말 위치가 모두 Redis 라 대여·반납이 503 이다(락만 건너뛰어도 DB 가 정합성은 지키지만, 위치 없이는 반납 판정을 할 수 없다).
- **PostgreSQL 장애**: 라이드 API 503, 결제 요청 릴레이·잠금 조정 정지. 떠 있는 인스턴스의 수집·명령·돌발상황 경로는 영향 없다(헬스에서 제외).
  하지만 장애 중 재기동·확장한 인스턴스는 Flyway 마이그레이션에서 기동이 실패한다. 마이그레이션을 배포 잡으로 분리하면(`spring.flyway.enabled=false`) 이 의존을 없앨 수 있다.

**공통**

- **Redis Cluster**: Lua 스크립트가 여러 키를 함께 다루므로 단일 Redis(또는 Sentinel) 전제다. Cluster 로 가려면 같은 스크립트의 키에 hash tag(`{vehicleId}`)를 붙여 한 슬롯에 모으고, 전역 키(GEO 인덱스·마감 zset·아웃박스)는 샤딩하거나 스크립트 밖으로 뺀다.
- **Lua 스크립트 중간 오류**: Redis 는 스크립트 중간에 오류가 나도 앞선 쓰기를 되돌리지 않는다. 오류가 날 수 있는 입력(좌표 범위 등)은 스크립트 전에 검증한다.
- **아웃박스 릴레이**는 잠금을 가진 인스턴스 하나만 돈다(중복 발행 가능, 유실 없음). 해석할 수 없는 항목은 `fms:outbox:dead` 에 중복으로 들어갈 수 있다.
- **반경 조회 상한**: GEOSEARCH 에 COUNT 를 걸지 않는다(활성·조건을 거른 뒤 자르기 위해). 넓은 반경에서 함대가 크면 Redis 부하가 커진다.
- **지오펜스**: 이벤트마다 모든 펜스와 거리를 잰다(펜스가 많아지면 공간 색인 필요). 오래 수신이 없는 차량도 "안에 있는 차량" 집합에서 빠지지 않는다.
- **ClickHouse 쓰기 경로**: 소비자 배치를 날짜별 INSERT 로 넣는다. 처리량이 더 크면 Kafka 엔진 테이블 + Materialized View 로 ClickHouse 가 직접 소비하게 한다.
- **JWT·개발용 토큰**: 데모를 위해 HS256 공유 키를 쓴다. 운영에서는 IdP(Keycloak 등)의 RS256 + JWKS 로 바꾸고, 단말은 mTLS 로 따로 인증한다. 개발용 토큰 발급 API 는 `local` 프로필에서만 열리며, compose 의 app 서비스는 이 프로필로 0.0.0.0 에 뜨므로 공유 네트워크에서 띄우지 않는다.

## 후속 요구사항 — 돌발상황 관제 이력

구현 완료(2026-11-25). 설계는 [핵심 설계 5·6](#5-c-its-돌발상황-전파), 데모는 [3분 데모 ④·⑤](#3분-데모)에 있다.
관제 운영자는 도로 센서가 보고한 돌발상황을 기간과 유형으로 찾아 경고 대상이 몇 대였고 Kafka 발행이 몇 건 확인됐는지 확인할 수 있어야 한다.

| 항목 | 요구사항 |
|---|---|
| 검색 | `GET /api/v1/incidents?from=…&to=…&type=ROAD_DEBRIS`로 발생 시각 범위와 선택적인 유형을 검색한다. 최신 발생 순으로 페이지를 제공하고, 조회 권한은 `fleet:read`로 제한한다. |
| 이력 | `incidentId`마다 보고 출처, 발생 시각, 좌표, 유형, 대상 차량 수, Kafka 발행 확인 건수, 전파 처리 상태를 기록한다. 대상이 0대인 보고와 발행 확인에 일부 실패한 보고도 조회할 수 있어야 한다. |
| 비동기 색인 | Kafka에 사고 단위의 처리 결과를 남기고, 별도 소비자가 Elasticsearch에 색인한다. `incidentId`를 문서 ID로 사용하며, 재전달은 문서를 늘리지 않고 이전 처리 결과는 더 최근 결과를 덮어쓰지 않는다. |
| 장애 복구 | Elasticsearch가 중단되어도 기존 접수와 경고 발행은 계속된다. 검색 API는 장애 중 `503`을 반환하고, 색인 소비자는 복구 후 밀린 결과를 처리한다. |

발행 확인 건수는 Kafka 브로커가 수신을 확인한 건수다. 차량 단말의 수신 여부는 현재 연동 범위 밖이다.

### 완료 기준

| 기준 | 검증 |
|---|---|
| 1. 서로 다른 시각과 유형으로 접수한 보고 중 기간과 유형 조건을 충족하는 이력만 최신순으로 조회된다. 조회 권한이 없는 토큰은 `403`을 받는다. | `IncidentHistoryIT` — 기간 밖 2건 제외, 유형 필터, 같은 시각 2건은 incidentId 순, 페이지 2개, `incident:write`·`telematics:write`·`vehicle:control` 토큰 403 |
| 2. 수신 대상이 0대인 보고와 일부 발행에 실패한 보고에서 각각 대상 수, 발행 확인 건수, 처리 상태를 확인할 수 있다. | `IncidentHistoryIT` — `NO_TARGETS` 0/0/0, `PARTIALLY_FAILED` 대상 3·확인 2·실패 1 → 재시도 뒤 `COMPLETED` 3/3 |
| 3. Elasticsearch 중단 중에도 경고가 Kafka에 발행된다. 복구 후 해당 이력을 찾을 수 있고, 같은 처리 결과를 재전달해도 검색 결과에는 한 건만 나타난다. | `IncidentHistoryIT` — 컨테이너 15초 일시 정지 중 접수 202·경고 2건 발행·검색 503·색인 없음 → 재개 후 이력 조회, 재전달·늦은 도착 뒤에도 문서 1건·최신 버전 유지 |

## 후속 요구사항 — 반납 구역 판정과 라이드 요금 정산

구현 완료(2026-11-26). 설계는 [핵심 설계 8](#8-라이드-반납정산--구역-판정-분산-락-결제-요청-아웃박스), 데모는 [3분 데모 ⑥](#3분-데모)에 있다.

| 항목 | 요구사항 | 구현 |
|---|---|---|
| 구역 정책 판정 | `SERVICE_ZONE`(정상), `DISCOUNT_ZONE`(주차 권장 −500원), `PENALTY_ZONE`(주차 금지 +10,000원), `NO_RETURN_ZONE`(반납 불가). 반납 시 기기의 최종 GPS 좌표가 속한 다각형을 판정 | JTS 다각형 + STRtree. 좌표는 단말 최신 텔레메트리(30초 이내·대여 뒤 측정·정차). 겹치면 더 제한적인 정책, 어느 구역에도 없으면 반납 불가 |
| 상태 머신 | `AVAILABLE → IN_USE → LOCKED`(원격 잠금 명령) | DB 조건부 갱신. `LOCKED` 에서 기존 명령 사가로 `DOOR_LOCK`, ACK 면 `AVAILABLE` |
| 동시성 | Redisson 분산 락으로 중복 반납·동시 대여 경합 차단 | 기기 단위 락(watchdog) + DB 조건부 갱신·부분 유니크 인덱스(락을 잃어도 한 번) + 멱등 반납 |
| 요금 | 잠금해제료 + 주행시간(분) × 분당요금 + 구역 벌금/할인 − 구독권 할인 | 분 올림, 대여 시점 요금표·지정한 구독권 스냅샷, 구독권은 벌금 비할인, 합계 ≥ 0 |
| 결제 요청 | 외부 API 를 바로 부르지 않고 트랜잭셔널 아웃박스로 DB 에 저장 후 Kafka/SQS 로 비동기 릴레이 | 반납 확정과 같은 트랜잭션에 `payment_outbox` INSERT(합계 > 0) → `FOR UPDATE SKIP LOCKED` 릴레이 → `ride-payment-requests`(멱등키) |

### 완료 기준

| 기준 | 검증 |
|---|---|
| 1. 반납 좌표에 따라 구역 정책이 정해지고, 반납 불가 구역·서비스 지역 밖에서는 반납이 거절되며 라이드가 계속된다. | `ReturnZoneIndexTest` — 겹침 우선순위·경계·지역 밖, `RideSettlementIT` — 벌금·할인·정상 구역 반납, 반납 불가·지역 밖 422(라이드 `IN_PROGRESS`, 기기 `IN_USE`, 결제 요청 없음) → 옮긴 뒤 반납 |
| 2. 요금이 계산식대로 확정되고 항목 합이 합계와 같다. | `FareCalculatorTest`, `RideSettlementIT` — 11분 벌금 구역 12,650원, 5분 주차 권장 + 구독권 30% 1,025원 |
| 3. 같은 기기 동시 대여·같은 라이드 중복 반납에서 상태 전이와 결제 요청은 한 번이다. 분산 락을 잃어도 같다. | `RideConcurrencyIT` — 동시 대여 1건, 동시 반납 16건 같은 요금·결제 요청 1건, 락 없이 반납 트랜잭션 16건 → 확정 1건, 락 상실 중 먼저 확정된 결과 반환, `DeviceStateGuardIT` |
| 4. 결제 요청은 반납 확정과 함께만 남고, 브로커 확인 뒤에만 발행 완료가 된다. 여러 릴레이가 같은 요청을 동시에 보내지 않는다. | `RideSettlementIT` — 결제 요청 Kafka 정확히 1건·`published_at`·0원은 없음, `PaymentOutboxRelayIT` — 실패 행만 재발행, 확인 마감 초과 미표시, 두 릴레이 동시, `PaymentOutboxSettingsTest` — 확인 대기 > 전송 마감 |
| 5. 반납 뒤 원격 잠금이 확인돼야 다시 대여할 수 있다. | `RideSettlementIT` — DOOR_LOCK ACK → `AVAILABLE` → 재대여, 무응답·거절 기기는 `LOCKED` 유지·재시도(첫 명령 종료 뒤)·대여 409 |
