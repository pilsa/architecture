# 소스 폴링 → 아웃박스 적재 메커니즘 작동원리 (v1)

- 작성 2026-11-05 · 상태: 검토 대상
- 선행 문서: `docs/polling-implementation-guide.html`(구현 가이드 v3), `polling-design-v2.md`, `review-codex-r1.md`, `review-codex-r2.md`
- 이 문서의 범위: v3는 **어느 행을, 누가, 언제 읽는가**(범위 청크·리스·fencing·sweep)를 정한다. 이 문서는 그 위에서 **읽은 행이 어떻게 아웃박스 레코드가 되어 Kafka까지 가는가**를 처음부터 끝까지 한 흐름으로 정의한다. 아울러 v3에서 비어 있던 아웃박스 쪽 결정 다섯 가지(§5~§9)를 채운다.
- v3에서 이미 정한 내용(커서 키, safe_ts, Planner CAS, claim SQL, Reaper, sweep)은 다시 설명하지 않고 해당 장을 가리킨다.

---

## 1. 한 장 요약

```
 [primary]                [복제본 · 읽기전용]              [앱 DB]                             [Kafka]
 INSERT 커밋 ──WAL──▶  ① 가시화  ─────────▶ ② 계획(Planner)
                        replay_ts 전진        safe_ts 미만 구간을 (from,to] 청크로 등록
                                              (CAS로 planned_hwm 전진)
                                                   │
                                                   ▼
                                              ③ 점유(Worker claim)
                                              SKIP LOCKED + lease_token++
                                                   │
                      ④ 읽기·변환 ◀───────────────┘
                      (from,to] keyset 페이지 조회
                      source_key · created_at · payload JSON을 SQL에서 생성
                                                   │
                                                   ▼
                                              ⑤ 적재 커밋 (단일 TX)
                                              멱등 INSERT ON CONFLICT DO NOTHING
                                              → RETURNING된 신규 행만 아웃박스 INSERT
                                              → 토큰 조건 DONE (0행이면 롤백)
                                                   │
                                                   ▼
                                              ⑥ 릴레이(기존) ─────────────────────▶ 토픽
                                              아웃박스 → 발행 → 발행표시               (key = aggregate_key)
```

| 단계 | 하는 일 | 보장 | 근거 |
|---|---|---|---|
| ① 가시화 | primary 커밋이 복제본에 재생됨 | 커밋 순서대로 보임 | 물리 복제 WAL 순차 재생 |
| ② 계획 | `created_at < safe_ts` 구간을 겹침·빈틈 없는 청크로 자름 | 계획된 구간은 이후 새 행이 생기지 않음 | v3 §4.2(필수조건 P1~P4), §4.3 CAS |
| ③ 점유 | 워커 하나가 청크를 리스로 가져감 | 같은 청크를 동시에 두 워커가 유효하게 갖지 않음 | v3 §4.4 |
| ④ 읽기·변환 | 청크 범위를 읽고 이벤트 재료(키·payload)를 만듦 | 최소 한 번 읽음, 다시 읽어도 결과 동일 | append-only + 고정 구간 |
| ⑤ 적재 커밋 | 멱등 판정 + 아웃박스 기록 + 청크 완료 | **소스 행 하나당 아웃박스 레코드 최대 1개** | §3 |
| ⑥ 릴레이 | 아웃박스 → Kafka | 최소 한 번 발행 | 기존 구현 |
| 컨슈머 | `event_id`로 중복 제거 | 실질적으로 한 번 처리 | §5 결정적 event_id |

핵심 문장: **"읽기는 여러 번 해도 되고, 쓰기는 멱등 테이블이 문지기로서 한 번만 통과시키며, 그 통과 여부와 청크 완료를 같은 트랜잭션으로 묶는다."**

---

## 2. 한 행의 생애와 지연 예산

예시: `sales.order_line`에 행 R이 `created_at = t0`로 들어간다.

| 시각 | 사건 | 위치 |
|---|---|---|
| t0 | 쓰기 TX 시작, `created_at = now() = t0` | primary |
| t0 + x (x < B) | TX 커밋 | primary |
| + 복제 지연 | 커밋이 재생되어 R이 보임, `replay_ts ≥ t0 + x` | 복제본 |
| `replay_ts − safety_lag > t0`가 되는 순간 | R이 safe_ts 아래로 들어옴 | 복제본 |
| + Planner 주기(≤ 1s) | R을 포함하는 청크 C가 READY로 등록 | 앱 DB |
| + claim 백오프(≤ 1s) | 워커가 C를 claim | 앱 DB |
| + 조회·커밋(수십~수백 ms) | 멱등 행과 아웃박스 레코드 커밋, C = DONE | 앱 DB |
| + 릴레이 주기 | Kafka 발행 | 기존 릴레이 |

**end-to-end 지연 ≈ safety_lag + Planner 주기 + claim 백오프 + 처리 시간 + 릴레이 주기**
기본값(lag 10s, 1s, 1s, 0.3s, 릴레이 0.5s)이면 약 13초다. 지연의 대부분은 safety_lag이고, 이 값은 정확성 조건(`lag ≥ B + 여유`)으로 하한이 정해지므로 **지연을 줄이려면 writer TX 상한 B를 줄여야 한다**. 워커 수를 늘려도 이 하한은 줄지 않는다(워커 수는 처리량을 늘린다).

---

## 3. 적재 커밋의 작동 원리 (핵심)

### 3.1 왜 "멱등 INSERT → RETURNING → 아웃박스"인가

같은 소스 행이 두 번 이상 적재 단계에 도착하는 경로는 정상적으로 존재한다.

1. 리스 만료로 청크가 재claim됨(처리가 느렸거나 pod가 죽음)
2. 좀비 워커: 리스를 잃은 줄 모르고 계속 처리
3. sweep → replay 요청이 이미 반영된 키를 다시 가져옴(sweep과 정상 경로 경합)
4. 운영자 재처리

따라서 "이 행을 이미 반영했는가"를 **앱 DB의 unique 제약**으로 판정한다. `INSERT ... ON CONFLICT DO NOTHING RETURNING`은 이 판정과 기록을 원자적으로 하고, **이번에 새로 들어간 키만** 돌려준다. 아웃박스는 그 결과와 조인해서만 INSERT하므로 "멱등에 새로 들어감 ⇔ 아웃박스 레코드 생성"이 한 문장 안에서 성립한다.

### 3.2 왜 청크 DONE까지 같은 트랜잭션인가

DONE 갱신은 `WHERE lease_token = 내 토큰 AND status = 'CLAIMED'` 조건을 가진다. 이것이 0행이면 다른 워커가 이미 재claim했다는 뜻이므로 예외로 **멱등·아웃박스 기록까지 전부 롤백**한다. 이렇게 묶어야 다음 두 성질이 동시에 성립한다.

- 좀비의 기록은 남지 않는다(fencing).
- 청크가 DONE이면 그 청크의 모든 행은 멱등 테이블 또는 격리 테이블(§9.1)에 있다(완료HWM과 sweep의 전제).

멱등 기록과 DONE을 따로 커밋하면 "멱등은 있는데 청크는 READY" 또는 그 반대 상태가 생긴다. 앞의 경우는 중복 없이 복구되지만, 뒤의 경우(DONE인데 멱등이 없음)는 sweep 전까지 보이지 않는 누락이 된다. 그래서 분리하지 않는다.

### 3.3 실패 지점별 결과

| 실패 지점 | 결과 | 복구 |
|---|---|---|
| claim 후 조회 중 pod 사망 | 아무것도 커밋되지 않음 | 리스 만료 → 재claim |
| 조회 완료 후 적재 TX 시작 전 사망 | 동일 | 동일 |
| 적재 TX 도중 사망·커넥션 끊김 | PostgreSQL이 TX를 롤백 | 동일 |
| 적재 TX 커밋 직후, 응답 수신 전 사망 | 커밋은 됨. 청크는 DONE | 할 일 없음(DONE 청크는 다시 claim되지 않음) |
| 좀비가 새 소유자보다 먼저 커밋 시도 | 토큰 불일치로 0행 → 롤백 | 새 소유자가 정상 커밋 |
| 좀비와 새 소유자가 같은 키를 동시에 INSERT | unique 인덱스가 뒤쪽을 대기시킨 뒤 판정. 둘 중 토큰이 유효한 쪽만 커밋 | — |
| 아웃박스 커밋 후 릴레이 발행 전 사망 | 아웃박스에 남음 | 릴레이가 재발행(최소 한 번) |
| 릴레이 발행 후 발행표시 전 사망 | Kafka에 중복 발행 | 컨슈머가 `event_id`로 제거(§5) |

### 3.4 적재 SQL (v3 §8.2 완료 SQL 대체안)

워커는 복제본에서 행마다 다음 네 값을 받아 같은 길이의 배열 네 개로 넘긴다: `source_key`(text), `source_created_at`(text), `payload`(text), `event_id`(text). Java는 이 값을 해석하지 않고 그대로 넘긴다.

```sql
-- 앱 DB · 단일 트랜잭션 (1)~(2)
-- (1) 멱등 판정 + 신규분만 아웃박스
WITH input AS (
  SELECT u.source_key::jsonb            AS source_key,
         u.source_created_at::timestamptz AS source_created_at,
         u.payload::jsonb               AS payload,
         u.event_id::uuid               AS event_id,
         u.ord
  FROM unnest(CAST(:keys       AS text[]),
              CAST(:createdAts AS text[]),
              CAST(:payloads   AS text[]),
              CAST(:eventIds   AS text[]))
       WITH ORDINALITY AS u(source_key, source_created_at, payload, event_id, ord)
), inserted AS (
  INSERT INTO idempotency (source_id, source_key, emit_generation, source_created_at,
                           chunk_id, event_id, created_at)
  SELECT :source_id, i.source_key, :generation, i.source_created_at,
         :chunk_id, i.event_id, statement_timestamp()
  FROM input i
  ON CONFLICT (source_id, source_key, emit_generation, source_created_at) DO NOTHING
  RETURNING source_key
)
INSERT INTO outbox (event_id, aggregate_key, event_type, payload, headers, created_at)
SELECT i.event_id,
       :aggregate_key_expr,                          -- §8. 기본은 i.source_key::text
       :event_type,
       i.payload,
       jsonb_build_object(
         'schema_version',   :schema_version,
         'source_id',        :source_id,
         'source_key',       i.source_key,
         'source_created_at', i.source_created_at,
         'emit_generation',  :generation,
         'chunk_id',         :chunk_id),
       statement_timestamp()
FROM inserted ins
JOIN input i ON i.source_key = ins.source_key
ORDER BY i.ord;                                     -- 청크 안에서는 커서 순서대로 기록

-- (2) fencing 완료: 1행이 아니면 예외 → (1)까지 전부 롤백
UPDATE poll_chunk
SET status = 'DONE', lease_owner = NULL, lease_until = NULL, done_at = statement_timestamp(),
    row_count = :row_count, new_count = :inserted_count
WHERE chunk_id = :chunk_id AND status = 'CLAIMED'
  AND lease_token = :token AND source_epoch = :source_epoch;
```

- 아웃박스 컬럼명(`headers` 등)은 기존 테이블에 맞춰 매핑한다. 기존 아웃박스에 헤더 컬럼이 없으면 envelope 필드를 payload 최상위의 `meta` 객체로 넣는다(§6.2).
- `(1)`의 반환 행 수 = 신규 건수다. `row_count − inserted_count`는 이미 반영된 키(재claim·sweep 경합)의 수이고, 정상 운영에서는 0에 가까워야 한다. 지속적으로 크면 리스가 짧다는 신호다.

---

## 4. 읽기·변환 단계의 원칙

### 4.1 이벤트 재료는 복제본 SQL에서 만든다

v3는 커서 경계값을 "복제본에서 `::text`로 받아 그대로 왕복한다"는 원칙을 쓴다. 이벤트 재료에도 같은 원칙을 적용한다.

| 값 | 생성 위치와 식 | 이유 |
|---|---|---|
| `source_key` | `jsonb_build_array(pk1::text, …)::text` | jsonb 텍스트 출력은 정규화되어 있어 같은 키는 항상 같은 문자열 → 멱등 키·event_id의 입력으로 안전 |
| `source_created_at` | `created_at::text` (세션 TimeZone=UTC, DateStyle=ISO) | 멱등 테이블 파티션 키(§7). 왕복이 정확해야 같은 행이 같은 파티션에 들어감 |
| `payload` | `jsonb_build_object('col', t.col, …)::text` | 타입 변환을 PostgreSQL 한 곳에서만 수행. Java의 `OffsetDateTime`·`BigDecimal` 변환에 따른 정밀도·타임존 차이를 원천 차단 |
| `cursor_key` | `ARRAY[created_at::text, pk1::text, …]` | 페이지 keyset 진행용(v3 그대로) |

payload 조회 예시(v3 §8.2 범위 조회의 SELECT 목록만 바뀜):

```sql
SELECT jsonb_build_array(t.order_no::text, t.line_no::text)::text           AS source_key,
       t.created_at::text                                                    AS source_created_at,
       ARRAY[t.created_at::text, t.order_no::text, t.line_no::text]          AS cursor_key,
       jsonb_build_object('order_no', t.order_no, 'line_no', t.line_no,
                          'item_cd',  t.item_cd,  'qty',     t.qty,
                          'created_at', t.created_at)::text                  AS payload
FROM ( ... v3와 같은 (from, to] keyset 서브쿼리, LIMIT :page_size ... ) t;
```

payload에 들어갈 컬럼과 JSON 이름은 소스 YAML의 `payload-columns`로 정의하고, 렌더러가 v3 §9.4와 같은 식별자 검증을 거쳐 `jsonb_build_object` 조각을 기동 시 한 번 만든다.

주의: `numeric`은 JSON 숫자로 정확히 나가지만, 컨슈머가 double로 파싱하면 정밀도를 잃는다. 금액처럼 정밀도가 중요한 컬럼은 `payload-columns`에서 `as-string: true`로 지정해 `t.amount::text`로 내보낸다.

### 4.2 청크 하나는 메모리에 모아 한 번에 커밋한다

페이지마다 커밋하면 메모리는 줄지만, "청크 DONE ⇔ 청크의 모든 행이 멱등에 있음"(§3.2)을 깨지 않으려면 DONE은 마지막 페이지에서만 해야 한다. 중간 페이지 커밋도 멱등 덕분에 중복은 없지만, 실익보다 상태 추론이 복잡해지므로 **청크 단위 단일 커밋**을 유지한다. 대신 청크 크기를 바이트로도 제한한다.

- `max-chunk-bytes`(기본 8 MiB): 읽는 도중 payload 누적 크기가 이 값을 넘으면 재시도해도 결과가 같으므로 치명 오류(`last_error_code = 'CHUNK_TOO_LARGE'`)로 실패 처리한다. v3 실패 SQL에 따라 청크는 DEAD, 소스는 SOFT_PAUSED가 된다. 같은 트랜잭션에서 `poll_source.chunk_size`를 절반으로 줄여 이후 청크가 작게 만들어지게 한다. DEAD 청크는 운영자가 같은 범위를 작은 복구 청크 여러 개로 다시 등록한 뒤 소스를 재개한다(v3 runbook 5번 "DEAD 처리"에 항목 추가).
- 평상시에는 `chunk_size × 평균 payload 크기`가 이 값의 1/4 이하가 되도록 소스별 `chunk_size`를 잡는다.

---

## 5. 결정 ① event_id는 결정적으로 만든다

v3 SQL은 `gen_random_uuid()`를 쓴다. 이 값은 아웃박스 안에서는 문제가 없지만, **릴레이가 최소 한 번 발행**이므로 컨슈머는 중복을 받을 수 있고, 무작위 UUID로는 "같은 소스 행에서 나온 이벤트"인지 판별할 수 없다. 따라서:

```
event_id = UUIDv5(namespace = 배포 고정 UUID,
                  name      = source_id || 0x00 || source_key(text) || 0x00 || emit_generation)
```

- 같은 소스 행 + 같은 세대 → 항상 같은 event_id. 릴레이 중복, 멱등 테이블 유실 후 재적재가 일어나도 컨슈머는 event_id로 걸러낸다.
- `source_key`는 §4.1의 정규화된 jsonb 텍스트이므로 입력이 흔들리지 않는다.
- 계산 위치: 앱 DB에 `uuid-ossp`가 있으면 `uuid_generate_v5(:ns, ...)`로 SQL에서, 없으면 Java에서 계산해 배열로 넘긴다. 어느 쪽이든 **한 곳으로 고정**하고 구현 간 일치 테스트(고정 입력 → 고정 UUID)를 둔다.
- `idempotency.event_id`에도 저장해 두면, 컨슈머 측 문의("이 이벤트는 어느 소스 행인가")에 바로 답할 수 있다.

## 6. 결정 ② 아웃박스 레코드 형식

### 6.1 필드

| 필드 | 값 | 비고 |
|---|---|---|
| `event_id` | §5 | 컨슈머 중복 제거 키 |
| `event_type` | 소스별 YAML `event-type` (예: `order_line.created`) | 소스 테이블이 append-only이므로 생성 이벤트 하나뿐 |
| `aggregate_key` | Kafka 메시지 키. 기본 `source_key` | §8 |
| `payload` | §4.1의 JSON | 소스 컬럼의 스냅샷 |
| 헤더 `schema_version` | 소스별 정수 | `payload-columns`를 바꾸면 +1 |
| 헤더 `source_id`, `source_key`, `source_created_at` | 추적용 | |
| 헤더 `emit_generation` | 기본 0 | §9 |
| 헤더 `chunk_id` | 추적용 | 장애 분석 시 청크 이력과 연결 |

### 6.2 스키마 버전과 에포크는 별개다

- **에포크**(v3)는 커서 정의(어느 행을 어떤 순서로 읽나)가 바뀔 때 올린다. 이 경우 HWM이 리셋된다.
- **schema_version**은 payload 모양만 바뀔 때 올린다. HWM·청크·멱등에는 영향이 없다. 바뀐 시점 이후 적재되는 행부터 새 형식으로 나간다.
- `config_hash`(v3)는 커서 정의만 해시한다. payload 정의는 별도의 `payload_hash`로 기동 시 대조하고, 다르면 `schema_version`이 함께 올라갔는지 확인한다(안 올렸으면 기동 거부).
- 소스 팀이 payload 컬럼을 DROP하면 범위 조회가 실패해 청크가 재시도 → DEAD → 소스 일시정지로 이어진다. 이것은 의도한 동작이다(조용히 필드가 빠진 이벤트를 내보내지 않음).

## 7. 결정 ③ 멱등 테이블 보존 기간

멱등 테이블은 계속 커진다. 그런데 **지운 키가 다시 적재 단계에 오면 새 이벤트로 발행**되므로 보존 기간은 "같은 키가 다시 올 수 있는 모든 경로"보다 길어야 한다.

| 다시 오는 경로 | 최대 과거 범위 |
|---|---|
| 리스 재claim·좀비 | 분 단위 |
| NEAR sweep | 10분 + 여유 |
| DAILY sweep | 48시간 |
| replay 요청(운영자) | 운영 정책 |
| FULL sweep | 초기시작일시까지 전부 |

규칙:

1. **멱등 보존 ≥ max(DAILY window, 운영자 replay 허용 범위) + 여유.** 기본 90일.
2. **FULL sweep과 replay는 보존 경계(`idempotency_horizon`)보다 오래된 행을 대상으로 하지 않는다.** 그보다 과거는 이미 컨슈머에 도달했다고 보고 감사 목적의 대조만 한다(재처리 요청 생성 금지). 이 경계는 `poll_source.idempotency_horizon`으로 기록하고 sweep 쿼리 하한에 넣는다.
3. 그래도 실수로 재적재되면 §5의 결정적 event_id 덕분에 컨슈머에서 걸러진다(최종 방어선).

물리 설계: `source_created_at` 기준 월 단위 range 파티션, 오래된 파티션은 DROP.

```sql
CREATE TABLE idempotency (
  source_id         varchar(64)  NOT NULL,
  source_key        jsonb        NOT NULL,
  emit_generation   smallint     NOT NULL DEFAULT 0,
  source_created_at timestamptz  NOT NULL,
  chunk_id          bigint,
  event_id          uuid         NOT NULL,
  created_at        timestamptz  NOT NULL DEFAULT now(),
  PRIMARY KEY (source_id, source_key, emit_generation, source_created_at)
) PARTITION BY RANGE (source_created_at);
```

**파티션 키를 unique 제약에 넣어도 전역 유일성이 유지되는 이유**: PostgreSQL 파티션 테이블의 PK에는 파티션 키가 반드시 들어가야 한다. 보통은 이 때문에 전역 유일성이 깨지지만, 여기서는 필수조건 P1(커서 컬럼은 INSERT 후 불변)에 의해 **같은 소스 키의 `source_created_at`은 항상 같다**. 따라서 같은 키는 항상 같은 파티션·같은 PK 값으로 가고, `(source_id, source_key, gen, source_created_at)` 유일성은 `(source_id, source_key, gen)` 유일성과 같다. 단, `source_created_at` 텍스트 왕복이 정확해야 하므로 v3 기동 검증의 왕복 테스트가 이 전제를 함께 지킨다.

기존 멱등 테이블이 이미 `(source_id, source_key)`로 운영 중이라면 마이그레이션은 별도 단계로 한다: 새 파티션 테이블 생성 → 이중 기록 기간 → 조회 전환 → 구 테이블 DROP. 그 전까지는 규칙 1~3만 적용한다.

## 8. 결정 ④ aggregate_key와 순서

### 8.1 기본: 행 단위 키

소스가 append-only이고 소스 키가 유일하므로, 행 단위 키(`aggregate_key = source_key`)에서는 같은 키의 이벤트가 하나뿐이다. 즉 **키 단위 순서 문제가 원천적으로 없다.** 기본값으로 이것을 쓴다.

### 8.2 부모 키 단위 순서가 필요한 경우

컨슈머가 "주문(`order_no`) 단위로 라인 순서대로" 받아야 한다면 `aggregate_key = order_no`로 둔다. 이때 청크 병렬 처리 때문에 순서가 뒤집힐 수 있다: 라인 1이 청크 5, 라인 2가 청크 6에 들어가고 청크 6이 먼저 커밋되면 라인 2가 먼저 발행된다.

선택지:

| 방식 | 작동 | 비용 | 권장 |
|---|---|---|---|
| (a) 컨슈머 정렬 | 헤더의 `source_created_at` + payload의 PK로 컨슈머가 순서를 판단하거나, 순서 무관하게 처리 | 컨슈머 로직 | **기본 권장** |
| (b) 순서 보장 모드 | 소스 YAML `ordered: true`. claim 조건에 "처음으로 DONE이 아닌 청크만"을 추가해 소스 내 청크를 직렬 처리 | 소스당 처리량이 워커 1개 수준으로 떨어짐 | 처리량이 작고 순서가 계약인 소스만 |

(b)의 claim 조건(v3 §8.2 candidate에 추가):

```sql
AND c.chunk_id = (SELECT min(o.chunk_id) FROM poll_chunk o
                  WHERE o.source_id = c.source_id AND o.source_epoch = c.source_epoch
                    AND o.status <> 'DONE')
```

(b)는 청크 단위 직렬이고, 청크 안에서는 §3.4의 `ORDER BY i.ord`로 커서 순서대로 아웃박스에 들어간다. 릴레이가 아웃박스 ID 순서로 같은 키를 같은 파티션에 발행하면 부모 키 단위 순서가 유지된다. 단, `INSERT ... SELECT ... ORDER BY`로 identity 값이 순서대로 배정되는 것은 PostgreSQL 문서가 보장하는 동작이 아니므로, 순서가 계약인 소스에서는 헤더에 청크 안 순번(`ord`)을 함께 넣어 컨슈머가 확인할 수 있게 한다. DEAD 청크가 생기면 v3대로 소스가 멈추므로 순서를 건너뛰지 않는다.

## 9. 결정 ⑤ 불량 행 격리와 의도적 재발행

### 9.1 불량 행(poison row)

v3에서는 청크 안의 한 행 때문에 실패해도 청크 전체가 재시도 → DEAD → 소스 일시정지로 간다. 원인이 **그 행에만 있고, 다시 해도 똑같이 실패하는** 오류라면 재시도는 의미가 없고 소스 전체가 멈춘다.

격리 대상(행 단위·결정적 오류만):

- payload 크기가 Kafka `max.message.bytes`나 아웃박스 컬럼 제한을 넘음
- payload JSON 생성 실패(예: 허용하지 않은 값, 인코딩 오류)

격리 대상이 아닌 것(청크 재시도): 복제본·앱 DB 연결 오류, 타임아웃, recovery conflict, 제약 위반 같은 인프라·버그성 오류.

작동:

1. 워커가 행을 검사해 격리 대상이면 그 행을 입력 배열에서 빼고, 같은 적재 TX에서 `poll_row_quarantine(source_id, source_key, source_created_at, chunk_id, reason, payload_excerpt, created_at, resolved_at)`에 INSERT한다(`UNIQUE(source_id, source_key)`, `ON CONFLICT DO NOTHING`).
2. 격리된 행은 멱등 테이블에 넣지 않는다. 대신 sweep의 누락 판정에서 격리 테이블에 있는 키는 제외한다(누락 오탐 방지).
3. 청크는 정상 DONE이 된다. 소스는 멈추지 않는다.
4. `quarantined_rows > 0`이면 즉시 알람. 운영자가 원인을 고친 뒤 `poll_replay_request(reason='OPERATOR')`로 해당 키를 다시 넣으면 정상 적재 경로를 탄다. 성공하면 격리 행에 `resolved_at`을 기록한다.

소스 YAML `on-poison: QUARANTINE | PAUSE`로 고를 수 있게 하고, 기본은 `QUARANTINE`이다. 데이터는 격리 테이블에 남으므로 사라지지 않고, 한 행 때문에 실시간 처리 전체가 멈추는 것을 막는 쪽이 낫다. 행 하나도 순서를 건너뛰면 안 되는 `ordered: true` 소스는 `PAUSE`를 강제한다.

### 9.2 의도적 재발행

"이미 보낸 이벤트를 컨슈머에게 다시 보내야 한다"(컨슈머 데이터 유실 등)는 재처리와 다르다. 일반 replay는 멱등 테이블이 막아서 아무것도 발행하지 않는다(v3 runbook 4번의 "검증 replay").

재발행은 **세대(`emit_generation`)**를 올려서 한다.

- 운영자가 `replay_run(range 또는 키 목록, generation = n, 사유, 승인자)`을 등록한다.
- 해당 실행의 적재 TX는 `:generation = n`으로 멱등 INSERT를 하므로, 세대 0에서 반영된 키도 새로 통과한다.
- event_id도 세대가 달라지므로 새 값이 된다. 컨슈머는 헤더 `emit_generation > 0`으로 재발행임을 알 수 있다.
- 같은 세대 안에서는 다시 실행해도 한 번만 나간다(재발행 작업 자체도 멱등).

---

## 10. 구간별 보장 정리

| 구간 | 보장 | 장치 |
|---|---|---|
| 소스 커밋 → 복제본 가시화 | 커밋 순서대로, 늦게라도 반드시 | 물리 복제 |
| 가시화 → 청크 | 누락 없음(필수조건 준수 시), 겹침 없음 | safe_ts + Planner CAS |
| 청크 → 읽기 | 최소 한 번 | 리스·재claim |
| 읽기 → 아웃박스 | 행당 정확히 한 번(세대별) | 멱등 unique + fencing 단일 TX |
| 필수조건 위반 시 | 탐지 후 복구 | NEAR/DAILY sweep → replay |
| 아웃박스 → Kafka | 최소 한 번 | 기존 릴레이 |
| Kafka → 컨슈머 처리 | 실질적으로 한 번 | 결정적 event_id 중복 제거 |
| 순서 | 기본 보장 안 함(행 단위 키라 필요 없음). `ordered: true` 소스는 부모 키 단위 보장 | §8 |

## 11. 추가 지표와 알람

v3 §11.1에 다음을 더한다.

| 지표 | 의미 | 알람 |
|---|---|---|
| `ingest_rows_read`, `ingest_rows_new`, `ingest_rows_dup` | 청크별 읽은 수, 신규, 이미 반영됨 | `dup / read`가 5% 이상 지속 → 리스가 짧거나 재claim 폭증 |
| `ingest_payload_bytes` p50/p99, `chunk_bytes` 최대 | payload 크기 분포 | `chunk_bytes`가 `max-chunk-bytes`의 50% 초과 |
| `quarantined_rows` | 격리된 행 수 | 0 초과 즉시 |
| `idempotency_partition_count`, 가장 오래된 파티션 나이 | 보존 정책 동작 | 보존 기간 + 1개월 초과 |
| `e2e_latency` = 아웃박스 기록 시각 − `source_created_at` | 소스 커밋부터 적재까지 | `safety_lag + 5s` 초과 지속 |
| `outbox_unpublished_age` | 릴레이 정체(기존) | 기존 SLO |

## 12. v3 가이드에 반영할 변경점

1. §8.2 완료 SQL → 이 문서 §3.4로 교체(event_id 결정적, 헤더, `ORDER BY ord`, 세대).
2. §8.2 범위 조회 SELECT 목록에 `source_created_at`, `jsonb_build_object` payload 추가(§4.1).
3. §5 멱등 테이블 요구사항에 `emit_generation`, `source_created_at`, `event_id`, 파티셔닝 추가(§7).
4. `poll_source`에 `schema_version`, `payload_hash`, `idempotency_horizon`, `ordered`, `on_poison` 추가.
5. 새 테이블 `poll_row_quarantine`(§9.1), `replay_run`에 `generation` 추가(§9.2).
6. sweep 누락 판정에서 격리 키 제외, 하한에 `idempotency_horizon` 적용.
7. 소스 YAML 예시:

```yaml
sources:
  - id: order_line
    schema: sales
    table: order_line
    event-type: order_line.created
    schema-version: 1
    aggregate-key: source-key        # 또는 column:order_no
    ordered: false
    on-poison: QUARANTINE
    max-chunk-bytes: 8MiB
    payload-columns:
      - { column: order_no }
      - { column: line_no }
      - { column: item_cd }
      - { column: qty }
      - { column: amount, as-string: true }
      - { column: created_at }
```

## 13. 결정이 필요한 사항

| # | 질문 | 이 문서의 가정 |
|---|---|---|
| Q1 | 컨슈머 중 부모 키(예: 주문) 단위 순서가 계약인 곳이 있는가? | 없음 → 모든 소스 `ordered: false` |
| Q2 | 기존 아웃박스 테이블에 헤더(메타데이터) 컬럼이 있는가? event_id 타입은? | 헤더 jsonb 컬럼이 있다고 가정. 없으면 payload `meta`로 |
| Q3 | 앱 DB에 `uuid-ossp` 확장을 설치할 수 있는가? | 불가 시 Java에서 UUIDv5 계산 |
| Q4 | payload 형식은 JSON인가, Avro/Protobuf + 스키마 레지스트리인가? | JSON |
| Q5 | 기존 멱등 테이블의 현재 키 구조와 데이터 양 | 마이그레이션은 별도 단계(§7) |
| Q6 | 운영자 replay 허용 범위(얼마나 과거까지) | 90일 |
