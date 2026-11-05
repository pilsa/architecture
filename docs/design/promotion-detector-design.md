# 프로모션 디텍터 설계 (처리 흐름 · 테이블 설계)

> 기반 문서: `polling-design-v2.md`(범위 청크 + 리스 병렬 폴링). 이 문서는 그 위에 **프로모션 · 가입고객 · 다중 소스** 요구를 얹는다. 청크 분할/claim/fencing 세부는 기반 문서를 따른다.

## 0. 전제와 가정

| 항목 | 내용 |
|---|---|
| rdt_db | CDC로 복제된 읽기 전용 DB. 소스 테이블마다 `created_at` 있음. 쓰기 불가. 인덱스 추가는 **요청하면 가능**하다고 가정 |
| prm_db | 프로모션, 가입고객, 멱등, 아웃박스 테이블. 디텍터가 C/R/U/D 가능 |
| 규모 | 가입고객 300만, 프로모션 복수(수십 개 수준), 고객 1명이 여러 프로모션에 가입 가능 |
| 소스 | 프로모션마다 읽을 소스 테이블이 다르다. **여러 프로모션이 같은 소스를 공유할 수 있다** |
| 데이터 | 소스에는 비가입 고객 행이 섞여 있다. 가입 비율은 소스마다 다르다 |
| 운영 | 24시간 상시. 가입은 계속 늘어난다 |
| DBMS | PostgreSQL 기준으로 쓴다(기존 설계와 동일). 다른 DBMS면 문법만 바꾼다 |

rdt_db와 prm_db는 **서로 다른 DB라 JOIN할 수 없다**. 설계 전체가 이 제약에서 출발한다.

---

## 1. 결론 요약

1. **스캔 단위는 프로모션이 아니라 소스 테이블이다.** 소스 하나를 한 번만 읽고 그 소스를 쓰는 모든 프로모션에 결과를 나눠 준다. 프로모션 단위로 읽으면 같은 테이블을 N번 읽게 된다.
2. **필터는 "청크 스캔 + prm_db 배치 조회"로 한다.** 청크(약 1,000행)를 읽고, 고객번호를 모아 `cust_no = ANY(:arr)` 쿼리 한 번으로 가입 여부를 확인한다. 가입고객 테이블에는 이 조회 전용 **커버링 인덱스**(`cust_no → promo_id, 유효기간`)를 둔다. 청크마다 index-only scan 한 번(수 ms)이면 끝난다.
3. **고객 목록을 메모리에 캐시하지 않는다.** 300만 명은 메모리에 들어가지만, 캐시 갱신이 늦으면 신규 가입자 이벤트를 조용히 놓친다. DB 조회 비용이 충분히 싸므로 캐시가 주는 이득보다 정합성 위험이 크다.
4. **신규 가입 경합은 "가입 백필"로 막는다.** 가입 커밋 직전에 이미 처리된 청크에서 놓친 이벤트는 `[가입 유효시점, 백필 시작 시점의 planned_hwm]` 구간만 고객번호로 다시 조회해 채운다. 시간 가정 없이 누락이 없다(4장).
5. **멱등 키는 `(promo_id, source_id, source_pk)`이다.** 소스 행 하나가 여러 프로모션의 리워드가 될 수 있기 때문이다. 백필, 재처리, 리스 만료 재claim이 겹쳐도 중복 발행이 없다.
6. **가입 비율이 아주 낮은 소스만 예외적으로 "고객 주도 조회"**(가입자 목록으로 소스를 직접 조회)로 바꿀 수 있게 소스별 전략 플래그를 둔다(3장).

---

## 2. 처리 흐름

```
                         prm_db                                      rdt_db (RO, CDC)
 ┌───────────────────────────────────────────────┐          ┌─────────────────────────┐
 │ promotion / promo_source (설정, 앱 캐시)      │          │ src_a, src_b, ...       │
 │ promo_customer (가입고객, 조회용 인덱스)      │          │  idx (created_at, pk)   │
 │ poll_source / poll_chunk (조율)               │          │  idx (cust_no, created_at)│
 │ promo_backfill (가입·프로모션 백필 작업)      │          │ cdc_heartbeat           │
 │ reward_idem (멱등) / outbox                   │          └─────────────────────────┘
 └───────────────────────────────────────────────┘
        ▲ ① Planner: 소스별 HWM → 청크 생성 (안전 워터마크까지)
        │ ② Worker: 청크 claim (SKIP LOCKED, 리스)
        │ ③ 청크 범위 조회 ─────────────────────────────────────▶ 필요한 컬럼만 SELECT
        │ ④ 고객번호 distinct → promo_customer 배치 조회 (1쿼리)
        │ ⑤ 프로모션별 규칙 평가 (기간, 가입 유효구간, 행 조건)
        │ ⑥ 단일 TX: reward_idem INSERT ON CONFLICT DO NOTHING
        │            → 신규 건만 outbox INSERT → chunk DONE(lease_token 일치)
        │ ⑦ Backfill Worker: 신규 가입/신규 프로모션 구간 보정
```

### 2.1 Planner (소스 단위)

- `poll_source` 한 행 = rdt_db 소스 테이블 하나. 그 소스를 쓰는 활성 프로모션이 하나도 없으면 `enabled=false`로 멈춘다.
- 청크 경계 상한은 **안전 워터마크**로 정한다. rdt_db는 물리 복제가 아니라 CDC라서 기존 설계의 `pg_last_xact_replay_timestamp()`를 쓸 수 없다. 대신:
  - 원천 DB에 `cdc_heartbeat(ts)`를 1초마다 갱신하고 CDC로 같이 복제한다.
  - CDC가 원천 커밋 순서대로 적용한다면, rdt_db에서 보이는 `heartbeat.ts`보다 먼저 커밋된 원천 행은 모두 rdt_db에 들어와 있다.
  - `safe_ts = rdt_db의 heartbeat.ts - safety_lag`. `safety_lag`는 원천 쓰기 TX 최대 지속시간 + 시계 오차보다 크게 잡는다.
  - CDC 도구가 테이블별 병렬 적용(순서 비보장)을 한다면 테이블마다 heartbeat를 두거나 적용 지연 지표를 대신 써야 한다. **CDC 도구를 확인해야 하는 항목이다.**
- 청크 크기는 행 수 기준(기본 1,000)이다. 꼬리를 따라갈 때는 1,000행이 안 돼도 300~500ms마다 청크를 끊는다.

### 2.2 Worker (청크 처리)

1. claim: 기존 설계와 같다(`FOR UPDATE SKIP LOCKED`, `lease_token++`).
2. 범위 조회: `WHERE (created_at, pk...) > :from AND <= :to ORDER BY ...`. **SELECT 컬럼은 pk, created_at, cust_no, 규칙에 쓰는 컬럼만** 고른다. 소스별 YAML에서 프로모션 규칙이 참조하는 컬럼의 합집합으로 자동 계산한다.
3. 배치 가입 조회(청크당 1회):
   ```sql
   SELECT cust_no, promo_id, valid_from, valid_to
   FROM promo_customer
   WHERE cust_no = ANY(:custNos)          -- 청크의 distinct 고객번호 (≤1,000)
     AND promo_id = ANY(:promoIdsOfSource) -- 이 소스를 쓰는 활성 프로모션 (앱 캐시)
     AND status = 'ACTIVE';
   ```
   커버링 인덱스 `(cust_no, promo_id) INCLUDE (valid_from, valid_to, status)` 하나로 index-only scan이 된다.
4. 규칙 평가(앱 메모리): 행마다 매칭된 `(cust_no, promo_id)`에 대해
   - `promo.start_at <= row.created_at < promo.end_at`
   - `valid_from <= row.created_at < coalesce(valid_to, ∞)` (가입 이후, 탈퇴 이전 이벤트만)
   - 행 단위 조건(거래유형, 금액 하한 등). 소스 YAML/프로모션 설정의 조건식으로 둔다.
   - **누적 조건(예: "3회 이상", "월 합계 10만원")은 디텍터가 판단하지 않는다.** 디텍터는 적격 이벤트만 아웃박스로 보내고, 누적·한도는 리워드 엔진이 처리한다. 디텍터를 무상태로 두어야 재처리와 병렬화가 쉽다.
5. 커밋(prm_db 단일 TX): `reward_idem` INSERT ... ON CONFLICT DO NOTHING RETURNING → 새로 들어간 건만 `outbox` INSERT → `poll_chunk` DONE(`lease_token` 조건, 0행이면 롤백).

가입자가 한 명도 없는 청크는 ③·④만 하고 바로 DONE 처리한다(TX 비용 최소화).

### 2.3 처리량 감각

- 청크 1,000행 기준: rdt_db 범위 조회 20~50ms, 가입 조회 2~5ms, 적중 행 INSERT 수 ms. 대부분의 시간은 rdt_db 읽기에서 쓴다.
- 처리량은 pod 수 × pod당 워커 스레드 수로 늘어난다. 청크는 겹치지 않으므로 소스 하나도 여러 pod가 나눠 읽는다.
- 한계는 rdt_db 읽기 부하와 prm_db 쓰기(적중 건수)다. 소스별 `max_pending`, pod별 워커 수로 조절한다.

---

## 3. 비가입 고객 필터 전략 비교

| 전략 | 방식 | 장점 | 단점 | 채택 |
|---|---|---|---|---|
| A. 스캔 + 배치 조회 | 소스 청크를 전부 읽고 prm_db에 고객번호 배치 조회 | 단순. 가입 직후 반영(캐시 없음). 소스 인덱스는 `(created_at, pk)`만 있으면 됨 | 비가입 행도 읽음(읽기량 = 소스 전체) | **기본** |
| B. 메모리 캐시 | pod마다 가입자 Set/Bitmap을 들고 필터 | prm_db 조회 없음 | 갱신 지연 = 누락. pod 간 버전 불일치. 무효화 설계 필요 | 미채택 |
| C. 고객 주도 조회 | `WHERE cust_no = ANY(:가입자) AND created_at 구간` | 가입자가 적으면 읽기량이 아주 작음 | 가입자가 많으면 IN 목록 폭발. `(cust_no, created_at)` 인덱스 필요. 시간 구간을 고객 묶음 × 시간으로 쪼개야 함 | 예외(소스별 선택) |

**A가 기본인 이유.** 비가입 행 필터의 비용은 "읽은 행 수"보다 "판정 비용"에서 커지기 쉬운데, 배치 조회로 청크당 판정 비용을 쿼리 1회로 고정했다. 소스를 어차피 시간순으로 읽어야 하므로 비가입 행을 읽는 비용은 피하기 어렵다.

**C로 바꾸는 기준(소스별 `filter_strategy`).** 해당 소스 프로모션의 가입자 수가 적고(예: 수천 명 이하) 소스 유입량이 매우 많아 A의 읽기 비용이 문제가 될 때만 쓴다. 이때도 청크·멱등·백필 구조는 그대로 쓰고, 청크 안의 조회 쿼리만 바뀐다.

**B를 쓰지 않는 이유.** 캐시가 30초 늦으면 그 30초 동안 가입한 고객의 이벤트를 "비가입"으로 버린다. 4장의 백필도 "백필 이후에 계획된 청크는 최신 가입 정보로 조회한다"는 전제 위에 서 있어서, 캐시가 끼면 이 전제가 깨진다. DB 조회가 수 ms라 캐시로 얻는 이득이 거의 없다.

---

## 4. 신규 가입과의 경합 (가장 중요한 정확성 이슈)

### 4.1 문제

```
t1  Worker: 청크 K(이벤트 created_at = 10:00:05) 가입 조회 → 고객 C 없음 → 버림
t2  가입 서비스: 고객 C 가입 커밋 (valid_from = 10:00:00)
```

고객 C의 10:00:05 이벤트는 리워드 대상인데 이미 버려졌다. 소급 가입(유효시점이 과거인 가입)이 허용되면 이 구간은 더 길어진다.

### 4.2 해법: 가입 백필

- `promo_customer`에 `bf_status`(기본 `'PENDING'`)를 둔다. 가입 서비스는 평소대로 INSERT만 하면 된다(기본값).
- Backfill Worker가 `bf_status='PENDING'` 행을 `FOR UPDATE SKIP LOCKED`로 가져와 처리한다. 부분 인덱스라 대상은 항상 작다.
- 가입 1건 × 그 프로모션의 소스마다:
  1. `H = poll_source.planned_hwm` 을 읽는다(**가입 행이 커밋된 뒤**에 읽는 값).
  2. `valid_from > H.created_at` 이면 **SKIP**. 아직 청크로 만들어지지 않은 구간이라 정상 경로가 처리한다.
  3. 아니면 rdt_db에서 `WHERE cust_no = :c AND created_at >= :valid_from AND (created_at, pk) <= :H` 를 조회한다. 인덱스 `(cust_no, created_at)`를 쓴다.
  4. 같은 규칙 평가 → `reward_idem`/`outbox` INSERT(멱등) → `bf_status='DONE'`.

**누락이 없는 이유.**
- H 이하 청크: 가입 조회가 가입 커밋 전에 일어났을 수 있다 → 백필이 덮는다.
- H 초과 청크: H를 읽은 뒤에 계획되므로, 그 청크의 가입 조회는 가입 커밋 이후에 일어난다 → 정상 경로가 잡는다.
- 겹치는 구간(둘 다 처리)은 멱등 키가 중복을 막는다.
- 시간 지연이나 TX 지속시간에 대한 가정이 필요 없다.

**비용.** 일반 가입은 `valid_from ≈ 지금`이고 H는 safety_lag만큼 뒤에 있으므로 대부분 SKIP되거나 수 초 구간만 조회한다. 가입이 하루 10만 건이어도 초당 약 1.2건, 인덱스 조회 1회씩이다.

`(cust_no, created_at)` 인덱스를 rdt_db에 만들 수 없으면, PENDING 가입을 소스별로 모아 `created_at >= min(valid_from) AND cust_no = ANY(:묶음)`으로 범위 조회한다. `valid_from`이 최근이면 구간이 짧아 감당할 만하다. 소급 가입이 길면 이 방식은 비싸지므로 인덱스 요청을 선행 조건으로 둔다.

### 4.3 같은 패턴을 쓰는 다른 변경

| 변경 | 처리 |
|---|---|
| 신규 프로모션(기존 소스 공유), `start_at < planned_hwm` | `promo_backfill(promo_id, source_id, from=start_at, to=H)` 생성 → 청크로 나눠 `promo_scope=promo_id`로 처리. 해당 프로모션만 평가 |
| 신규 프로모션(새 소스) | `poll_source` 생성, `planned_hwm = (start_at, '', ...)` |
| 프로모션 종료 | `end_at` 이후 이벤트는 규칙에서 제외. 그 소스에 활성 프로모션이 없으면 HWM이 `end_at`을 넘은 뒤 `enabled=false` |
| 탈퇴 | `valid_to` 설정. `valid_to` 이후 이벤트 제외. 탈퇴 전 이미 발행된 건은 리워드 엔진 정책을 따른다 |

프로모션 설정(활성 프로모션 목록, 규칙)은 앱에서 캐시한다. 설정은 자주 바뀌지 않고, 바뀔 때는 위 표의 백필이 공백을 메운다. 캐시에 `config_version`을 두고 청크 처리 시작 시점에 고정해서 쓴다.

---

## 5. prm_db 테이블 설계

### 5.1 프로모션 설정

```sql
CREATE TABLE promotion (
  promo_id      varchar(32) PRIMARY KEY,
  name          varchar(200) NOT NULL,
  start_at      timestamptz NOT NULL,
  end_at        timestamptz NOT NULL,
  status        varchar(16)  NOT NULL,         -- READY|ACTIVE|ENDED
  retro_allowed boolean      NOT NULL DEFAULT false, -- 소급 가입 허용 여부
  updated_at    timestamptz  NOT NULL DEFAULT now()
);

-- 프로모션 ↔ 소스 매핑 + 행 단위 규칙
CREATE TABLE promo_source (
  promo_id   varchar(32) REFERENCES promotion,
  source_id  varchar(64) REFERENCES poll_source,
  rule_json  jsonb NOT NULL,     -- 예: {"txn_type":["PAY"],"amount_gte":10000}
  PRIMARY KEY (promo_id, source_id)
);
CREATE INDEX ix_promo_source_src ON promo_source (source_id);
```

규칙은 디텍터가 해석할 수 있는 제한된 조건식(동등, IN, 범위)만 허용한다. 임의 SQL은 받지 않는다.

### 5.2 가입고객 (조회 중심 설계)

```sql
CREATE TABLE promo_customer (
  promo_id    varchar(32) NOT NULL,
  cust_no     varchar(20) NOT NULL,     -- 소스의 고객번호와 같은 타입·형식
  valid_from  timestamptz NOT NULL,     -- 리워드 대상 시작 시점 (보통 가입 시각, DB now())
  valid_to    timestamptz,              -- 탈퇴 시각, NULL = 유지
  status      varchar(8)  NOT NULL DEFAULT 'ACTIVE',
  bf_status   varchar(8)  NOT NULL DEFAULT 'PENDING',  -- PENDING|DONE|SKIP
  joined_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (promo_id, cust_no)       -- 업무 조회: 프로모션별 가입자 목록
);

-- 디텍터 전용: 고객번호 → 가입 프로모션 (index-only scan)
CREATE UNIQUE INDEX ux_pc_detect
  ON promo_customer (cust_no, promo_id)
  INCLUDE (valid_from, valid_to, status);

-- 백필 대기열 (항상 작음)
CREATE INDEX ix_pc_bf_pending
  ON promo_customer (joined_at)
  WHERE bf_status = 'PENDING';
```

설계 포인트:

- **선두 컬럼이 `cust_no`인 인덱스가 핵심이다.** 디텍터의 질문은 항상 "이 고객들이 어떤 프로모션에 가입했나"다. PK `(promo_id, cust_no)`만 있으면 `cust_no = ANY` 조회가 프로모션 수만큼 인덱스를 훑는다.
- **INCLUDE로 테이블 접근을 없앤다.** 규칙 평가에 필요한 `valid_from/valid_to/status`가 인덱스에 있어 heap을 읽지 않는다. index-only scan이 실제로 되려면 visibility map이 최신이어야 하므로, 이 테이블은 autovacuum을 공격적으로 둔다(`autovacuum_vacuum_insert_scale_factor` 낮춤, PG13+).
- **고객 1명 = 프로모션 수만큼 행(정규화)**을 유지한다. 고객 1행에 `promo_ids[]` 배열로 비정규화하면 조회는 조금 빨라지지만, 가입/탈퇴마다 같은 행을 갱신해 경합과 bloat가 생기고 프로모션별 유효구간 표현이 어렵다.
- **용량.** 300만 명 × 평균 2~3개 = 600만~900만 행. heap 약 0.6~0.9GB, 디텍터 인덱스 약 0.4~0.6GB. 인덱스 전체가 shared_buffers에 올라가는 크기라 조회는 메모리에서 끝난다. 파티셔닝은 필요 없다. 수천만 행을 넘기면 `cust_no` 해시 파티션(8~16개)을 고려한다.
- **타입 일치.** `cust_no`의 타입·자릿수·앞자리 0 처리 같은 형식은 소스와 완전히 같게 맞춘다. 정규화가 다르면 조회가 조용히 빗나간다. 소스마다 형식이 다르면 소스 YAML에 정규화 함수를 지정한다.
- `valid_from`은 앱 시각이 아닌 DB `now()`로 넣고, 소급 가입일 때만 과거 값을 허용한다(`promotion.retro_allowed`).

### 5.3 조율 테이블

기존 설계의 `poll_source`, `poll_chunk`를 그대로 쓰고 아래만 추가한다.

```sql
ALTER TABLE poll_source
  ADD COLUMN filter_strategy varchar(16) NOT NULL DEFAULT 'SCAN';  -- SCAN|CUSTOMER

ALTER TABLE poll_chunk
  ADD COLUMN promo_scope varchar(32);   -- NULL=정상 청크, 값=해당 프로모션 백필 청크

CREATE TABLE promo_backfill (
  backfill_id  bigserial PRIMARY KEY,
  promo_id     varchar(32) NOT NULL,
  source_id    varchar(64) NOT NULL,
  from_key     text[] NOT NULL,
  to_key       text[] NOT NULL,          -- 생성 시점의 planned_hwm
  status       varchar(16) NOT NULL DEFAULT 'READY',  -- READY|PLANNED|DONE
  created_at   timestamptz NOT NULL DEFAULT now()
);
```

프로모션 백필은 `promo_backfill` 구간을 Planner가 `promo_scope`가 붙은 청크로 잘라 넣고, 워커는 같은 경로로 처리하되 그 프로모션만 평가한다. 정상 청크와 같은 소스를 동시에 읽으므로 `max_pending`을 따로 둬 정상 처리 지연을 막는다.

### 5.4 멱등 · 아웃박스

```sql
CREATE TABLE reward_idem (
  promo_id     varchar(32) NOT NULL,
  source_id    varchar(64) NOT NULL,
  source_key   text        NOT NULL,     -- 소스 PK 값의 JSON 배열 인코딩
  cust_no      varchar(20) NOT NULL,
  event_at     timestamptz NOT NULL,     -- 소스 created_at
  created_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (promo_id, source_id, source_key)
) PARTITION BY LIST (promo_id);          -- 프로모션 종료 후 파티션 단위 보관/삭제
```

- 파티션 키를 `promo_id`로 두면 프로모션이 끝나고 보관 기간이 지난 뒤 `DETACH/DROP` 한 번으로 정리된다. 프로모션 하나가 매우 크면 그 안에서 `event_at` 월 단위 하위 파티션을 쓴다.
- 아웃박스는 기존 구현을 그대로 쓴다. 페이로드에 `promo_id, cust_no, source_id, source_key, event_at`과 규칙 평가에 쓴 값을 담는다. 아웃박스 메시지 키를 `cust_no`로 두면 Kafka 파티션 안에서 고객별 순서가 유지되어 리워드 엔진의 누적 계산이 쉬워진다.

---

## 6. rdt_db 요구사항 (소스 소유팀에 요청)

| 항목 | 용도 | 필수 |
|---|---|---|
| 소스별 인덱스 `(created_at, pk...)` | 청크 경계 계산과 범위 조회 | 필수 |
| 소스별 인덱스 `(cust_no, created_at)` | 가입 백필, 고객 주도 조회 | 강력 권장(소급 가입이 있으면 필수) |
| `cdc_heartbeat` 복제 | 안전 워터마크 | 필수 |
| `created_at` NOT NULL, 불변, 원천 DB 시각 | 커서 정합성 | 필수 |
| CDC 적용 순서(커밋 순서 보장 여부) | 워터마크 방식 결정 | 확인 필요 |
| 원천 쓰기 TX 최대 지속시간 | `safety_lag` 산정 | 확인 필요 |

---

## 7. 다중 pod 운영

- 모든 pod가 Planner, Worker, Backfill Worker를 함께 돌린다. 조율은 전부 prm_db 행 잠금(`SKIP LOCKED`)과 리스로 하므로 리더 선출이 없다.
- 커넥션 풀은 rdt_db용과 prm_db용을 분리한다. rdt_db 풀 크기가 곧 소스 읽기 동시성 상한이다.
- 소스 간 공정성: 워커는 소스를 라운드로빈으로 돌며 claim한다. 유입이 큰 소스가 작은 소스를 굶기지 않게 소스별 동시 claim 상한을 둔다.
- 스케일 아웃 신호: 소스별 `안전 워터마크 - 처리 완료 워터마크` 지연이 목표(예: 10초)를 넘으면 HPA로 pod를 늘린다.

## 8. 모니터링 지표

| 지표 | 의미 / 알람 |
|---|---|
| `detect_lag_seconds{source}` | 안전 워터마크와 DONE 청크의 차이. SLA 초과 시 알람 |
| `cdc_lag_seconds` | 원천 now() - rdt_db heartbeat. CDC 장애 감지 |
| `chunk_rows`, `chunk_hit_ratio{source}` | 가입자 적중 비율. 매우 낮게 유지되면 전략 C 검토 |
| `member_lookup_ms` p99 | 가입 조회 지연. 오르면 index-only scan 깨짐(vacuum) 의심 |
| `backfill_pending_count`, 최고 age | 가입 백필 적체 |
| `backfill_recovered_rows` | 백필로 새로 발행된 건. 일반 가입에서는 0에 가까워야 함 |
| `chunk_dead_count` | 재시도 초과 청크. 즉시 알람 |

## 9. 확인이 필요한 사항

1. CDC 도구와 적용 순서 보장 여부(Debezium, DMS, OGG 등). 안전 워터마크 방식이 달라진다.
2. rdt_db에 `(cust_no, created_at)` 인덱스 추가가 가능한가.
3. 소급 가입(가입 전 이벤트 인정) 정책이 있는가. 있다면 최대 소급 기간은 얼마인가.
4. 프로모션 조건 중 누적·한도 조건을 리워드 엔진이 처리하는가(이 설계는 그렇게 가정).
5. 소스 테이블마다 고객번호 컬럼명·형식이 같은가.
6. 소스별 유입량(TPS)과 가입자 비율. pod 수와 청크 크기 산정에 쓴다.
