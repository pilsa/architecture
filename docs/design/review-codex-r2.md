# Read-Only 소스 병렬 폴링 설계 검토 — Codex R2

## 결론 요약

| 쟁점 | 판정 | 최종 권고 |
|---|---|---|
| 1. sweep | **[조건부 동의]** | writer의 최대 transaction lifetime과 `created_at` 오차가 실제로 강제된다면 full sweep 상시 순환은 필수가 아니다. 근거리/일일 sweep은 보증 수단이 아니라 계약 위반 탐지·복구 수단으로 둔다. |
| 2. planner | **[동의]** | 정확성은 `source_epoch + expected planned_hwm` CAS와 chunk INSERT의 단일 TX로 충분하다. token 없는 `owner + lease_until`을 중복 조회 억제용으로 쓰고 advisory lock은 기본안으로 쓰지 않는다. |
| 3. 경계 타입 | **[조건부 동의]** | 허용 타입을 제한하고 catalog 대조·canonical round-trip 시험을 강제하면 전체 tuple `text[]` + typed `to_created_at`으로 충분하다. 임의 PostgreSQL 타입까지 “범용”으로 받는 안은 반대한다. |
| 4. collation | **[조건부 동의]** | `C`는 권장으로 낮춰도 된다. 단, index/query collation 동일성과 deterministic 여부뿐 아니라 collation version, B-tree/opclass, 키 순서·방향, index 유효성까지 기동 시 검증해야 한다. |

## 1. sweep 계층화와 full sweep 기본 비활성

**판정: [조건부 동의]**

Claude의 원인 분석이 맞다. planner/worker가 멈춘 동안 `planned_hwm`도 멈추므로 그 시간에 정상 commit된 행은 재기동 후 `(planned_hwm, ...]`에 남는다. 물리 replica도 WAL을 순서대로 replay하므로 아직 보이지 않는 commit 앞을 `replay_ts`가 건너뛰지 않는다. 따라서 장애시간이나 통상적인 replica 지연은 그 자체로 누락 원인이 아니다.

정상 keyset 경로가 놓치는 행은 HWM이 이미 그 키를 지난 뒤 가시화된 행, 즉 본질적으로 다음 경우다.

- DB `now()`를 썼다면 `commit_time - transaction_start_time >= safety_lag`인 late commit
- 앱이 `created_at`을 넣는다면 위 값에 backdating/시계 오차를 더한 값이 `safety_lag`를 넘는 경우
- 운영 중 cursor 값 또는 정렬 규칙이 바뀌는 계약 위반

따라서 아래가 모두 **온보딩 차단 조건**이면 무한 순환 full sweep을 정상 정확성 장치로 상시 돌릴 필요는 없다.

1. `B = max(commit_time - effective_created_at)`의 hard bound가 있다.
2. `safety_lag > B + clock/측정 여유`가 강제된다.
3. PostgreSQL 17+에서는 writer에 non-zero `transaction_timeout`을 적용하고, `max_prepared_transactions=0` 등으로 2PC를 금지한다. `transaction_timeout`은 prepared transaction에 적용되지 않는다.
4. 앱 지정 `created_at`을 허용한다면 단순 NTP 정책이 아니라 허용 backdating까지 `B`에 포함해 강제한다. 이를 강제할 수 없으면 DB가 생성하는 값만 허용한다.
5. cursor 컬럼은 `NOT NULL`이고 insert 후 불변이며, source는 append-only다.

다만 `ALTER ROLE writer SET transaction_timeout=...`만으로 적대적·오설정 writer에 대한 DB-level hard bound가 되지는 않는다. `transaction_timeout`은 user-settable GUC라 writer가 세션에서 `SET transaction_timeout=0`으로 덮을 수 있다. 신뢰된 writer 코드/프록시에서 `SET`을 차단하고 연결마다 설정을 검증하거나 동등한 외부 강제를 둬야 한다. PostgreSQL의 parameter `SET` privilege는 원래 superuser-only인 parameter 권한을 확장하는 기능이므로 user-settable GUC의 변경 금지 수단이 아니다. [transaction_timeout](https://www.postgresql.org/docs/17/runtime-config-client.html), [Parameter 설정](https://www.postgresql.org/docs/17/config-setting.html), [2PC 설정](https://www.postgresql.org/docs/17/runtime-config-resource.html#GUC-MAX-PREPARED-TRANSACTIONS)

**최종 권고**

- 1분 주기 근거리 sweep과 일 1회 24–48시간 sweep, 감사 시 수동 full sweep의 계층화에 동의한다.
- 근거리 `window = 2B`는 경험식일 뿐이다. 탐지하려는 계약 초과분, sweep 주기, 허용 replica 지연을 포함해 `window >= 탐지대상 최대 age + 주기 + 지연 여유`로 산정한다.
- 두 sweep을 “정상 경로 보증”이 아니라 invariant 위반 탐지·복구로 명시하고, `recovered_rows > 0`이면 writer bound 위반으로 page한다.
- hard bound나 앱 timestamp 제한을 실제로 강제하지 못하는 source만 full sweep 상시 순환 또는 CDC를 필수로 한다. 따라서 full sweep **전역 기본 활성은 불필요**, source별 risk flag로 선택한다.

## 2. planner CAS, lease, advisory lock

**판정: [동의]**

아래 불변식만 지키면 planner token은 정확성 장치가 아니다.

- replica 조회 전에 읽은 `(source_id, source_epoch, expected_hwm, config_hash)`를 보관한다.
- replica I/O는 앱 DB transaction 밖에서 수행한다.
- 짧은 앱 DB TX에서 pending 수를 다시 확인하고, `source_epoch/config_hash/expected_hwm` CAS로 HWM을 전진시킨 뒤 chunk를 INSERT한다.
- CAS 1행과 chunk INSERT가 같은 TX이며 어느 하나라도 실패하면 전체 rollback한다.

동일 HWM에서 여러 planner가 조회해도 CAS 승자 하나만 chunk를 확정한다. 늦은 planner의 boundary가 더 작거나 커도 CAS 패자는 결과를 버리므로 겹침이나 건너뜀은 생기지 않는다.

```sql
BEGIN;

UPDATE poll_source AS s
SET planned_hwm = :new_hwm,
    updated_at = statement_timestamp()
WHERE s.source_id = :source_id
  AND s.source_epoch = :source_epoch
  AND s.config_hash = :config_hash
  AND s.enabled
  AND s.planned_hwm IS NOT DISTINCT FROM :expected_hwm
  AND (
    SELECT count(*)
    FROM poll_chunk AS c
    WHERE c.source_id = s.source_id
      AND c.source_epoch = s.source_epoch
      AND c.status IN ('READY', 'CLAIMED')
  ) < s.max_pending;

-- UPDATE row count가 1일 때만 실행. INSERT 실패 또는 0행이면 전체 rollback.
INSERT INTO poll_chunk
  (source_id, source_epoch, config_hash, from_key, to_key, status)
VALUES
  (:source_id, :source_epoch, :config_hash, :expected_hwm, :new_hwm, 'READY');

COMMIT;
```

`planned_hwm`을 nullable 초기 상태로 쓴다면 `=`가 아니라 위처럼 `IS NOT DISTINCT FROM`을 써야 한다. 또는 `has_hwm/initial_start_ts`를 분리해 HWM을 non-null로 유지한다.

중복 replica 조회를 줄이기 위한 lease는 token 없이도 충분하다. 다만 `lease_until`만 두기보다 진단과 안전한 갱신을 위해 `planner_owner + planner_lease_until`을 두고 atomic conditional UPDATE로 claim한다. lease 만료 후의 stale planner도 최종 CAS에서 fence되므로 lease token은 정확성에 기여하지 않는다. stale owner가 새 lease를 지우지 않도록 명시적 release를 생략하거나 `owner + 기존 lease_until` 일치 조건으로 release한다.

`pg_try_advisory_lock`도 가능하지만 기본 권고는 아니다. transaction-level lock은 replica 조회 동안 앱 DB TX를 다시 길게 만들고, session-level lock은 조회 내내 동일한 앱 DB physical connection을 전용으로 잡아야 한다. pool 반환/connection 교체 시 lock 소유 세션이 달라지는 문제도 있다. dedicated connection, 충돌 없는 numeric key, session 종료 시 자동 해제를 운영할 수 있을 때만 lease의 대안으로 쓴다. [Advisory lock 동작](https://www.postgresql.org/docs/17/explicit-locking.html#ADVISORY-LOCKS)

**최종 권고:** 정확성은 CAS로 단일화하고, token 없는 persisted lease는 비용 최적화로 유지한다. advisory lock은 source 수가 매우 적고 dedicated connection 비용이 허용되는 배치에만 선택한다.

## 3. 전체 tuple `text[]`와 typed 경계

**판정: [조건부 동의]**

source별 typed coordination table은 정확성의 필수조건이 아니며, 현재처럼 source가 여러 개이고 PK 모양이 다른 경우에는 운영비가 더 크다. 아래 조건이면 전체 tuple `text[]`가 DB 간 transport/storage 형식으로 충분하다.

1. `source_cursor_column(source_id, position, column_name, type_schema, type_name, type_oid_at_validation, typmod, collation_schema, collation_name, direction)`를 두고 실제 source catalog와 기동 시 대조한다. OID만 영구 식별자로 믿지 말고 schema/name도 저장한다.
2. 허용 cursor 타입을 canonical text round-trip이 검증된 built-in 타입으로 제한한다. 우선 `timestamptz`, `text/varchar`, 정수, `uuid` 정도로 시작한다. domain, enum, locale-sensitive/custom type, `float`, `numeric` 특수값 등은 별도 승인·시험 없이 받지 않는다.
3. source 세션마다 `TimeZone='UTC'`, `DateStyle='ISO, YMD'`를 설정하고 pool checkout 때 실제 값을 확인한다. 허용 타입을 넓히면 `IntervalStyle`, `bytea_output` 등 해당 typoutput/typinput에 영향을 주는 GUC도 고정한다.
4. 경계는 source에서 `::text`로 만들고 같은 source type으로 cast해 되돌리며, Java가 parse하거나 배열 literal을 조립하지 않는다. custom JDBC `text[]` handler를 사용한다.
5. 온보딩 시험에서 각 컬럼에 대해 `v IS NOT DISTINCT FROM ((v::text)::declared_type)`와 경계 전후 ordering, 최대 길이·Unicode·quote/backslash를 검증한다.

typed 컬럼은 `poll_chunk.to_created_at timestamptz NOT NULL` 하나면 정상 청크의 모니터링·시간 기반 탐색에는 충분하다. 최초 시작점은 `poll_source.initial_start_ts timestamptz`로 별도 보존한다. history partition retention 기준은 source event time보다 `archived_at/done_at`이 더 안전하므로 그 컬럼도 typed여야 하지만 이는 cursor 표현 문제와 별개다.

**최종 권고:** 제한된 타입 registry + 전체 tuple `text[]` + `to_created_at`을 채택한다. “모든 PostgreSQL 타입을 metadata만으로 수용”하는 범용안은 거부하고, 새 타입은 codec/round-trip/ordering 시험을 통과한 뒤 allowlist에 추가한다.

## 4. `C`가 아닌 deterministic collation 허용

**판정: [조건부 동의]**

소스 소유팀이 기존 collation과 다른 `COLLATE "C"` index를 거부한다면 기존 deterministic collation을 허용해도 keyset 정확성은 유지할 수 있다. deterministic collation은 locale 비교가 동률이어도 byte-wise tie-break를 하므로 서로 다른 byte string을 같다고 보지 않는다. 그러나 다음을 모두 만족해야 한다. [Deterministic collation](https://www.postgresql.org/docs/17/sql-createcollation.html), [pg_collation](https://www.postgresql.org/docs/17/catalog-pg-collation.html)

- index 각 key의 collation OID와 column/parameter에 명시한 query collation OID가 정확히 같다.
- cursor 전체가 `NOT NULL`, 전부 ASC이고 query의 row comparison과 `ORDER BY`가 같은 컬럼·타입·collation을 쓴다.
- index는 B-tree이고 예상 opclass, key 순서, 유효/ready/live 상태를 만족한다. expression/partial index는 별도 승인 없이는 거부한다.
- `collisdeterministic=true`이며 catalog의 `collversion`과 replica 호스트의 `pg_collation_actual_version()`이 일치한다. 불일치는 index ordering을 신뢰할 수 없는 기동 차단 사유다. [Collation version 불일치](https://www.postgresql.org/docs/current/sql-altercollation.html)
- OS/ICU/PostgreSQL upgrade 뒤에는 source pause → REINDEX → version refresh → 새 epoch 또는 검증 sweep 절차를 거친다.

다음 SQL을 **실제 읽을 replica endpoint**에서 실행한다. `:qualified_index_name`에는 기동 설정에 등록된 schema-qualified index 이름을 넣는다. 결과가 없거나 header/key 중 하나라도 metadata와 다르면 source를 enable하지 않는다.

```sql
WITH target AS MATERIALIZED (
  SELECT
      i.indexrelid,
      i.indrelid,
      i.indnkeyatts,
      i.indnatts,
      i.indisvalid,
      i.indisready,
      i.indislive,
      i.indpred,
      i.indexprs,
      i.indkey,
      i.indcollation,
      i.indclass,
      i.indoption,
      am.amname,
      ni.nspname AS index_schema,
      ci.relname AS index_name,
      nt.nspname AS table_schema,
      ct.relname AS table_name
  FROM pg_index AS i
  JOIN pg_class AS ci ON ci.oid = i.indexrelid
  JOIN pg_namespace AS ni ON ni.oid = ci.relnamespace
  JOIN pg_class AS ct ON ct.oid = i.indrelid
  JOIN pg_namespace AS nt ON nt.oid = ct.relnamespace
  JOIN pg_am AS am ON am.oid = ci.relam
  WHERE ci.oid = to_regclass(CAST(:qualified_index_name AS text))
), key_detail AS (
  SELECT
      t.*,
      s.pos0 + 1 AS position,
      a.attname AS column_name,
      a.attnotnull,
      a.atttypid AS type_oid,
      a.atttypmod AS typmod,
      format_type(a.atttypid, a.atttypmod) AS formatted_type,
      opc.oid AS opclass_oid,
      nopc.nspname AS opclass_schema,
      opc.opcname AS opclass_name,
      opc.opcdefault,
      coll.oid AS collation_oid,
      ncoll.nspname AS collation_schema,
      coll.collname AS collation_name,
      coll.collprovider,
      coll.collisdeterministic,
      coll.collversion,
      pg_collation_actual_version(coll.oid) AS actual_collversion,
      (t.indoption[s.pos0] & 1) = 0 AS is_asc,
      (t.indoption[s.pos0] & 2) <> 0 AS nulls_first
  FROM target AS t
  CROSS JOIN LATERAL generate_series(0, t.indnkeyatts - 1) AS s(pos0)
  JOIN pg_attribute AS a
    ON a.attrelid = t.indrelid
   AND a.attnum = t.indkey[s.pos0]
   AND NOT a.attisdropped
  JOIN pg_opclass AS opc ON opc.oid = t.indclass[s.pos0]
  JOIN pg_namespace AS nopc ON nopc.oid = opc.opcnamespace
  LEFT JOIN pg_collation AS coll ON coll.oid = t.indcollation[s.pos0]
  LEFT JOIN pg_namespace AS ncoll ON ncoll.oid = coll.collnamespace
)
SELECT
    index_schema,
    index_name,
    table_schema,
    table_name,
    amname,
    indisvalid,
    indisready,
    indislive,
    indpred IS NULL AS is_not_partial,
    indexprs IS NULL AS has_no_expressions,
    indnkeyatts,
    indnatts,
    position,
    column_name,
    attnotnull,
    type_oid,
    typmod,
    formatted_type,
    opclass_oid,
    opclass_schema,
    opclass_name,
    opcdefault,
    collation_oid,
    collation_schema,
    collation_name,
    collprovider,
    collisdeterministic,
    collversion,
    actual_collversion,
    collversion IS NOT DISTINCT FROM actual_collversion
      AS collation_version_ok,
    is_asc,
    nulls_first
FROM key_detail
ORDER BY position;
```

애플리케이션은 결과를 `source_cursor_column`과 position별로 대조하고 다음 predicate를 모두 요구한다.

```text
amname = 'btree'
AND indisvalid AND indisready AND indislive
AND is_not_partial AND has_no_expressions
AND indnkeyatts = cursor_column_count
AND every(attnotnull AND is_asc)
AND every(column/type/typmod/opclass/collation == registered spec)
AND every(noncollatable OR (collisdeterministic AND collation_version_ok))
```

query SQL은 catalog에서 검증한 collation을 schema-qualified identifier로 렌더링하여 column과 parameter 양쪽에 명시한다. 마지막으로 production-like 값으로 `EXPLAIN (ANALYZE, BUFFERS)`를 실행해 `Index Cond`와 sort 부재를 온보딩 증거로 남긴다. `C`는 가장 이식성 높은 신규 index 권장값으로 유지하되 필수값으로 두지 않는다.

## R1에서 추가로 드러난 결함

1. **writer role 기본 GUC는 hard bound가 아니다.** 앞서 설명한 대로 `ALTER ROLE ... SET transaction_timeout`은 writer가 세션에서 0으로 덮을 수 있다. “강제”의 주체와 우회 방지, 실제 세션 설정 모니터링을 온보딩 체크리스트에 추가해야 한다.
2. **DEAD가 backpressure에서 빠지면 source가 손실 gap을 남긴 채 계속 전진한다.** 현재 pending 계산이 READY/CLAIMED만 세면 DEAD 이후에도 planner가 무한히 새 chunk를 만든다. source의 contiguous completed HWM은 멈추더라도 운영자가 gap을 놓칠 수 있으므로 `DEAD > 0`이면 자동 hard-pause하거나 DEAD를 unresolved backlog로 계산하고, 승인된 recovery chunk가 생겨야 resume하도록 한다.
