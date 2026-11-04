# Read-Only 소스 병렬 폴링 설계 v2 검토 — Codex R1

## 검토 범위와 판정 기준

- 기준 문서는 `polling-design-v2.md`이다. PostgreSQL 버전이 명시되지 않아 **PostgreSQL 18 공식 문서**를 주 근거로 삼았고, 14–18에서 달라질 수 있는 기능은 별도로 표시했다.
- 대상 버전, 소스 컬럼의 실제 `NOT NULL`/collation/opclass, writer 트랜잭션 제한, MyBatis/pgJDBC 버전이 제시되지 않았다. 이에 의존하는 판단은 조건 또는 **불확실**로 명시했다.
- `[OK]`는 현재 원리가 PostgreSQL 동작과 맞다는 뜻이다. `[개선권고]`는 핵심 원리는 맞지만 운영 전 명세/보강이 필요하다는 뜻이다. `[결함]`은 현재 설명대로 구현하면 누락, 영구 정체, 또는 DDL/운영 불일치가 생길 수 있다는 뜻이다.

## A. 정합성(누락·중복)

### 1. `(created_at, varchar PK...)` row-value keyset 및 B-tree/collation

**[개선권고] 근거(PG 문서/동작)**

PostgreSQL의 `<`, `<=`, `>`, `>=` row constructor 비교는 왼쪽부터 비교하여 최초의 unequal 또는 null 쌍에서 멈추는 사전식(lexicographic) 비교다. 각 비교 연산자는 B-tree operator class의 멤버여야 한다. 따라서 모든 키가 non-null이고, 비교식과 `ORDER BY`가 동일한 타입·collation·방향을 쓰며, 전체 tuple이 유일하면 `(after, to]` keyset 자체는 정확하다. 반대로 `created_at`이 null이면 row 비교 결과가 null이 되어 해당 행은 영구적으로 빠질 수 있다. [Row Constructor Comparison](https://www.postgresql.org/docs/18/functions-comparisons.html#ROW-WISE-COMPARISON)

효율적인 실행을 위해 소스에 정확히 같은 순서의 복합 B-tree가 필요하다. 다중 B-tree는 선두 컬럼의 equality와 그 다음 inequality를 가장 직접적으로 scan 범위로 쓰므로, 이 설계에서는 `(created_at, pk1, ..., pkn)`가 올바른 기본 인덱스다. 단순히 기존 `(pk1, ...)` PK만 있는 것으로는 충분하지 않다. 실제 계획에서 row comparison이 `Index Cond`가 되고 `Sort` 없이 limit을 만족하는지는 소스별 `EXPLAIN (ANALYZE, BUFFERS)`로 검증해야 한다. [Multicolumn Indexes](https://www.postgresql.org/docs/18/indexes-multicolumn.html), [Indexes and ORDER BY](https://www.postgresql.org/docs/18/indexes-ordering.html)

문자열의 `<`와 `ORDER BY`는 collation을 사용한다. 비결정적 ICU collation은 서로 다른 바이트 문자열을 equal로 볼 수 있고 B-tree deduplication도 사용할 수 없다. 소스 PK/unique constraint와 cursor 비교가 **같은** 비결정적 collation을 쓰면 constraint가 비교상 동률인 PK 조합의 공존을 막으므로 논리적 유일성은 유지되지만, 성능 저하와 ICU 버전 변경에 따른 순서 변경 위험이 남는다. collation 버전이 바뀌면 PostgreSQL도 affected index의 `REINDEX`를 요구한다. 이미 저장한 HWM의 앞/뒤 관계도 바뀔 수 있으므로 단순 REINDEX만 하고 폴링을 계속해서는 안 된다. [Collation Support](https://www.postgresql.org/docs/18/collation.html#COLLATION-NONDETERMINISTIC), [ALTER COLLATION](https://www.postgresql.org/docs/18/sql-altercollation.html)

**구체적 수정안**

1. 소스 온보딩 조건에 `created_at IS NOT NULL`, 실제 PK 컬럼 전부 `NOT NULL`, 전체 cursor tuple unique를 넣는다. PK는 원래 non-null이지만 `created_at`은 별도 확인이 필요하다.
2. 가능한 경우 cursor 전용 정렬을 deterministic하고 안정적인 `C` collation으로 고정하고, 인덱스와 모든 비교/정렬에 동일하게 명시한다.

```sql
-- primary에서 소스 소유팀이 실행. standby에서는 CREATE INDEX 불가.
CREATE INDEX CONCURRENTLY ix_src_poll_cursor
ON source_schema.source_table
  (created_at, pk1 COLLATE "C", pk2 COLLATE "C");

SELECT ...
FROM source_schema.source_table
WHERE ROW(created_at, pk1 COLLATE "C", pk2 COLLATE "C")
    > ROW(CAST(? AS timestamptz),
          CAST(? AS varchar) COLLATE "C",
          CAST(? AS varchar) COLLATE "C")
  AND created_at < CAST(? AS timestamptz)
ORDER BY created_at, pk1 COLLATE "C", pk2 COLLATE "C"
LIMIT ?;
```

3. cursor 명세에 각 컬럼의 PostgreSQL 타입, typmod, collation, ASC/DESC, NULLS 정책, index 이름/정의를 저장하고 기동 시 catalog와 대조한다.
4. ICU/libc 또는 PostgreSQL major upgrade 시 해당 source를 pause하고 collation version 확인, REINDEX, 전체 검증 sweep 또는 새 epoch 재기동 절차를 둔다.

### 2. `pg_last_xact_replay_timestamp() - lag`의 가시성 갭 방지 논리

**[결함] 근거(PG 문서/동작)**

`now()`/`CURRENT_TIMESTAMP`/`transaction_timestamp()`는 **트랜잭션 시작시각**이다. `pg_last_xact_replay_timestamp()`는 standby가 마지막으로 replay한 트랜잭션의 commit 또는 abort WAL record가 primary에서 생성된 시각이다. 즉 두 값은 같은 종류의 시각이 아니다. [Date/Time Functions](https://www.postgresql.org/docs/18/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT), [Recovery Information Functions](https://www.postgresql.org/docs/18/functions-admin.html#FUNCTIONS-RECOVERY-INFO-TABLE)

현재 안전성 주장은 아래 실행으로 깨진다.

1. `Tlong`이 `t0`에 시작하고 `created_at DEFAULT now()`인 행을 INSERT하지만 아직 commit하지 않는다.
2. 다른 짧은 트랜잭션들이 `t1`까지 commit되고 standby가 이를 replay한다.
3. `t1 - safety_lag > t0`가 되면 planner는 `Tlong`의 행을 볼 수 없는 상태에서 HWM을 그 키 뒤로 전진시킨다.
4. `Tlong`이 나중에 commit/replay되면 그 행은 이미 HWM 뒤에 생겨 정상 폴링에서 영구 누락된다.

WAL은 commit record가 적용되기 전에는 그 트랜잭션의 변경을 visible하게 만들지 않으므로, **자기 commit record가 이미 replay된 트랜잭션의 행은 visible**하다. 그러나 미래에 commit할 오래된 `now()` 값까지 안전하다는 뜻은 아니다. 이 방식이 안전하려면 `commit_time - transaction_start_time + clock anomaly`의 상한이 실제로 강제되고, `safety_lag`가 그 상한보다 커야 한다. 문서의 “전제”만으로는 보장이 아니다. PostgreSQL 17+의 `transaction_timeout`은 전체 transaction 시간을 제한할 수 있지만 prepared transaction은 대상이 아니므로, 2PC 금지/제한도 필요하다. [Client Connection Defaults — transaction_timeout](https://www.postgresql.org/docs/18/runtime-config-client.html#GUC-TRANSACTION-TIMEOUT)

복제 지연은 replay timestamp를 과거로 두어 보통 보수적으로 작용하지만, 유휴 시 `pg_last_xact_replay_timestamp()`는 새 commit/abort가 없으므로 정체한다. `pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn()`은 “받은 WAL까지 적용했다”는 뜻일 뿐, 그 순간 primary의 flush LSN까지 받았다는 보장도, 오래 열린 primary transaction이 없다는 보장도 아니다. 따라서 이 equality만으로 standby의 `now()`를 쓰는 보완은 안전하지 않다. standby 시계 오차와 아직 수신하지 않은 commit도 추가 누락 원인이다.

`hot_standby_feedback`은 primary VACUUM이 standby snapshot에 필요한 row version을 제거하는 것을 줄이는 기능이다. 미래 commit을 미리 보이게 하거나 transaction-start 시각을 commit 시각으로 바꾸지 않으므로 이 가시성 갭의 해결책이 아니다. 오히려 primary bloat를 늘릴 수 있다. [Hot Standby](https://www.postgresql.org/docs/18/hot-standby.html#HOT-STANDBY-CONFLICT)

**구체적 수정안**

우선순위대로 다음 중 하나를 채택한다.

1. **권장: commit 순서 기반 CDC/WAL 또는 소스 측 change log를 사용한다.** commit LSN/논리복제 순서를 cursor로 쓰면 transaction 시작시각 문제를 제거할 수 있다. 단순 sequence 값은 commit 전에 배정되므로 그 자체로는 해결책이 아니다.
2. polling을 유지한다면 writer role에 hard transaction lifetime을 강제하고, prepared transaction을 금지하며, `safety_lag > 강제 상한 + primary 시계 변동 여유`로 둔다. 버전이 17 미만이면 `transaction_timeout`이 없으므로 애플리케이션/프록시 수준 강제와 `pg_stat_activity.xact_start` 알람만으로는 장애 시 엄밀한 보장이 약하다.
3. 유휴 시에는 correctness를 위해 watermark 정체를 허용한다. 준실시간이 반드시 필요하면 source owner가 primary에서 주기적으로 commit되는 heartbeat를 만들고 그 commit이 standby에 replay된 뒤에만 전진하거나, primary의 특정 flush LSN을 캡처하여 standby `replay_lsn >= captured_lsn`을 확인한다. 현재의 receive=replay 단독 fallback은 제거한다.
4. 위 hard bound를 강제할 수 없다면 5번의 **무한 순환 full sweep** 또는 CDC 없이는 “누락 없음”을 주장하지 않는다.

### 3. 동일 `created_at` 대량 행의 청크 경계

**[OK] 근거(PG 문서/동작)**

경계를 timestamp 하나가 아니라 `(created_at, pk1, ..., pkn)` 전체 tuple로 잡고 `(from, to]`를 사용하므로, 같은 timestamp가 `chunk_size`보다 많아도 PK 순서로 여러 청크에 정확히 분할된다. outer query가 오름차순 첫 N행의 마지막 full tuple을 boundary로 고르는 구조도 맞다. row comparison의 사전식 동작이 이 결론의 근거다. [Row Constructor Comparison](https://www.postgresql.org/docs/18/functions-comparisons.html#ROW-WISE-COMPARISON)

다만 초기 HWM `{<시작시각>, '', ''}`은 “문자열 최소값”이 아니다. PK가 빈 문자열일 수 있거나 collation이 달라지면 시작시각과 같은 행을 제외할 수 있고, PK 컬럼 수/타입별 sentinel도 취약하다.

**구체적 수정안**

- `planned_hwm`을 nullable로 두어 최초 실행만 `created_at >= :start_ts`를 사용하거나, `has_hwm`/`initial_start_ts`를 분리한다.
- boundary를 항상 전체 tuple로 저장하고, source별 cursor 컬럼 수가 boundary 배열 길이와 일치하는지 CHECK/애플리케이션 검증한다.
- 테스트에 `created_at` 동일 행이 `3 * chunk_size` 이상인 경우, 빈 문자열 PK, Unicode/대소문자/정규화 차이, 청크 경계 직전·직후 값을 넣는다.

### 4. `text[]` 경계 왕복과 `timestamptz::text`

**[개선권고] 근거(PG 문서/동작)**

PostgreSQL timestamp/timestamptz의 저장 resolution은 1 microsecond이고, 기본 ISO 출력은 fractional seconds와 numeric UTC offset을 표현하므로 동일 서버 설정에서 `timestamptz -> text -> timestamptz`는 일반적으로 microsecond 정밀도를 보존한다. `timestamptz` 출력은 현재 `TimeZone`으로 변환되고, 출력/입력 형식은 `DateStyle`의 영향을 받는다. 설계는 `TimeZone=UTC`만 고정하여 `DateStyle`, 다른 cursor 타입의 text 출력, pool의 세션 초기화 누락을 명세하지 않았다. [Date/Time Types — resolution/input/output](https://www.postgresql.org/docs/18/datatype-datetime.html)

ISO 출력에 명시적 `+00` offset이 있으면 parse 시 세션 TimeZone에 의한 instant 변화는 없지만, SQL/Postgres/German DateStyle이나 `timestamp without time zone`은 해석 위험이 있다. `text[]`는 각 element의 원래 type/typmod/collation을 잃고, 앱 DB와 소스 DB의 default collation도 다를 수 있다.

**구체적 수정안**

1. 모든 물리 source connection 생성 시 아래 값을 고정하고 회귀 테스트한다.

```sql
SET TimeZone = 'UTC';
SET DateStyle = 'ISO, YMD';
```

2. 최소한 `created_at`은 `from_created_at timestamptz`, `to_created_at timestamptz`의 typed column으로 분리한다. PK 경계는 source별 typed coordination table을 생성하는 방법이 가장 안전하다. 범용 구조가 필수라면 `jsonb` 값과 함께 `cursor_spec`(type schema/name, typmod, collation, position)을 저장하고 허용 타입별 canonical codec을 둔다.
3. `infinity`, BC, 최대 6자리 fractional second, DST, non-ASCII varchar, embedded comma/quote/backslash를 round-trip 테스트한다. Java에서 PostgreSQL array literal 문자열을 직접 만들지 말고 JDBC `Array`를 사용한다.

### 5. 최근 10분 sweep의 충분성

**[결함] 근거(PG 문서/동작)**

5분마다 최근 10분만 재스캔하면 “late commit의 최대 지연 + planner/worker 장애시간 + replica 지연”이 10분 미만이라는 hard bound가 있을 때만 충분하다. 그런 bound가 없으면 11분 이상 열린 transaction의 `created_at` 행, 10분을 넘긴 장애 중 들어온 행, 오래 지연된 WAL은 sweep window 밖에 나타나 영구 누락된다. 이는 2번의 반례를 늦출 뿐 제거하지 않는다.

또한 현재 `UNIQUE (source_id, from_key)`는 동일 시작점의 recovery chunk를 막을 수 있고, “누락 row 발견 → 복구 청크 생성”의 정확한 from/to 산출, 원래 청크와의 중복 허용 방식, sweep 자체의 cursor/run 상태가 정의되지 않았다. 앱 멱등성은 중복을 안전하게 만들지만 누락을 발견해 주지는 않는다.

**구체적 수정안**

- hard transaction bound를 강제할 수 있으면 `sweep_window >= bound + 최대 운영중단 + 최대 replica lag + 여유`로 계산하고 config가 아니라 SLO/알람과 연결한다.
- hard bound가 없으면 source 전체를 반복 순환하는 `sweep_run`을 둔다. 각 cycle은 `-infinity`/초기시점에서 시작하여 끝까지 keyset scan 후 새 cycle로 reset한다. 어떤 row가 한 cycle의 cursor 뒤에서 late commit돼도 다음 full cycle이 발견한다. 데이터가 무한히 커져 비용이 감당되지 않으면 CDC가 필요하다.
- missing row는 일반 range와 충돌시키지 말고 `poll_replay_request(source_id, source_epoch, key, reason, status, ...)`에 exact key로 넣거나 `chunk_kind='RECOVERY'`, `run_id`를 uniqueness에 포함한다.
- `sweep_run(run_id, source_id, cycle_no, cursor, started_at, completed_at, rows_scanned, rows_missing, status, error)`를 추가하고 sweep 중단/재개가 가능해야 한다.

## B. 동시성

### 6. Planner가 앱 DB transaction/row lock을 잡고 replica를 조회하는 구조

**[결함] 근거(PG 문서/동작)**

`SELECT poll_source ... FOR UPDATE SKIP LOCKED`는 선택한 row에 transaction 종료까지 row lock을 건다. 그 transaction 안에서 별도 DataSource의 replica network I/O와 반복 scan을 수행하면 recovery conflict, network timeout, 느린 index scan 동안 앱 DB transaction과 pool connection, row lock을 계속 점유한다. `SKIP LOCKED`는 다른 planner를 빨리 포기시키지만 긴 transaction 자체를 짧게 만들지 않으며, row-level lock 외의 table-level lock도 없애지 않는다. PostgreSQL도 `SKIP LOCKED`를 일반 일관 조회가 아니라 queue-like table의 contention 회피 용도로 설명한다. [SELECT Locking Clause](https://www.postgresql.org/docs/18/sql-select.html#SQL-FOR-UPDATE-SHARE)

두 DataSource는 하나의 PostgreSQL local transaction이 아니므로 source read를 앱 DB commit과 원자화할 수도 없다. 원자성이 필요한 지점은 “예상 HWM이 아직 현재값인가 + chunk insert + HWM update”뿐이다.

**구체적 수정안**

`poll_source`에 planner lease/fencing을 두고 세 단계를 분리한다.

1. 짧은 앱 DB transaction으로 `planner_token++`, owner, lease_until을 claim하고 commit한다.
2. replica scan은 앱 DB transaction 밖에서 실행한다. 긴 loop면 planner heartbeat를 한다.
3. 짧은 앱 DB transaction에서 `source_id + planner_token + expected_planned_hwm`을 CAS 조건으로 확인하고, pending 수를 다시 확인한 뒤 chunk insert와 HWM update를 함께 commit한다. 0행이면 stale 결과를 버린다.

```sql
UPDATE poll_source
SET planner_token = planner_token + 1,
    planner_owner = :owner,
    planner_lease_until = statement_timestamp() + :lease
WHERE source_id = :sid
  AND enabled
  AND (planner_lease_until IS NULL
       OR planner_lease_until < statement_timestamp())
RETURNING planner_token, planned_hwm;

-- replica 조회 후 별도 짧은 TX
UPDATE poll_source
SET planned_hwm = :new_hwm,
    updated_at = statement_timestamp()
WHERE source_id = :sid
  AND planner_token = :token
  AND planned_hwm = :expected_hwm;
-- 같은 TX에서 chunk INSERT; UPDATE가 1행이 아니면 전체 rollback
```

### 7. claim query의 경합 및 EvalPlanQual

**[OK] 근거(PG 문서/동작)**

기본 `READ COMMITTED`에서 `UPDATE`, `SELECT FOR UPDATE`가 snapshot 이후 concurrent update를 만나면 최신 row version에 대해 search condition을 재평가한다(EvalPlanQual에 해당). subquery의 `FOR UPDATE SKIP LOCKED LIMIT 1`은 후보를 잠그고, 다른 worker는 잠긴 후보를 건너뛰므로 queue consumer들이 서로 다른 row를 잡는다. PostgreSQL 공식 `UPDATE` 문서도 제한된 batch update에 CTE + `FOR UPDATE`, contention 시 `SKIP LOCKED`를 예시한다. [Transaction Isolation — Read Committed](https://www.postgresql.org/docs/18/transaction-iso.html#XACT-READ-COMMITTED), [UPDATE](https://www.postgresql.org/docs/18/sql-update.html)

`ORDER BY` + locking의 out-of-order 경고는 ordering 컬럼이 concurrent update될 때 해당한다. 이 설계에서 `chunk_id`가 immutable이면 정확성 문제는 아니다. `SKIP LOCKED`가 제공하는 view는 의도적으로 불완전하므로 queue 이외 조회에는 쓰지 않아야 한다.

**구체적 수정안**

현재 scalar subquery도 원리상 맞지만, 공식 문서 패턴에 맞춘 CTE로 lock 대상과 update를 명확히 하고 outer UPDATE에도 방어 조건을 반복한다. claim transaction은 반드시 짧게 commit한다. 긴 transaction의 `now()`는 transaction 시작시각으로 고정되므로 lease 판단에는 `statement_timestamp()`를 권장한다.

```sql
WITH candidate AS MATERIALIZED (
  SELECT chunk_id
  FROM poll_chunk
  WHERE source_id = :sid
    AND attempts < :max_attempts
    AND (status = 'READY'
      OR (status = 'CLAIMED' AND lease_until < statement_timestamp()))
  ORDER BY chunk_id
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
UPDATE poll_chunk AS c
SET status = 'CLAIMED',
    lease_token = c.lease_token + 1,
    lease_owner = :owner,
    lease_until = statement_timestamp() + :lease,
    attempts = c.attempts + 1
FROM candidate AS x
WHERE c.chunk_id = x.chunk_id
  AND c.attempts < :max_attempts
  AND (c.status = 'READY'
    OR (c.status = 'CLAIMED' AND c.lease_until < statement_timestamp()))
RETURNING c.chunk_id, c.from_key, c.to_key, c.lease_token;
```

### 8. lease token fencing, 단일 completion transaction, heartbeat

**[개선권고] 근거(PG 문서/동작)**

핵심 fencing은 정확하다. stale worker가 멱등 row와 outbox를 INSERT한 뒤 `UPDATE poll_chunk ... WHERE lease_token=? AND status='CLAIMED'`에서 0행을 받고 **동일 transaction 전체를 rollback**하면 stale 결과는 남지 않는다. 새 worker와 같은 idempotency key를 동시에 INSERT해도 unique index conflict가 serialize하고, 최종적으로 유효 token을 가진 transaction만 완료할 수 있다.

다만 다음 조건이 반드시 필요하다.

- 멱등 INSERT, 그 `RETURNING` 기반 outbox INSERT, DONE update가 같은 앱 DB physical connection/transaction이어야 한다.
- 0행 예외를 catch하고 정상 return하거나 `noRollbackFor`로 처리해서는 안 된다.
- source read나 외부 side effect는 이 원자성에 포함되지 않는다. Kafka publish는 기존 transactional outbox relay만 담당해야 한다.

heartbeat는 safety보다 liveness/비용에 필요하다. 정상 p99 처리시간이 lease보다 길면 계속 reclaim되어 어느 worker도 DONE하지 못하는 livelock이 생길 수 있다.

**구체적 수정안**

- lease를 `max(p99 chunk 처리시간 * 3, recovery retry budget)` 이상으로 잡고, lease의 1/3 간격으로 별도 짧은 transaction heartbeat를 한다.
- 이미 만료된 lease를 뒤늦게 부활시키지 않도록 token/owner/status와 아직 유효함을 모두 검사한다.

```sql
UPDATE poll_chunk
SET lease_until = statement_timestamp() + :lease,
    last_heartbeat_at = statement_timestamp()
WHERE chunk_id = :chunk_id
  AND status = 'CLAIMED'
  AND lease_token = :token
  AND lease_owner = :owner
  AND lease_until > statement_timestamp()
RETURNING lease_until;
```

0행이면 즉시 source read/처리를 중단하고 completion transaction을 시작하지 않는다.

### 9. attempts 초과와 DEAD 전이

**[결함] 근거(PG 문서/동작)**

현재 claim predicate는 `attempts < :max`만 고른다. 마지막 허용 claim에서 `attempts`가 max가 된 뒤 worker가 실패/사망하면, row는 만료된 `CLAIMED, attempts=max`로 남고 다시 claim되지도 DEAD로 바뀌지도 않는다. `READY, attempts>=max`도 동일하다. 문서에 “attempts 초과 시 DEAD”라고 적혀 있지만 실행 SQL이 없다.

**구체적 수정안**

명시적 실패 처리와 별도 reaper를 모두 둔다. 모든 update는 token으로 fence한다.

```sql
-- worker가 오류를 인지한 경우
UPDATE poll_chunk
SET status = CASE WHEN attempts >= :max_attempts THEN 'DEAD' ELSE 'READY' END,
    lease_owner = NULL,
    lease_until = NULL,
    next_attempt_at = CASE WHEN attempts < :max_attempts
                           THEN statement_timestamp() + :backoff END,
    last_error_code = :code,
    last_error_message = left(:message, 4000),
    last_error_at = statement_timestamp(),
    dead_at = CASE WHEN attempts >= :max_attempts
                   THEN statement_timestamp() END
WHERE chunk_id = :chunk_id
  AND status = 'CLAIMED'
  AND lease_token = :token;

-- crash/kill을 처리하는 주기적 reaper (batch + SKIP LOCKED 권장)
UPDATE poll_chunk AS c
SET status = 'DEAD',
    lease_owner = NULL,
    lease_until = NULL,
    dead_at = statement_timestamp(),
    last_error_code = COALESCE(last_error_code, 'LEASE_EXHAUSTED')
FROM poll_source AS s
WHERE c.source_id = s.source_id
  AND c.attempts >= s.max_attempts
  AND (c.status = 'READY'
    OR (c.status = 'CLAIMED' AND c.lease_until < statement_timestamp()));
```

`next_attempt_at`을 claim predicate에 포함하고 DEAD 생성 즉시 알람한다. attempts가 “claim 횟수”인지 “실패 횟수”인지도 명시한다. 현재 SQL은 claim 횟수다.

## C. 성능/운영

### 10. tail mode, `poll_chunk` 증가, partial index, partitioning, VACUUM

**[결함] 근거(PG 문서/동작)**

300–500ms마다 `chunk_size` 미만이어도 즉시 chunk를 만들면 저유량 source는 행당 chunk에 가까워져 초당 수 개의 coordination row/transaction을 만든다. DONE update는 partial index의 predicate 컬럼 `status`를 바꾸므로 HOT update 조건을 만족하지 못하고 dead heap/index tuple을 만든다. PostgreSQL HOT은 index가 참조하는 컬럼을 바꾸지 않을 때만 가능하다. 고빈도 update/delete table은 낮은 autovacuum threshold/scale factor가 필요하다. [HOT](https://www.postgresql.org/docs/18/storage-hot.html), [Routine Vacuuming](https://www.postgresql.org/docs/18/routine-vacuuming.html)

현재 `CREATE TABLE`은 partitioned table이 아닌데 “일 단위 파티셔닝 + DROP”만 별도로 적혀 있다. 실제로 `PARTITION BY RANGE (created_at)`를 적용하면 partitioned table의 PK/UNIQUE는 partition key를 모두 포함해야 하므로 `PRIMARY KEY(chunk_id)`와 `UNIQUE(source_id, from_key)`는 그대로 생성할 수 없다. PostgreSQL은 partition 사이 global unique index를 제공하지 않는다. [Partitioning Limitations](https://www.postgresql.org/docs/18/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE-LIMITATIONS)

현재 partial index predicate는 query의 literal `status='READY' OR ...`가 `status IN ('READY','CLAIMED')`를 imply하므로 사용 가능하다. 그러나 partial index predicate를 parameter로 바꾸면 planner가 planning 시 implication을 증명하지 못해 사용할 수 없다. [Partial Indexes](https://www.postgresql.org/docs/18/indexes-partial.html)

**구체적 수정안**

1. partial chunk를 매 tick마다 봉인하지 말고 `row_count >= chunk_size OR oldest_unsealed_age >= max_chunk_delay`일 때 생성한다. 예: chunk_size=1000, max_chunk_delay=1s. 준실시간 상한은 max_chunk_delay로 관리한다.
2. 가장 단순한 구조는 작은 **active `poll_chunk`**와 range-partitioned **`poll_chunk_history`**를 분리하는 것이다. DONE/DEAD를 짧게 보관한 뒤 history로 archive하면 claim이 모든 과거 partition을 훑지 않고, active table에서 global uniqueness도 유지할 수 있다.
3. 현재 table을 직접 일 파티셔닝한다면 최소 PK를 `(created_at, chunk_id)`, unique를 `(created_at, source_id, from_key)`로 바꿔야 하지만 후자는 source/from의 global uniqueness를 보장하지 않는다. 별도 dedup table 또는 source epoch + planner CAS로 보완한다. drop 전 partition의 모든 chunk가 terminal이고 contiguous completion HWM보다 과거인지 확인한다.
4. 아래 같은 per-table autovacuum을 부하 시험으로 조정하고 `n_dead_tup`, last_autovacuum, index size를 감시한다.

```sql
ALTER TABLE poll_chunk SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 1000,
  autovacuum_analyze_scale_factor = 0.02,
  fillfactor = 80
);
```

5. READY와 만료 CLAIMED의 분포가 커지면 claim을 두 query로 나누고 다음 인덱스를 검토한다. 실제 선택은 `EXPLAIN (ANALYZE, BUFFERS)` 결과로 한다.

```sql
CREATE INDEX ix_chunk_ready
ON poll_chunk (source_id, chunk_id)
WHERE status = 'READY';

CREATE INDEX ix_chunk_expired
ON poll_chunk (source_id, lease_until, chunk_id)
WHERE status = 'CLAIMED';
```

### 11. standby recovery conflict 대응

**[개선권고] 근거(PG 문서/동작)**

페이지를 작게 하고 취소 시 재시도하는 방향은 맞지만 운영 정책이 부족하다. `max_standby_streaming_delay`는 개별 query 최대 실행시간이 아니라 받은 WAL을 apply할 때 허용하는 **누적 지연**이므로, 앞선 query가 예산을 사용하면 다음 짧은 query도 즉시 취소될 수 있다. `hot_standby_feedback`은 cleanup conflict를 줄이지만 primary bloat를 유발하고 모든 conflict(DDL lock, buffer pin 등)를 없애지 않는다. PostgreSQL은 원인별 취소 수를 `pg_stat_database_conflicts`에 제공한다. [Replication Settings](https://www.postgresql.org/docs/18/runtime-config-replication.html#GUC-MAX-STANDBY-STREAMING-DELAY), [Hot Standby Conflicts](https://www.postgresql.org/docs/18/hot-standby.html#HOT-STANDBY-CONFLICT), [Statistics Views](https://www.postgresql.org/docs/18/monitoring-stats.html#MONITORING-PG-STAT-DATABASE-CONFLICTS-VIEW)

**구체적 수정안**

- source query는 한 page마다 짧은 statement/snapshot으로 끝내고, 취소 시 마지막 **앱에서 확정한 page cursor가 아니라 현재 chunk의 in-memory after cursor**에서 동일 page를 재조회한다. 앱 DB 반영은 여전히 chunk completion transaction 한 번이다.
- exponential backoff + jitter, chunk별 recovery-conflict retry budget, 다른 replica로 failover, 장기 초과 시 lease heartbeat/안전한 포기를 정의한다.
- `statement_timeout`을 standby delay보다 작게 두되, 이것이 correctness 장치는 아님을 명시한다.
- `hot_standby_feedback=on`은 primary bloat/replication slot/WAL 보존 지표와 함께 승인한다.
- `pg_stat_database_conflicts`, replay/receive LSN 차이, `now()-pg_last_xact_replay_timestamp()`, query cancel rate를 알람한다.

### 12. `INSERT ... ON CONFLICT DO NOTHING RETURNING`과 신규분 outbox

**[OK] 근거(PG 문서/동작)**

PostgreSQL `INSERT ... RETURNING`은 실제로 insert된 row만 반환한다. `ON CONFLICT DO NOTHING`으로 skip된 기존 row는 반환되지 않으므로, 반환된 key/payload만 outbox에 넣는 방식은 “새 idempotency row당 outbox 1건”을 정확히 구현한다. concurrent insert가 있어도 unique arbiter가 둘 중 하나만 신규 insert로 만든다. [INSERT](https://www.postgresql.org/docs/18/sql-insert.html)

**구체적 수정안**

가능하면 한 SQL의 data-modifying CTE로 관계를 분명히 한다. payload가 크거나 batch source를 join해야 하면 `RETURNING`에 outbox에 필요한 값 또는 stable batch ordinal을 포함한다.

```sql
WITH inserted AS (
  INSERT INTO poll_idempotency (source_id, source_epoch, source_key, payload)
  SELECT source_id, source_epoch, source_key, payload
  FROM unnest_or_staging_input
  ON CONFLICT (source_id, source_epoch, source_key) DO NOTHING
  RETURNING source_id, source_epoch, source_key, payload
)
INSERT INTO outbox (event_id, aggregate_key, payload, created_at)
SELECT gen_random_uuid(), source_key, payload, statement_timestamp()
FROM inserted;
```

- idempotency unique constraint는 반드시 canonical source key 전체를 보장해야 한다. app에서 JSON 문자열을 만들면 배열 순서, 문자열 escaping, Unicode 표현을 canonical하게 고정하거나 `jsonb`/구조화 컬럼을 사용한다.
- 과거 버그로 idempotency row만 있고 outbox가 없는 상태는 이 패턴이 자동 복구하지 못하므로 별도 reconciliation을 둔다.
- 기존 row를 반환받기 위해 no-op `DO UPDATE`를 쓰지 않는다. 불필요한 lock/update/bloat가 생긴다.

### 13. MyBatis 동적 SQL, 식별자, `text[]`, `ArrayTypeHandler`

**[개선권고, 일부 불확실] 근거(PG/MyBatis/pgJDBC 문서/동작)**

**불확실:** MyBatis와 pgJDBC 버전이 명시되지 않았다. 아래 `ArrayTypeHandler` 내용은 현재 MyBatis 3.5.x(공식 xref 3.5.19 계열) 기준이며, `affectData`는 MyBatis 3.5.12+ 기능이다.

MyBatis `${}`는 문자열을 escape하지 않고 SQL에 그대로 삽입한다. 기동 시 regex만 검사하면 reserved word, 대소문자 quoted identifier, schema-qualified name, collation/type 이름을 안전하고 정확하게 표현하지 못한다. user input을 그대로 공급하면 SQL injection이라고 공식 문서도 경고한다. `#{}`는 prepared parameter이므로 값에만 사용해야 한다. [MyBatis String Substitution](https://mybatis.org/mybatis-3/sqlmap-xml.html#String_Substitution)

현재 MyBatis `ArrayTypeHandler`는 Java `String[]`을 `Connection.createArrayOf("VARCHAR", ...)`로 만들고, `List<String>`은 지원하지 않는다. PostgreSQL `text[]`를 확실히 bind하려면 `createArrayOf("text", String[])`을 쓰는 custom handler가 낫다. pgJDBC는 `Connection.createArrayOf` 후 `PreparedStatement.setArray` 방식을 공식 지원한다. [MyBatis ArrayTypeHandler source](https://mybatis.org/mybatis-3/xref/org/apache/ibatis/type/ArrayTypeHandler.html), [pgJDBC Arrays](https://jdbc.postgresql.org/documentation/server-prepare/#arrays)

PostgreSQL `RETURNING`처럼 DML이 result set을 내는 mapper는 MyBatis에서 `<select affectData="true" flushCache="true">`로 정의해야 한다(3.5.12+). 단순 `<insert>` mapping으로 반환 row 목록을 받는다고 가정하면 버전/매핑에 따라 실패한다. [MyBatis Mapper XML — DML returning data](https://mybatis.org/mybatis-3/sqlmap-xml.html)

**구체적 수정안**

1. YAML의 schema/table/column/type/collation은 자유 문자열이 아니라 source별 immutable registry에서 선택한다. 시작 시 `pg_catalog`에서 실제 OID/type/collation/index를 조회해 일치 여부를 검증하고, identifier 각 segment를 `"` doubling 규칙으로 quote한다. `${}`에 들어가는 값은 이 registry가 생성한 최종 fragment만 허용한다.
2. `${type}`은 `varchar(64)`, domain, array, schema type 등 문법 표면이 넓으므로 regex로 받지 않는다. 허용한 PostgreSQL type OID/정규화된 type name을 enum/template로 렌더링한다. 값 cast는 `CAST(#{v} AS <검증된 타입>)`을 권장한다.
3. custom `TextArrayTypeHandler`에서 `String[]`만 받고 `createArrayOf("text", value)`/`setArray`/`free`를 명시한다. null array와 null element 정책, result `java.sql.Array#getArray()` 타입을 통합 테스트한다. Java 배열 literal 문자열을 직접 조립하지 않는다.
4. source/app `SqlSessionFactory`, transaction manager, mapper package를 분리하고 completion service에 단일 app transaction manager를 명시한다. source mapper는 write 권한이 없는 DB role로 연결한다.
5. 동일한 SQL fragment가 index의 explicit `COLLATE`, column order, cast type을 항상 보존하는 snapshot test와 실제 PostgreSQL integration test를 둔다.

## D. 누락된 구성요소

### 14. 상태 전이 다이어그램과 전이 규칙

**[결함] 근거(PG 문서/동작 및 설계 영향)**

현재 READY/CLAIMED/DONE/DEAD 이름만 있고, timeout, 명시적 실패, heartbeat 실패, 운영자 재처리, source disable 시 전이가 없다. 9번의 영구 CLAIMED 같은 결함이 이 누락에서 발생한다.

**구체적 수정안**

```text
                 claim(token++, attempts++)
        +----------------------------------------+
        |                                        v
     READY <--- fenced retryable failure ----- CLAIMED
       |                                        |  |
       | attempts >= max                        |  +-- heartbeat(token 동일)
       v                                        |      -> CLAIMED
      DEAD <--- expired + attempts >= max ------+
       ^                                        |
       | fenced fatal failure                   | fenced completion TX
       +----------------------------------------+------> DONE

expired CLAIMED + attempts < max --reclaim(token++, attempts++)--> CLAIMED
DEAD --operator replay(new replay/run id; 원 row 불변)------------> 새 READY
sweep missing ----------------------------------------------------> RECOVERY READY
```

각 화살표에 실행 주체, SQL predicate, token 변화, transaction 경계, metric/event를 표로 명세한다. DONE/DEAD는 immutable terminal로 두고 재처리는 기존 row status를 되돌리지 말고 새 `replay_run`/`chunk_kind=RECOVERY`로 만든다.

### 15. 추가 테이블·컬럼·제약

**[결함] 근거(PG 문서/동작 및 설계 영향)**

현재 DDL의 `PK`, `FK`는 PostgreSQL DDL 문법이 아니라 축약 표기다. 그대로 실행할 수 없고, 대부분 컬럼에 `NOT NULL`/CHECK가 없어 `status=NULL`, `attempts=NULL`, CLAIMED인데 lease가 없는 상태가 가능하다. source config 변경/HWM reset 중 이전 worker를 분리할 epoch도 없다. 일 partitioning과 현재 PK/UNIQUE의 충돌은 10번과 같다.

**구체적 수정안**

최소 추가/수정 항목은 다음과 같다.

- `poll_source`: `source_epoch`, `max_attempts`, `cursor_spec jsonb`, `config_hash`, `initial_start_ts`, `completed_hwm`, planner owner/token/lease, sweep policy, `paused_reason`.
- `poll_chunk`: `source_epoch`, `chunk_kind`, `next_attempt_at`, `claimed_at`, `last_heartbeat_at`, `last_error_code/message/at`, `dead_at`, `config_hash`; 모든 counter/status/token에 `NOT NULL`.
- `poll_attempt`: attempt별 owner/token/start/end/outcome/error/row count/duration을 append-only 기록.
- `sweep_run`, `poll_replay_request`: 5번과 17번의 복구/감사 상태.

예시 제약은 다음과 같다.

```sql
ALTER TABLE poll_source
  ADD COLUMN source_epoch bigint NOT NULL DEFAULT 1,
  ADD COLUMN max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
  ADD COLUMN planner_token bigint NOT NULL DEFAULT 0,
  ADD COLUMN planner_owner varchar(128),
  ADD COLUMN planner_lease_until timestamptz,
  ADD COLUMN cursor_spec jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN config_hash text NOT NULL DEFAULT '';

ALTER TABLE poll_chunk
  ADD COLUMN source_epoch bigint NOT NULL DEFAULT 1,
  ADD COLUMN chunk_kind varchar(16) NOT NULL DEFAULT 'NORMAL',
  ADD COLUMN next_attempt_at timestamptz,
  ADD COLUMN claimed_at timestamptz,
  ADD COLUMN last_heartbeat_at timestamptz,
  ADD COLUMN last_error_code text,
  ADD COLUMN last_error_message text,
  ADD COLUMN last_error_at timestamptz,
  ADD COLUMN dead_at timestamptz,
  ALTER COLUMN status SET NOT NULL,
  ALTER COLUMN lease_token SET NOT NULL,
  ALTER COLUMN attempts SET NOT NULL,
  ADD CONSTRAINT ck_chunk_status
    CHECK (status IN ('READY','CLAIMED','DONE','DEAD')),
  ADD CONSTRAINT ck_chunk_lease
    CHECK ((status = 'CLAIMED') =
           (lease_owner IS NOT NULL AND lease_until IS NOT NULL)),
  ADD CONSTRAINT ck_chunk_bounds
    CHECK (cardinality(from_key) = cardinality(to_key));
```

실제 신규 DDL에서는 `source_id varchar(64) PRIMARY KEY`, `chunk_id bigserial PRIMARY KEY`, `source_id ... REFERENCES poll_source(source_id)`로 풀어 쓴다. `completed_hwm`은 DONE인 최고 키가 아니라 **앞에서부터 빈틈 없이 DONE인 최고 경계**여야 retention과 lag 판단에 쓸 수 있다.

DONE/DEAD/READY로 전이할 때는 위 `ck_chunk_lease`를 만족하도록 `lease_owner`와 `lease_until`을 함께 null로 지운다.

### 16. 모니터링 지표와 알람

**[결함] 근거(PG 문서/동작 및 설계 영향)**

현재 `sweep_recovered_rows` 하나만으로는 backlog, 영구 CLAIMED, lease thrash, HWM 정체, standby conflict, vacuum 악화를 구분할 수 없다.

**구체적 수정안**

source별 최소 지표/알람을 정의한다.

| 영역 | 지표 예시 | 필수 알람 |
|---|---|---|
| 진척 | planned HWM age, contiguous completed HWM age, safe_ts age, source 최신 eligible key와 HWM 차이 | HWM 정체, 준실시간 SLO 초과 |
| 큐 | READY/CLAIMED/expired/DEAD 수, oldest READY age, pending/max_pending | expired > 0 지속, DEAD > 0, backlog 상한 |
| 리스 | claim/reclaim 수, token 증가율, heartbeat 실패, 처리시간 p50/p95/p99 ÷ lease | reclaim 폭증, p99가 lease 접근 |
| 데이터 | source rows read, idempotency inserted/conflicted, outbox inserted, chunk row_count, empty chunk | read와 inserted+conflict 불일치 |
| sweep | cycle duration/progress, scanned, missing, recovered, oldest covered time | cycle 미완료, recovered > 0 |
| standby | receive/replay LSN gap, last replay timestamp age, query cancellation, `pg_stat_database_conflicts` 원인별 delta | lag/conflict budget 초과 |
| 앱 DB | completion TX duration/rollback, lock wait, pool active/wait, `n_dead_tup`, autovacuum, index/table bytes | 긴 TX, vacuum 지연/bloat |
| outbox | unpublished age/count, relay failure | relay SLO 초과 |

로그/trace에는 `source_id, source_epoch, chunk_id, lease_token, owner, attempt, from/to hash, SQLSTATE`를 공통 correlation field로 넣되 민감한 PK/payload 전체는 노출하지 않는다.

### 17. 운영 절차(재처리, HWM reset, source 추가/변경)

**[결함] 근거(PG 문서/동작 및 설계 영향)**

현재 enabled flag만으로는 planner만 멈추는지 worker도 멈추는지 불명확하고, HWM reset 중 이전 lease worker가 commit하는 것을 source epoch로 막지 못한다. 기존 idempotency key는 단순 replay 시 outbox를 새로 만들지 않으므로 “재처리”가 검증 재읽기인지 이벤트 재발행인지도 구분해야 한다.

**구체적 수정안**

다음 runbook을 문서화하고 자동화한다.

1. **source 추가:** catalog/type/collation/NOT NULL/PK 검증 → primary에 cursor index `CONCURRENTLY` 생성 → production-like `EXPLAIN` 검증 → start mode(처음부터/시각부터) 선택 → bootstrap sweep → 저율 enable → SLO 확인.
2. **soft pause:** planner만 중지하고 기존 chunk drain. **hard pause:** planner 중지 + 신규 claim 중지, 현재 worker heartbeat/완료 대기 후 필요 시 lease 만료. 두 의미를 별도 상태로 둔다.
3. **HWM reset/config 변경:** hard pause → nonterminal chunk 정리/terminal 보존 → `source_epoch++` 및 새 config hash/HWM 설정을 한 TX에서 commit → 새 epoch 시작. completion은 chunk의 source_epoch/config_hash도 확인한다. cursor 컬럼/정렬/collation 변경은 같은 epoch에서 금지한다.
4. **재처리:** 기존 DONE을 READY로 되돌리지 않는다. `replay_run`에 range/exact keys, 이유, 요청자, 승인, event policy를 기록한다. 검증 replay는 기존 idempotency로 suppress하고, 이벤트 재발행은 별도 replay generation/event id와 소비자 계약을 사용한다.
5. **DEAD 처리:** 원인 수정 → 운영자 승인 → 새 recovery chunk 생성. 원 DEAD row와 attempt history는 보존한다.
6. **failover/upgrade:** 새 replica가 `pg_is_in_recovery()`인지, server major, timeline/lag, collation version, index 정의를 확인한다. collation ordering이 바뀌면 full validation sweep 없이 기존 HWM을 신뢰하지 않는다.
7. **앱 DB restore/소스 장기 outage:** source와 idempotency/outbox/HWM의 시점 차이를 계산하고 full sweep 또는 새 epoch bootstrap 후 resume한다.

## 우선순위별 변경 요약

| 우선순위 | 관련 항목 | 반드시 할 변경 | 미수정 시 영향 |
|---|---:|---|---|
| P0 | 2 | `created_at=now()`와 replay commit 시각의 차이를 인정하고, CDC/commit-order cursor 또는 강제된 max transaction lifetime + 충분한 lag를 채택; receive=replay 기반 `now()` fallback 제거 | late commit 영구 누락 |
| P0 | 5 | 10분 sweep을 hard bound로 산정하거나 무한 순환 full sweep/CDC로 교체; recovery 전용 상태 추가 | window 밖 누락 영구화 |
| P0 | 9 | 명시적 실패 전이와 expired exhausted reaper 구현 | `CLAIMED attempts=max` 영구 정체 |
| P0 | 8, 12 | idempotency + 신규분 outbox + fenced DONE을 동일 앱 DB TX로 강제하고 0행을 rollback | stale worker 결과 또는 outbox 불일치 |
| P0 | 15, 17 | source epoch/config hash와 reset/replay 절차 도입 | reset/설정 변경 중 구·신 worker 혼합 |
| P1 | 1, 3, 4 | NOT NULL/total order/collation 고정, 전용 복합 index, sentinel 없는 초기 HWM, typed/canonical boundary | 특정 키 누락, index 미사용, upgrade 시 순서 변경 |
| P1 | 6 | planner DB row lock을 replica I/O와 분리하고 planner lease + CAS finalize 사용 | 긴 앱 DB TX, pool/lock 정체 |
| P1 | 8 | worker/planner heartbeat와 lease SLO 정의 | 반복 reclaim/livelock, 중복 source read |
| P1 | 10 | tail coalescing, active/history 분리 또는 제약을 반영한 partition DDL, autovacuum tuning | coordination 폭증, bloat, 실행 불가한 partition 계획 |
| P1 | 11, 16 | recovery-conflict retry budget/failover 및 전체 지표·알람 구현 | 장애 원인 미분류, lag/취소 폭증 |
| P1 | 13 | identifier/type registry, explicit quoting, custom `text[]` handler, MyBatis version별 RETURNING mapping | SQL injection/identifier 오류/array binding 실패 |
| P2 | 7 | claim을 공식 CTE 패턴으로 정리하고 outer 방어 조건/statement timestamp 사용 | 유지보수 시 경합 조건 회귀 |
| P2 | 14 | 상태 전이와 주체/SQL/token/metric 표 문서화 | 예외 경로 재누락 및 운영 혼선 |

## 최종 결론

범위 청크, `SKIP LOCKED` claim, lease token fencing, 멱등 INSERT의 `RETURNING`만 outbox로 보내는 큰 방향은 타당하다. 그러나 현재 v2는 `created_at=now()`의 transaction-start 의미를 commit/replay watermark와 혼동하고 있고, 유한 sweep 및 DEAD 전이 누락 때문에 **누락 없음/자동 복구를 보장하지 못한다**. P0 항목을 반영하기 전에는 “at-least-once read + exactly-once-effect에 가까운 앱 DB 반영”이라고도 운영적으로 확정하기 어렵고, P0/P1 반영 후 부하·장애·late-commit 테스트로 보증 범위를 명시해야 한다.
