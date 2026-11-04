# Read-Only 소스 테이블 병렬 폴링 설계 v2 (검토 대상)

## 0. 전제
- 소스 테이블: **append-only**, **읽기 복제본(PostgreSQL 물리 복제본)** 에 있음. 쓰기/DDL/행잠금 불가. 인덱스는 소스 소유팀에 primary 추가 요청만 가능.
- 앱 DB(애플리케이션 소유, PostgreSQL): 멱등 테이블 + 아웃박스(이미 구현) + 이번에 추가할 조율 테이블. 아웃박스 → Kafka 릴레이는 이미 구현.
- 소스 테이블 N개로 확장. 각 테이블 PK 구조가 다름, 대부분 **varchar 복합키**.
- 데이터 실시간 증가, **준실시간**(수 초) 처리 필요.
- 스택: Spring Boot, MyBatis, PostgreSQL, EKS 다중 pod.

## 1. 핵심 아이디어: 범위 청크 + 리스
- 커서 키 = `(created_at, pk1, ..., pkn)` 전부 ASC, 전체 유일. (varchar PK 단독은 삽입 순서와 무관하여 next-key 커서로 신규 행을 놓침)
- Planner가 커서 키 공간을 겹치지 않는 `(from_key, to_key]` 청크로 잘라 앱 DB `poll_chunk`에 등록.
- Worker가 `FOR UPDATE SKIP LOCKED`로 청크를 claim(리스: owner, lease_until, lease_token++), 복제본에서 범위 조회, 앱 DB 단일 TX로 [멱등 INSERT ON CONFLICT DO NOTHING + 신규분만 아웃박스 INSERT + chunk DONE(lease_token 일치 조건)] 커밋. DONE update가 0행이면 예외→롤백(fencing).
- 리스 만료 청크는 다른 워커가 재claim(attempts++), attempts 초과 시 DEAD + 알람.

## 2. 조율 테이블 (현재안)
```sql
CREATE TABLE poll_source (
  source_id varchar(64) PK, planned_hwm text[] NOT NULL,
  chunk_size int DEFAULT 1000, max_pending int DEFAULT 64, safety_lag_sec int DEFAULT 5,
  enabled boolean DEFAULT true, updated_at timestamptz DEFAULT now());

CREATE TABLE poll_chunk (
  chunk_id bigserial PK, source_id varchar(64) FK,
  from_key text[] NOT NULL /*exclusive*/, to_key text[] NOT NULL /*inclusive*/,
  status varchar(16) DEFAULT 'READY' /*READY|CLAIMED|DONE|DEAD*/,
  lease_token bigint DEFAULT 0, lease_owner varchar(128), lease_until timestamptz,
  attempts int DEFAULT 0, row_count int, created_at timestamptz DEFAULT now(), done_at timestamptz,
  UNIQUE (source_id, from_key));
CREATE INDEX ix_chunk_claimable ON poll_chunk (source_id, chunk_id) WHERE status IN ('READY','CLAIMED');
```
- 경계값은 복제본에서 `col::text`로 받아 text[]로 저장, 비교 시 SQL 안에서 `#{v}::${type}` 캐스팅. 앱은 경계값을 비교/변환하지 않음. 소스 커넥션 세션 TimeZone=UTC 고정.
- 소스 정의는 YAML: `cursor-columns: [{name, type}]`(인덱스 순서), `select-columns`, `chunk-size`, `safety-lag`. `${}` 식별자는 기동 시 정규식/허용목록 검증.

## 3. Planner (모든 pod에서 실행, 소스별 1개만 유효)
앱 DB TX:
1. `SELECT ... FROM poll_source WHERE source_id=? AND enabled FOR UPDATE SKIP LOCKED` (못 잡으면 종료)
2. 미완료(READY/CLAIMED) 청크 수 ≥ max_pending 이면 종료(백프레셔)
3. (복제본) `safe_ts = pg_last_xact_replay_timestamp() - safety_lag`
4. (복제본) 경계 조회:
```sql
SELECT ARRAY[c1::text, c2::text, c3::text] FROM (
  SELECT c1,c2,c3 FROM src WHERE (c1,c2,c3) > (:k1::timestamptz, :k2::varchar, :k3::varchar)
    AND c1 < :safe_ts ORDER BY c1,c2,c3 LIMIT :chunk_size) t
ORDER BY c1 DESC, c2 DESC, c3 DESC LIMIT 1;
```
5. 결과 있으면 `INSERT poll_chunk(from=planned_hwm, to=boundary)`, `UPDATE poll_source SET planned_hwm=boundary`. max_pending/safe_ts 도달까지 반복.
- 주기: 꼬리 모드 300~500ms, 행 수가 chunk_size 미만이어도 청크 생성.
- 초기 HWM: `{<시작시각>, '', ''}`.

## 4. Worker
- claim:
```sql
UPDATE poll_chunk c SET status='CLAIMED', lease_token=c.lease_token+1, lease_owner=:owner,
  lease_until=now()+make_interval(secs=>:leaseSec), attempts=c.attempts+1
WHERE c.chunk_id=(SELECT chunk_id FROM poll_chunk WHERE source_id=:sid
   AND (status='READY' OR (status='CLAIMED' AND lease_until<now())) AND attempts<:max
   ORDER BY chunk_id LIMIT 1 FOR UPDATE SKIP LOCKED)
RETURNING chunk_id, from_key, to_key, lease_token;
```
- 조회(복제본, 페이지 keyset): `WHERE (cols) > (:after) AND (cols) <= (:to) ORDER BY cols LIMIT :page`, fetchSize 500.
- 완료(앱 DB 단일 TX): 멱등 INSERT ... ON CONFLICT DO NOTHING RETURNING → 신규분만 아웃박스 → `UPDATE poll_chunk SET status='DONE' ... WHERE chunk_id=? AND lease_token=? AND status='CLAIMED'` (0행 → 예외 롤백).
- 멱등 키: source_id + PK 값들의 JSON 배열 인코딩.
- 빈 claim 시 100ms→1s 백오프. 종료 시 graceful shutdown.

## 5. 가시성 갭 대응
- safety lag: 복제본 `pg_last_xact_replay_timestamp() - lag` 미만만 청크화. 전제: primary 쓰기 TX 최대 지속시간 + 시계오차 < lag. created_at은 DB now() 권장.
- 복제본 유휴 시 replay_ts 정체 → `pg_last_wal_receive_lsn()=pg_last_wal_replay_lsn()`이면 now() 기준 보완.
- 재스캔 sweep: 5분마다 최근 10분 키만 복제본에서 읽어 앱에서 멱등 테이블과 대조(ANY 배치), 누락 시 복구 청크 생성, `sweep_recovered_rows` 지표(평소 0) 알람.

## 6. 기타
- 복제본 쿼리 취소(recovery conflict) 재시도, 페이지 작게.
- DataSource/SqlSessionFactory 소스용/앱용 분리.
- poll_chunk 일 단위 파티셔닝 + DROP.
- Aurora면 replay_timestamp 대신 aurora_replica_status().
