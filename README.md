# architecture

디텍터(rdt_db 소스 폴링 → prm_db 멱등·아웃박스 적재) 설계 문서 모음.

## 문서

### 웹 문서 (GitHub Pages)

| 문서 | 내용 |
|---|---|
| [프로모션 디텍터 설계 v2](https://pilsa.github.io/architecture/promotion-detector-design.html) | detector·provider 역할 분리, 처리 흐름(설정→행 평가→가입 조회), 멱등키 규칙, 테이블 모델링 |
| [범위 청크 + 리스 병렬 폴링 구현 가이드](https://pilsa.github.io/architecture/polling-implementation-guide.html) | 작동 원리, 테이블 설계, SQL, Spring·MyBatis 구현, 운영·테스트 |
| [범위 청크 + 리스 쉬운 설명](https://pilsa.github.io/architecture/chunk-lease-explained.html) | 청크 분할과 리스 동작 원리 입문 |

### 설계 원문 (Markdown)

| 문서 | 내용 |
|---|---|
| [promotion-detector-design-v2.md](docs/design/promotion-detector-design-v2.md) | 프로모션 디텍터 설계 v2 원문 (최신) |
| [promotion-detector-design.md](docs/design/promotion-detector-design.md) | 프로모션 디텍터 설계 v1 (v2로 대체됨) |
| [polling-design-v2.md](docs/design/polling-design-v2.md) | 범위 청크 + 리스 병렬 폴링 설계 (검토 대상) |
| [review-codex-r1.md](docs/design/review-codex-r1.md) | 폴링 설계 검토 1차 |
| [review-codex-r2.md](docs/design/review-codex-r2.md) | 폴링 설계 검토 2차 |

`docs/` 폴더는 GitHub Pages로 게시됩니다. `master`에 push하면 1분 안팎으로 반영됩니다.
