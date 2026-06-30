# CLAUDE.md — quantus-price-collector (Quantus 가격 수집기)

> 모노레포 루트 규율 전파본(신규). 권위 원본: quantus-mono 루트 CLAUDE.md.

---

## 🔒 엔지니어링 규율 (모노레포 루트 전파 · 협상 불가)

> 권위 원본: quantus-mono 루트 `CLAUDE.md` + DevKit 스펙 `docs/superpowers/specs/2026-06-17-quantus-devkit-adoption.md`(재발방지 §6).

**Iron Laws** — ① **근본원인 먼저**(`systematic-debugging`): 증상서 결론 금지, NO FIX WITHOUT ROOT CAUSE — 전체 체인/설정 추적 후 결론. ② **검증증거 없이 주장 금지**(`verification-before-completion`): "안전/취약/고침/됨/통과" 주장 전 이 메시지에서 검증 실행·증거 인용. 코드 읽기 ≠ 검증, 라이브 ground-truth 우선.

**하드와이어 휴리스틱** — 결론 ≥3번 뒤집히면 STOP · 보안/상태 단정 전 라이브 프로브 · 설정/배포 전 **배포 진실 + canonical/배포 브랜치 확정**(작업 베이스 포함) · **코드(호출자·구조·로직) vs 라이브(배포상태)** 분담 · **비밀값 출력 금지**(비번·키·시크릿·토큰을 메시지·로그·커밋·알림·UI에 *값으로* 금지 → 이름/위치로만; 사용자-노출 UI엔 게이팅 규칙·도메인·조직명도 익명 비노출) · **재사용·최단경로 먼저**(새 인프라/스택/의존성 전 기존 grep·재사용, "더 단순한 길?" 먼저, 새 의존성 명시 승인) · 고위험(보안/실거래/삭제/권한) 변경엔 독립 리뷰.

**안전선** — 🚨 signal `ALLOW_REAL_ORDERS` 활성화 절대 금지(실거래/실머니 — CEO 승인+코드리뷰 필수) · 자동배포 금지 · 공유 브랜치(main/master/dev/production) 직접커밋 금지(전용 브랜치+PR) · 파괴적 `rm -rf` 금지.

> 규율은 "보유"가 아니라 "발동"이 핵심. 조사·주장 타입 작업이면 위 두 스킬을 *먼저* 발동하라.
