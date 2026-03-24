# Deep Research 기반 TODO (Inventory 전환)

보고서(`deep-research-report.md`)에서 제안한 항목을 코드 기준으로 분해한 실행 TODO.

## P0 (핵심)

- [x] Inventory 원장 집계 뼈대 (`on_hand/allocated/available`)
  - `src/inventory/ledger.py`
- [x] ROP + 안전재고 + 권장 발주 수량 계산
  - `src/inventory/replenishment.py`
- [x] Inventory 리포트 파이프라인
  - `src/pipelines/inventory_report.py`
- [x] Internal vs Channel 재고 정합(Reconcile) 리포트
  - `src/inventory/reconcile.py`
  - `inventory_report`에 mismatch 요약 포함
- [x] CLI 진입점 추가
  - `python src/main.py --inventory-report`
- [x] 텔레그램 메뉴/명령 연결
  - `/inventory_report`
  - `Inventory Report (Beta)` 버튼
- [x] Inventory daily scheduler 추가(옵션)
  - `INVENTORY_MODE_ENABLED`
  - `INVENTORY_REPORT_TIME`

## P1 (다음 반영 권장)

- [ ] 채널 커넥터 1개 end-to-end (Shopify/스마트스토어/카페24 중 택1)
- [ ] 주문 동기화 → `allocated` 자동 반영
- [ ] 발주 승인 워크플로(텔레그램 approve/hold)
- [ ] 재고 조정 권한/감사로그 강화
- [ ] 재고 스냅샷 백업 + push kill-switch

## P2 (확장)

- [ ] 웹 대시보드(조회/검색/필터/CSV)
- [ ] 멀티채널 커넥터 확장
- [ ] 고급 리포트(품절률/회전율/리드타임 KPI)

