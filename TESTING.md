# Hướng dẫn test

Test được đặt riêng trong `tests/`; không đặt fixture hoặc fake driver vào production package.

## Ánh xạ test theo component

| Production component | Test tương ứng |
|---|---|
| `browser/session_manager.py` | `test_browser_lifecycle.py`, `test_browser_session.py` |
| `navigator.py`, `health.py` | `test_navigation_health.py` |
| `review_surface.py` | `test_review_surface.py`, `test_review_pane_handoff.py` |
| `paginator.py` | `test_paginator.py` |
| `extractor.py` | `test_extractor.py` |
| `crawler.py`, status mapping | `test_architecture.py`, `test_observability.py` |
| `crawlee_adapter.py`, retry | `test_browser_lifecycle.py`, `test_architecture.py` |
| Full-crawl artifacts/summary | `test_full_crawl_runner.py` |
| Smoke runner logic | `test_smoke_test_runner.py` |
| Access diagnostics | `test_access_benchmark.py`, `test_cooldown_probe.py` |

## Quy tắc viết test

- Unit test dùng fake driver/element và không truy cập Google Maps thật.
- Test navigation tách rõ `SEARCH_RESULTS`, `SEARCH_PREVIEW` và `PLACE_ENTITY`.
- Test review access chỉ bắt đầu sau khi fixture đã xác nhận place entity.
- Test selector/DOM đặt cạnh test của component sở hữu selector đó.
- Test repository dùng database tạm hoặc `:memory:`.
- Test runner dùng `tmp_path`; không ghi vào `data/` thật.
- Mỗi bug DOM nên có một regression test nhỏ tái tạo đúng evidence gây lỗi.
- Test không phụ thuộc thứ tự chạy, browser thật, network, thời gian thực hoặc dữ liệu từ run trước.

## Mức test

1. **Unit:** một component với fake dependency; đây là lớp test mặc định.
2. **Component flow:** ghép navigator/surface/crawler bằng fake driver để kiểm tra thứ tự và status semantics.
3. **Runner artifact:** kiểm tra metadata, checkpoint, DLQ và summary trong thư mục tạm.
4. **Live validation:** chỉ chạy thủ công qua script có timeout và artifact riêng; không đưa vào suite mặc định.

Khi sửa code, chạy test tập trung của component trước rồi mới chạy toàn bộ suite. Không ghi số lượng test đã pass hoặc run ID vào tài liệu vì các giá trị đó thay đổi theo thời gian.
