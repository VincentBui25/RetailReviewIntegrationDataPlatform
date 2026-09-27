# Retail Review Integration Data Platform

Crawler thu thập review cửa hàng từ Google Maps. SeleniumBase/Crawlee phụ trách phiên duyệt; SQLite giữ trạng thái crawler và idempotency, còn Parquet là payload review chuẩn hóa cho analytics.

Crawler chỉ sử dụng giao diện guest hợp lệ: không giải CAPTCHA, không tự động đăng nhập, không nhập cookie và không xoay proxy.

Đọc thêm:

- [ARCHITECTURE.md](ARCHITECTURE.md): luồng xử lý, trách nhiệm từng component và ranh giới mở rộng.
- [TESTING.md](TESTING.md): cách tổ chức và viết test.

## Cài đặt

Yêu cầu Python 3.11+, Google Chrome/Chromium và môi trường có thể mở browser.

```bash
python -m venv .venv
python -m pip install -r requirements.txt
```

Dependencies trực tiếp:

- `crawlee>=1.10,<2`
- `seleniumbase>=4,<5`
- `pyarrow>=17`

## Chạy crawler chuẩn

```bash
python -m crawl_experiment
```

Tương đương:

```bash
python -m crawl_experiment \
  --manifest config/known_coles_stores.json \
  --database data/state/crawler_state.sqlite3 \
  --parquet-root data/lake/google_maps_reviews \
  --checkpoints data/checkpoints \
  --concurrency 1
```

| Tham số | Mặc định | Ý nghĩa |
|---|---|---|
| `--manifest` | `config/known_coles_stores.json` | Danh sách cửa hàng đầu vào |
| `--database` | `data/state/crawler_state.sqlite3` | SQLite state/idempotency bền vững qua nhiều run |
| `--parquet-root` | `data/lake/google_maps_reviews` | Dataset review Parquet append-only |
| `--checkpoints` | `data/checkpoints` | Checkpoint JSON theo store |
| `--concurrency` | `1` | Số request đồng thời, từ 1 đến 10 |
| `--headed` | tắt | Hiển thị cửa sổ browser |

Manifest tối thiểu:

```json
{
  "stores": [
    {
      "retailer_store_id": "710",
      "store_name": "Coles World Square",
      "address": "650 George St, Sydney NSW 2000, Australia"
    }
  ]
}
```

`store_name` dùng để chọn đúng search candidate. `address`, nếu có, được dùng để loại candidate trùng tên nhưng sai địa điểm và xác nhận place entity.

## Runner quan sát đầy đủ

Runner dưới đây phù hợp khi cần log, checkpoint, DLQ và summary chi tiết:

```cmd
run_coles_small_set_full_crawl.cmd --store-id coles-berowra --validation-mode --max-scrolls 50 --timeout-seconds 600
```

Mỗi run tạo một thư mục dưới `data/full_crawl/coles_small_set/<run-id>/` gồm:

| Artifact | Nội dung |
|---|---|
| `data/state/crawler_state.sqlite3` | Identity, content hash, first/last seen và outbox chưa export |
| `data/lake/google_maps_reviews/crawl_date=YYYY-MM-DD/run_id=<run_id>/part-*.parquet` | Payload review INSERT/UPDATE |
| `checkpoints/` | Trạng thái gần nhất của từng store |
| `metadata/` | Metadata, navigation diagnostics và kết quả từng store |
| `progress.log` | Log luồng browser/crawl |
| `scroll_metrics.jsonl` | Timing và kết quả mỗi lần scroll |
| `review_id_trace.jsonl` | Review ID được phát hiện ở scroll nào |
| `dlq.jsonl` | Kết quả terminal không thành công |
| `summary.json` | Tổng hợp cấu hình và kết quả run |

Review được ghi ngay sau khi parse thành công. Vì vậy dữ liệu đã thu thập vẫn được giữ nếu run dừng giữa chừng. Checkpoint hiện dùng để quan sát, chưa khôi phục vị trí scroll.

## Giới hạn

- Google Maps DOM có thể thay đổi theo thời điểm, locale và rollout giao diện.
- Crawler phải vào đúng URL `/maps/place/...` và xác nhận place entity trước khi đọc review.
- Guest access có thể chuyển giữa `FULL`, `LIMITED` và `AUTH_REQUIRED`; crawler ghi nhận trạng thái, không tìm cách vượt hạn chế.
- Retry policy có quyết định `COOLDOWN`, nhưng adapter chưa tự thực thi `delay_seconds`.
- Production CLI hiện tạo retailer/store ID theo manifest Coles.
