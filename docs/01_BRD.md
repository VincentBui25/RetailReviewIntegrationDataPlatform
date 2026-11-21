# F&B Data Integration & Recommendation Platform

**Business Requirements Document (BRD)**\
**Phiên bản:** 1.5 · **Ngày:** 19/11/2026 · **Trạng thái:** Draft — chờ duyệt\
**Phạm vi đầu tiên:** Phường Khánh Hội, TP.HCM

## Lịch sử thay đổi và baseline

| Phiên bản | Ngày | Trạng thái / thay đổi |
|---|---|---|
| 1.3 | 28/10/2026 | Baseline làm việc, Draft — chưa được phê duyệt; phạm vi nguồn trước đây là Overture + OSM. |
| 1.4 | 05/11/2026 | Draft — chờ duyệt; thay vai trò nguồn, tách DQ/dedup/ER, xác định trạng thái xác minh, bổ sung Point Map và phân tích loại hình/khu vực bắt buộc; Density Map tùy chọn; giữ reviews/sentiment/recommendation ở giai đoạn sau. Giữ BG-01–05 và BR-01–11. |
| 1.5 | 19/11/2026 | Draft — Store Catalog Snapshot và Customer Review Analytics; grain rõ ràng, review → Google identity → accepted canonical store; DQ tách khỏi business facts; sentiment/recommendation/ML giữ ở phase sau. |

Bản v1.5 bổ sung analytics trên các publications đã kiểm chứng; không thay ER policy hoặc khẳng định quyền tái phân phối dữ liệu.

Bản này kế thừa BRD v1.4 và lịch sử v1.3 theo quyết định phạm vi hiện tại, không xác nhận v1.3 đã được duyệt và không nhập yêu cầu từ BRD Market Intelligence riêng. SRS liên kết: `03_SRS.md` v1.1 Draft. **PRD `02_PRD.md` còn chờ đối soát**; các liên kết PF hiện có chưa được tái xác nhận.

## 1. Tổng quan dự án

Thông tin về quán ăn, nhà hàng và quán cà phê đang nằm ở nhiều nguồn dữ liệu địa điểm. Cùng một chi nhánh có thể được ghi bằng tên, địa chỉ, tọa độ hoặc mã định danh khác nhau; ngược lại, hai chi nhánh cùng thương hiệu có thể bị nhầm là một. Điều này gây khó khăn khi xây danh mục cửa hàng, thống kê hoặc kết hợp thông tin đánh giá.

**F&B Data Integration & Recommendation Platform** hợp nhất dữ liệu địa điểm thành một **danh mục cửa hàng thống nhất**, có thể tìm kiếm, phân tích và truy về bản ghi gốc. Sau khi danh mục đủ tin cậy và có dữ liệu reviews được phép sử dụng, hệ thống sẽ phân tích nội dung đánh giá để hỗ trợ gợi ý địa điểm phù hợp hơn với nhu cầu.

**Ưu tiên của dự án:** xây dựng dữ liệu đầu ra chính xác, kiểm chứng được và có thể cập nhật; recommendation là sản phẩm khai thác dữ liệu ở giai đoạn sau.

## 2. Bài toán cần giải quyết

| Vấn đề | Ảnh hưởng hiện tại | Kết quả mong muốn |
|---|---|---|
| Một cửa hàng xuất hiện ở nhiều nguồn | Thống kê trùng, khó liên kết dữ liệu | Nhận diện cùng chi nhánh và quản lý dưới một định danh thống nhất |
| Thông tin thiếu hoặc không đồng nhất | Khó tìm kiếm, phân loại, xác minh | Chuẩn hóa thông tin và giữ nguồn gốc để đối chiếu |
| Không rõ dữ liệu có đầy đủ, đáng tin hay không | Dễ hiểu nhầm số POI thành số quán thực tế | Đo độ phủ, chất lượng và thể hiện phần chưa chắc chắn |
| Khó chọn quán theo nhu cầu cụ thể | Chỉ dựa vào vị trí/loại hình chưa thể hiện đầy đủ trải nghiệm | Bổ sung đặc điểm từ reviews hợp lệ và đánh giá hiệu quả gợi ý ở giai đoạn sau |
| Dữ liệu thay đổi theo thời gian | Danh mục nhanh lỗi thời, khó biết thay đổi ở đâu | Có khả năng cập nhật, đối chiếu các phiên bản dữ liệu |

## 3. Mục tiêu nghiệp vụ

- **BG-01 — Hợp nhất địa điểm:** Tạo một danh mục cửa hàng F&B không bị nhân đôi chỉ vì xuất hiện ở nhiều nguồn.
- **BG-02 — Đảm bảo tính tin cậy:** Mỗi cửa hàng có thông tin nguồn gốc, tình trạng dữ liệu và kết quả đối chiếu có thể kiểm tra.
- **BG-03 — Hỗ trợ khai thác dữ liệu:** Có thể tìm kiếm, lọc và phân tích địa điểm theo khu vực, loại hình, nguồn và mức độ đầy đủ thông tin.
- **BG-04 — Có thể cập nhật:** Dữ liệu mới hoặc thay đổi được xử lý mà không làm hỏng danh mục đã có.
- **BG-05 — Tạo giá trị từ reviews:** Khi có nguồn dữ liệu phù hợp, đánh giá liệu đặc điểm rút ra từ reviews có cải thiện gợi ý quán so với chỉ dùng thông tin cơ bản hay không.

## 4. Đối tượng sử dụng

| Đối tượng | Nhu cầu chính |
|---|---|
| Người tìm địa điểm ăn uống | Xem danh sách quán không trùng lặp và, ở giai đoạn sau, nhận gợi ý theo nhu cầu |
| Người phân tích dữ liệu | Tra cứu danh mục, thống kê địa điểm, so sánh độ phủ và chất lượng giữa các nguồn |
| Người vận hành dữ liệu | Kiểm tra dữ liệu đầu vào, các trường hợp đối chiếu chưa chắc chắn, kết quả cập nhật và lỗi |

## 5. Phạm vi dự án

### 5.1. Giai đoạn đầu — danh mục F&B tại Khánh Hội

- **Khu vực:** Phường Khánh Hội, TP.HCM; sử dụng ranh giới địa lý được xác minh và ghi rõ phiên bản.
- **Nguồn địa điểm:** Overture Maps là nguồn POI chính. Google Maps cung cấp bằng chứng xác minh chi nhánh, khử trùng lặp và bổ sung thuộc tính từ quan sát đã lưu được phép sử dụng; ghi nguồn, phiên bản/thời điểm và phạm vi quan sát. Không yêu cầu xác minh Google cho mọi ứng viên.
- **Đối tượng:** Các địa điểm thuộc nhóm F&B theo danh mục được thống nhất trước khi xử lý. Một cửa hàng được tính theo **chi nhánh vật lý**, không tính theo thương hiệu.
- **Xử lý nghiệp vụ:** Tách kiểm tra chất lượng dữ liệu (DQ), khử trùng lặp trong cùng nguồn (deduplication) và đối chiếu danh tính giữa các nguồn (ER); chuẩn hóa và tạo danh mục với bằng chứng, quyết định và mức độ chưa chắc chắn được giữ riêng.
- **Sử dụng kết quả bắt buộc:** Tra cứu/lọc danh mục, Point Map (bản đồ điểm POI), phân tích loại hình/khu vực và báo cáo chất lượng có nguồn gốc, trạng thái xác minh. Không bắt buộc website. **Density Map/phân tích mật độ địa lý là tùy chọn**, không chặn nghiệm thu; chỉ phản ánh dữ liệu quan sát, không phải mật độ đầy đủ của thị trường.

### 5.2. Giai đoạn tiếp theo

- Mở rộng nguồn/phạm vi reviews chỉ khi xác minh điều kiện **thu thập, lưu trữ, sử dụng và phân phối**. Analytics hiện tại chỉ dùng các logical reviews trong release pilot đã pin; không yêu cầu crawl thêm.
- Phân tích sentiment và những đặc điểm được phản ánh trong reviews như món ăn, dịch vụ, không gian, mức giá khi có đủ bằng chứng.
- Xây dựng và đánh giá chức năng gợi ý địa điểm; bổ sung cập nhật định kỳ và mở rộng sang khu vực khác khi kết quả pilot đáp ứng yêu cầu.

### 5.3. Ngoài phạm vi hiện tại

- OSM không thuộc luồng tích hợp POI hiện tại; Foody tạm dừng. Không chạy crawler live để đáp ứng MVP. Sentiment, recommendation và ML ngoài Analytics MVP. Review analytics chỉ dùng dữ liệu pilot hiện có; không mặc định có quyền công bố review text hoặc dữ liệu gốc.

- Thu thập toàn bộ cửa hàng hoặc reviews của TP.HCM ngay từ đầu.
- Xây chức năng đặt bàn, đặt món, giao hàng hoặc thanh toán.
- Coi mọi POI thu thập được là cửa hàng đang hoạt động hoặc coi dữ liệu quan sát là danh sách đầy đủ toàn bộ quán trong khu vực.
- Phụ thuộc vào dữ liệu reviews chưa xác nhận quyền sử dụng để hoàn thành giai đoạn đầu.

### 5.4. Analytics MVP v1.5

- **BP1 Store Catalog Snapshot:** 1 canonical store × 1 Gold release; phân tích số records, category, khu vực, nguồn, trạng thái liên kết và Google Place rating/count nếu có evidence hợp lệ.
- **BP2 Customer Review Analytics:** 1 unique logical review; phân tích số reviews, phân bố rating và thời gian khi timestamp verified. Không nhân bản do collection/release lặp lại.
- Reviews luôn truy về Google identity gốc. Association sang canonical store chỉ qua accepted mapping; unmatched reviews vẫn được giữ và báo riêng. Thiếu ngày/rating giữ UNKNOWN/NULL, không dùng ngày crawl thay thế.
- Review coverage là tỷ lệ canonical anchors có ít nhất một attached review quan sát; không phải tỷ lệ thu thập đủ mọi review Google. Google Place aggregate rating không phải AVG review sample; không cộng store_count qua các releases.
- Warehouse có bốn dimensions (store/category/area/date), hai business facts và source bridge. ER quality/source reconciliation là audit views, không tạo business fact thứ ba. BI chỉ đọc một active release; refresh atomic và idempotent.
- PostgreSQL analytics và Superset metadata tách database khỏi Airflow/crawler. Superset dùng serving views với credentials read-only; công thức KPI định nghĩa tại dbt/PostgreSQL. Superset là giao diện BI hiện hành; prototype HTML đã ngừng duy trì, các publication lịch sử được giữ nguyên.
- Point Map dùng street tiles ít chi tiết có attribution; một ward được chọn mặc định nếu catalog chỉ hỗ trợ ward đó. Bỏ lọc khu vực trả grid, không tải mọi points toàn thành phố. Phân tích thị trường cho phép tỷ lệ catalog F&B/km² và F&B/1.000 dân theo reference đã pin, ghi rõ năm/basis/nguồn và WARNING nếu chưa xác minh độc lập. Không gọi số liệu đề án là census hoặc diện tích đã chứng thực; thiếu/0 mẫu số trả NULL. Không suy diễn sức mua, bão hòa hoặc độ phủ toàn thị trường từ các tỷ lệ này.
- Quyền sử dụng/tái phân phối Google/reviews vẫn là giới hạn độc lập với việc pipeline có dữ liệu. Không công bố raw reviews, reviewer PII, credentials hoặc source datasets khi chưa có quyền.

## 6. Yêu cầu nghiệp vụ

| ID | Yêu cầu | Giai đoạn |
|---|---|---|
| BR-01 | Tiếp nhận POI Overture làm nguồn chính và quan sát Google Maps đã lưu, được phép dùng để xác minh/bổ sung; lưu nguồn, phiên bản/thời điểm và phạm vi; không yêu cầu Google phủ mọi ứng viên | Đầu tiên |
| BR-02 | Nhận diện các bản ghi thuộc cùng **một chi nhánh** và tạo định danh cửa hàng thống nhất | Đầu tiên |
| BR-03 | Giữ liên kết từ cửa hàng thống nhất về các bản ghi và mã định danh của từng nguồn | Đầu tiên |
| BR-04 | Tách DQ, dedup trong nguồn và ER giữa nguồn; phân biệt trùng/khác/cần xác minh, giữ trạng thái xác minh và không tự gộp khi thiếu bằng chứng | Đầu tiên |
| BR-05 | Cung cấp danh mục tra cứu/lọc, Point Map và phân tích loại hình/khu vực/nguồn bắt buộc; Density Map tùy chọn, không chặn MVP | Đầu tiên |
| BR-06 | Báo cáo riêng DQ, dedup trong nguồn, liên kết ER được chấp nhận, đóng góp nguồn và trạng thái xác minh; ghi mẫu số/phạm vi, không coi độ phủ Google là đầy đủ | Đầu tiên |
| BR-07 | Khi xử lý lại cùng dữ liệu và cùng quy tắc, không phát sinh thêm cửa hàng trùng và giữ kết quả nhất quán | Đầu tiên |
| BR-08 | Hỗ trợ nhận diện bản ghi mới, thay đổi hoặc không còn xuất hiện trong phiên bản nguồn mới, nhưng không tự kết luận quán đã đóng cửa | Tiếp theo |
| BR-09 | Bảo toàn logical reviews, phân tích counts/rating/ngày có bằng chứng; chỉ gắn canonical store qua accepted mapping; tách Google Place rating khỏi collected-review rating | Analytics MVP, với nguồn hợp lệ |
| BR-10 | Cho phép tìm kiếm/gợi ý quán theo nhu cầu và so sánh kết quả có dùng review features với phương án chỉ dùng thông tin cơ bản | Tiếp theo |
| BR-11 | Cho phép kiểm tra tình trạng cập nhật, chất lượng dữ liệu và các lỗi ảnh hưởng đến kết quả đã công bố | Tiếp theo |

## 7. Quy tắc nghiệp vụ

1. **Một cửa hàng là một chi nhánh thực tế.** Hai địa điểm cùng thương hiệu nhưng ở hai chi nhánh khác nhau không được gộp.
2. Trùng tên hoặc gần tọa độ **không đủ** để khẳng định hai bản ghi là cùng một cửa hàng.
3. Các bản ghi chưa chắc chắn phải được giữ để kiểm tra, không bị tự động xóa hoặc hợp nhất.
4. Mỗi cửa hàng trong danh mục phải có ít nhất một bản ghi nguồn truy vết được; thông tin suy ra phải phân biệt với thông tin nguồn.
5. Thiếu một địa điểm ở nguồn mới **không có nghĩa** địa điểm đã đóng cửa; trạng thái hoạt động cần bằng chứng riêng.
6. Số POI trong dữ liệu không được công bố như số cửa hàng thực tế hay mức độ bao phủ tuyệt đối nếu chưa có kiểm chứng độc lập.
7. Chỉ truy cập, lưu trữ, tái sử dụng và chia sẻ dữ liệu trong giới hạn được phép của từng nguồn, kể cả metadata Google Maps, reviews và dữ liệu có điều kiện tái phân phối. Có crawler hoặc dữ liệu đã lưu không chứng minh quyền sử dụng; không tự đưa dữ liệu hạn chế vào bản đồ/báo cáo công bố.
8. Dữ liệu và các quyết định đối chiếu được công bố phải có khả năng giải thích, kiểm tra và tái tạo bằng đầu vào tương ứng.

### Trạng thái xác minh và phạm vi quyết định

- **MACHINE_ACCEPTED_LINK:** liên kết được canonical ER strict policy chấp nhận; không phải human-verified hoặc ground truth, không chứng minh cửa hàng đang hoạt động.
- **HUMAN_VERIFIED_LINK:** chỉ khi có quyết định thực của con người, evidence và provenance còn áp dụng; không tạo từ nhãn AI-assisted thử nghiệm.
- **UNVERIFIED_SOURCE_ONLY:** canonical anchor chưa có accepted Google mapping; không đồng nghĩa NON_MATCH.
- **REVIEW_REQUIRED / INSUFFICIENT_DATA / NON_MATCH:** phân loại proposal, không tự hợp nhất. Một NON_MATCH chỉ bác bỏ cặp cụ thể, không xóa Overture anchor.

Giữ raw ER proposals, quyết định mapping cuối và Gold taxonomy evidence riêng. Không thêm matcher sau canonical ER. Shared Google ID giữa hai Overture records phải giữ conflict nếu chưa có evidence/decision hợp lệ phân biệt thực thể. `data_id` và `place_id` là hai loại định danh nguồn, không hoán đổi.
725 records của pilot là Overture-anchored canonical records, không phải census hoặc 725 cửa hàng đã xác minh độc lập. Source agreement/provisional category không được nâng thành human verified.

## 8. Kết quả cần bàn giao

| Kết quả | Người sử dụng nhận được |
|---|---|
| **Danh mục cửa hàng thống nhất** | Mỗi chi nhánh có định danh ổn định, thông tin cơ bản và trạng thái dữ liệu |
| **Liên kết nguồn** | Truy được POI Overture và liên kết Google được chấp nhận khi có, cùng phiên bản/thời điểm; đề xuất chưa xác nhận không được trình bày như liên kết tin cậy |
| **Danh sách cần xác minh** | Biết những cặp địa điểm chưa đủ bằng chứng để tự quyết định |
| **Báo cáo chất lượng và độ phủ** | Số lượng nguồn, độ trùng, phần bổ sung, dữ liệu thiếu hoặc không hợp lệ |
| **Kết quả tra cứu/phân tích** | Có thể sử dụng danh mục để trả lời các câu hỏi cơ bản về địa điểm F&B trong phạm vi pilot |
| **Point Map và phân tích loại hình/khu vực** *(bắt buộc MVP)* | Bản đồ điểm và thống kê đối soát với catalog cùng phạm vi/phiên bản, thể hiện trạng thái chưa xác minh |
| **Review Analytics** *(Analytics MVP)* | Counts, rating distribution và review trends có ngày verified; bảo toàn Google identity và attached/unmatched, không đưa reviewer/text vào serving views |
| **Density Map** *(tùy chọn)* | Mật độ dữ liệu catalog quan sát với phạm vi và phương pháp được ghi rõ; không phải tổng thị trường |
| **Báo cáo đánh giá gợi ý** *(giai đoạn sau)* | So sánh kết quả có/không sử dụng thông tin rút ra từ reviews |

## 9. Tiêu chí thành công của giai đoạn đầu

Một pilot được xem là hoàn thành khi:

1. **Có dữ liệu Overture thật và quan sát Google đã lưu được phép dùng** trong phạm vi pilot; ghi thời điểm/phạm vi từng nguồn. Không yêu cầu Google xác minh toàn bộ 725 ứng viên hoặc mọi canonical entry.
2. **Có danh mục thống nhất:** mỗi cửa hàng đầu ra truy được về nguồn và giữ riêng các trường hợp chưa đủ bằng chứng để gộp.
3. **Có kiểm chứng chất lượng đối chiếu:** chuẩn bị tập cặp được gán nhãn, báo cáo Precision/Recall/F1 trên tập đánh giá và giải thích các lỗi gộp nhầm/tách nhầm. Không dùng các chỉ số này để khẳng định độ phủ toàn bộ địa điểm thực tế.
4. **Có báo cáo chất lượng và đóng góp của từng nguồn:** tách DQ, dedup trong nguồn và ER; nêu liên kết được chấp nhận, đề xuất bị bác bỏ/chưa xác định, phần chỉ có ở một nguồn, dữ liệu thiếu và mẫu số. Không suy ra độ phủ thị trường.
5. **Có khả năng chạy lại:** cùng đầu vào và quy tắc cho kết quả logic nhất quán, không nhân đôi cửa hàng. Mọi thay đổi giữa các lần chạy phải có thể giải thích.
6. **Có thể khai thác đầu ra:** người sử dụng truy vấn/lọc được danh mục, xem Point Map, phân tích loại hình/khu vực và báo cáo pilot có thể đối soát. Density Map không chặn nghiệm thu; sentiment/recommendation/ML được hoãn. Review analytics dùng grain riêng và phải đối soát attached/unmatched, không hồi tố thay đổi các milestone integration đã PASS.

Các mục tiêu định lượng cụ thể về độ chính xác, thời gian xử lý và độ mới sẽ được đặt sau khi có dữ liệu khảo sát và baseline, thay vì ghi các con số chưa được đo.

## 10. Giả định, phụ thuộc và rủi ro

| Vấn đề | Cách xử lý hoặc giới hạn |
|---|---|
| Overture/Google thiếu hoặc sai địa điểm | Đánh giá dữ liệu thực tế, giữ trạng thái chưa chắc chắn; không hứa bao phủ toàn bộ thị trường |
| Gộp nhầm hai chi nhánh | Ưu tiên quyết định thận trọng, có tập kiểm chứng và danh sách cần xem xét |
| Khác biệt về phân loại F&B và ranh giới địa lý | Xác nhận quy tắc phân loại và ranh giới trước khi so sánh hai nguồn |
| Dữ liệu reviews có giới hạn quyền sử dụng | Chỉ tích hợp sau khi xác minh điều kiện của nguồn; nếu không, tiếp tục với danh mục POI |
| Crawler đã có nhưng chưa xác minh khả năng sử dụng lại | Kiểm tra code, dữ liệu và giới hạn truy cập riêng; không coi việc có code là đã sẵn sàng chạy live |
| Mở rộng phạm vi khiến chi phí tăng | Hoàn thành pilot, đo kết quả và nhu cầu thực tế trước khi mở rộng |

---

**Ranh giới tài liệu:** BRD xác định bài toán, mục tiêu, yêu cầu nghiệp vụ và điều kiện thành công. Chi tiết chức năng nằm ở PRD; yêu cầu kiểm thử, hiệu năng và độ tin cậy nằm ở SRS; các luồng tương tác nằm ở Use Case Specification; thiết kế dữ liệu và công nghệ nằm ở System Design.
