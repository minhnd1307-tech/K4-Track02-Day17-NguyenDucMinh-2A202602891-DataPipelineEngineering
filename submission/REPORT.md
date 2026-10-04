# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Đức Minh / 2A202602891
**Repo:** https://github.com/minhnd1307-tech/K4-Track02-Day17-Data-Pipeline-Engineering
**Commit bài nộp:** 13e0f0fcc4dee72202f3377f60e429323334cf6e
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (hỗ trợ phân tích triệu chứng lỗi, thiết kế câu lệnh SQL MERGE, đo lường lateness và triển khai cache cho bước LLM)
**Nguồn tham khảo khác (nếu có):** Slide bài giảng K4 Day 17 (Data Pipeline Engineering), tài liệu DuckDB và dbt Core documentation.

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | Check `silver_tickets has exactly one row per ticket_id` FAIL (24 dòng cho 12 tickets); `T-91` có 3 dòng trạng thái thay vì trạng thái mới nhất. | Check `gold_feature_daily reconciles with a full recompute` FAIL (`c50b8851affe != 8630e04a61d1`); user `u05` ngày 08-12 chỉ có `(2, 0)` thay vì `(5, 1)`. | Check `deleted ticket T-97 is a tombstone` FAIL (`is_deleted=False`, còn nguyên PII); `T-97` vẫn còn trong training snapshot `v2026-08-16` và RAG chunks. |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng lệnh `INSERT INTO` thuần túy, ghi nối tiếp từng batch không theo dõi khóa và không chặn dữ liệu cũ bằng LSN guard. | `LOOKBACK_DAYS = 0` trong config, daily run chỉ tính đúng ngày hôm đó, bỏ sót các event offline xảy ra ngày 08-12 nhưng đến Kafka vào 08-15. | Debezium delete (`op='d'`) có `after = null`; query staging lấy `after->>'ticket_id'` nên bị NULL và bị `WHERE ticket_id IS NOT NULL` lọc mất. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: Dùng `MERGE INTO silver_tickets AS target USING _latest_changes AS src ON target.ticket_id = src.ticket_id WHEN MATCHED AND src._lsn > target._lsn THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...`. | `pipeline/config.py`: Đo độ trễ từ Bronze (`python main.py --lateness`), ghi nhận P99 = 3 ngày và cập nhật `LOOKBACK_DAYS = 3`. | `pipeline/staging.py`: Sửa trích xuất khóa thành `coalesce(j->'value'->'after'->>'ticket_id', j->'value'->'before'->>'ticket_id') AS ticket_id`. |
| **Khái niệm trên slide** | Silver — Có khoá; Bốn cách viết idempotent; MERGE on key & LSN guard. | Data về muộn; Event time vs Ingest time; Overwrite-partition với cửa sổ trượt (lookback = ceil(P99)). | CDC log-based (Debezium envelope before/after); Xoá phải lan (Deletes propagate); Tombstone vs Hard delete. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: `silver_tickets` cần duy trì trạng thái mới nhất cho từng thực thể và bảo vệ chống ghi đè khi replay batch cũ qua LSN guard; còn `gold_feature_daily` là bảng tổng hợp theo ngày nên ghi đè cả phân vùng (partition) đơn giản hơn, rẻ hơn và tự động hấp thụ toàn bộ dữ liệu đến muộn trong cửa sổ lookback mà không lo trùng lặp.
- Tombstone thay vì xoá hẳn hàng trong Silver: Tombstone giữ lại khoá `ticket_id` và `_lsn` mới nhất để ngăn hiện tượng "hồi sinh" (resurrection) khi hệ thống replay lại log CDC cũ, đồng thời xoá trắng dữ liệu cá nhân (`user_id`, `subject`, `body` = NULL) để tuân thủ quyền được xoá dữ liệu (GDPR).
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Mô hình AI cần tính tái lập (reproducibility) tuyệt đối để debug và kiểm định; việc dựng snapshot "as-of" từ Bronze bất biến đảm bảo dữ liệu quá khứ không bị rò rỉ thông tin tương lai (data leakage) và không bị sai lệch khi pipeline chạy lại.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Với quy mô dữ liệu vừa và nhỏ chạy trên máy đơn, DuckDB là engine OLAP dạng in-process có tốc độ vượt trội nhờ vectorized execution, chi phí vận hành bằng 0, không gặp chi phí phụ (overhead) về khởi động cụm, JVM hay shuffle mạng như Spark.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   - Để cân bằng giữa tính bất biến (phục vụ kiểm toán, tái lập mô hình) và quyền được quên (GDPR Article 17), giải pháp chuẩn trong thực tế là áp dụng **Crypto-shredding**: văn bản nhạy cảm của mỗi khách hàng được mã hoá bằng một khoá riêng (user-specific encryption key). Khi khách hàng yêu cầu xoá, hệ thống chỉ cần huỷ khoá mã hoá này; file snapshot cũ vẫn giữ nguyên tính toàn vẹn (checksum không đổi) nhưng nội dung của T-97 trở thành vô nghĩa vĩnh viễn và không thể khôi phục. Ngoài ra, cần thiết lập chính sách lưu trữ (Data Retention Policy): các snapshot quá hạn (ví dụ > 90 ngày) sẽ được dọn dẹp hoặc chạy quy trình compaction định kỳ để redact dữ liệu đã xoá.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   - **Chốt PII**: Cần kết hợp Regex (đối với mẫu cố định như email, SĐT, số tài khoản) với mô hình **NER (Named Entity Recognition)** đa ngữ/tiếng Việt (như spaCy, XLM-RoBERTa hoặc Microsoft Presidio) để nhận diện thực thể tên người (PER) và địa chỉ (LOC).
   - **Tầng áp dụng**: Đặt ngay tại **cửa ngõ giữa Bronze và Silver** (bước Staging/Cleansing). Tầng Bronze được cô lập và mã hoá nghiêm ngặt (chỉ cấp quyền cho pipeline), toàn bộ dữ liệu đi vào Silver và Gold bắt buộc phải đi qua chốt che PII này để đảm bảo dữ liệu an toàn cho downstream users và AI models.
   - **Cách đo lường**: Đánh giá trên tập dữ liệu kiểm thử được gán nhãn thủ công (Ground Truth) qua hai chỉ số **Precision** và **Recall**. Trong an toàn dữ liệu, ưu tiên tối đa Recall (mục tiêu Recall >= 99.5%) nhằm hạn chế thấp nhất rủi ro bỏ sót PII (False Negative), đồng thời chạy các batch test tự động quét rò rỉ định kỳ trên Silver/Gold.

## 5. Output (dán nguyên văn)

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
============================= test session starts =============================
platform win32 -- Python 3.11.0, pytest-8.4.2, pluggy-1.6.0
rootdir: D:\AI Vin\K4-Track02-Day17-Data-Pipeline-Engineering
configfile: pytest.ini
collected 34 items

tests\test_contracts.py ............                                     [35%]
tests\test_extensions.py ..........                                      [64%]
tests\test_rerun.py .                                                    [67%]
tests\test_units.py ...........                                          [100%]

============================= 34 passed in 3.42s ==============================

$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
Running with dbt=1.12.5
Registered adapter: duckdb=1.11.0
Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test

Concurrency: 1 threads (target='dev')

1 of 19 START sql view model main.stg_events ................................... [RUN]
1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.09s]
2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.04s]
3 of 19 START sql incremental model main.silver_events ......................... [RUN]
3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.26s]
4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.21s]
8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.21s]
5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.06s]
6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
7 of 19 START test unique_silver_events_event_id ............................... [RUN]
7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.03s]
9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.05s]
10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.04s]
11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.04s]
12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.03s]
14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.03s]
15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.05s]
Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.04s]
Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.04s]
Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.04s]
Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.04s]
Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.04s]
Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.04s]
16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.33s]
17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.04s]
18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]

Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.84 seconds (1.84s).

Completed successfully

Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree

$ make bonus-llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```
