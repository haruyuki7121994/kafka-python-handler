# Fanout state giữa Consumer và Worker

Tài liệu mô tả code tại commit `3c6330c` của repository `kafka-python-handler`.
Các khối Mermaid có thể hiển thị thành sơ đồ trong GitHub hoặc trình đọc Markdown hỗ trợ Mermaid.

## 1. Hai trạng thái trên cùng một outbox

`status` theo dõi việc publish Kafka. `fanoutStatus` theo dõi việc phân phối Post đến feed của follower. Sau khi publish thành công, `status` giữ giá trị `PUBLISHED` trong suốt quá trình fanout.

```mermaid
flowchart LR
    P["Producer: publish Kafka thành công"] --> O["Outbox: status = PUBLISHED"]
    O --> C["Consumer: chọn cách phân phối"]
    C --> N["Normal: fanoutStatus = IN_PROGRESS"]
    C --> R["Celebrity: fanoutStatus = FANOUT_ON_READ"]
    N --> W["SQS → Worker → Redis"]
    W --> D["fanoutStatus = COMPLETED"]
    W --> F["fanoutStatus = FAILED"]
```

`FANOUT_ON_READ` chỉ xác nhận đã chọn nhánh celebrity. API đọc feed vẫn cần lấy bài celebrity và trộn vào kết quả; consumer không thực hiện bước đọc feed đó.

## 2. Sơ đồ chuyển trạng thái

```mermaid
stateDiagram-v2
    state "Chưa có fanoutStatus" as Unset
    state "Đang phân phối" as IN_PROGRESS
    state "Chọn đọc bài celebrity khi lấy feed" as FANOUT_ON_READ
    state "Tất cả batch hoàn thành" as COMPLETED
    state "Fanout thất bại" as FAILED

    [*] --> Unset: outbox.status = PUBLISHED
    Unset --> FANOUT_ON_READ: Consumer / followersCount > 100000
    Unset --> IN_PROGRESS: Consumer / prepare số batch và hash
    IN_PROGRESS --> IN_PROGRESS: Consumer gửi task hoặc Worker hoàn thành một batch
    IN_PROGRESS --> COMPLETED: scheduled = true và completed = expected
    IN_PROGRESS --> FAILED: Worker bắt lỗi ở lượt nhận SQS thứ 3 trở lên
    FANOUT_ON_READ --> [*]: expiresAt = thời điểm xử lý + 7 ngày
    COMPLETED --> [*]: expiresAt = thời điểm hoàn thành + 7 ngày
    FAILED --> [*]: expiresAt = thời điểm thất bại + 7 ngày
```

Consumer bỏ qua event gửi lại nếu `fanoutStatus` đã là `COMPLETED`, `FAILED` hoặc `FANOUT_ON_READ`. Nếu đang `IN_PROGRESS`, consumer tiếp tục nhánh ghi feed, kể cả khi `followersCount` đã vượt ngưỡng celebrity.

## 3. Luồng thành công giữa Consumer và Worker

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka
    participant C as Consumer Lambda
    participant D as DynamoDB outboxes
    participant Q as SQS fanout
    participant W as Worker Lambda
    participant S as Secrets Manager
    participant R as Redis

    K->>C: POST_CREATED với eventId, postId, authorId
    C->>D: Đọc fanoutStatus
    Note over C: Đọc followersCount từ users<br/>Query followers và chia tối đa 500 IDs/task
    C->>D: prepare(expected=N, planHash)<br/>IN_PROGRESS, completed=0
    Note over C,D: prepare dùng if_not_exists<br/>Retry không đặt completed về 0
    C->>Q: Gửi N task, tối đa 10 message mỗi API call
    Note over C,W: Worker có thể bắt đầu trước khi Consumer gửi hết task
    Q->>W: Nhận một task
    W->>D: Kiểm tra fanoutStatus = IN_PROGRESS
    W->>S: Lấy cấu hình khi chưa có Redis client
    S-->>W: host, port, username, password, ssl
    W->>R: ZADD vào từng follower feed<br/>Trim còn 1000 bài, đặt TTL key 30 ngày
    R-->>W: Pipeline thành công
    W->>D: Transaction: tạo receipt mới<br/>và tăng fanoutCompleted lên 1
    W->>D: complete_if_ready()
    Note over W,D: Chỉ COMPLETED khi scheduled=true<br/>và completed=expected
    W-->>Q: Lambda xử lý thành công
    C->>D: Sau khi mọi task được SQS chấp nhận:<br/>fanoutScheduled=true
    C->>D: complete_if_ready()
    C-->>K: Handler hoàn thành thành công
```

Consumer và worker đều gọi `complete_if_ready()`. Vì vậy, dù worker hoàn thành trước hay sau khi consumer đặt `fanoutScheduled=true`, bên chạy sau vẫn có thể kết thúc event. Nếu không có follower, `expected=completed=0`; consumer tự hoàn thành sau bước `scheduled()`.

## 4. Retry và DLQ

```mermaid
flowchart TD
    A["Worker nhận task"] --> B{"Xử lý thành công?"}
    B -->|Có| C["Trả thành công; Lambda xóa message"]
    B -->|Không| D{"ApproximateReceiveCount >= 3?"}
    D -->|Không| E["Trả batchItemFailures"]
    E --> F["Message có thể được nhận lại<br/>sau visibility timeout"]
    F --> A
    D -->|Có| G["Thử đặt fanoutStatus = FAILED<br/>expiresAt = hiện tại + 7 ngày"]
    G --> H["Trả batchItemFailures"]
    H --> I["SQS redrive sang DLQ<br/>maxReceiveCount = 3"]
    I --> J["Chờ tiến trình xử lý thất bại sau này"]
```

Cấu hình hiện tại là **3 lượt nhận tổng cộng**, gồm lượt đầu tiên. Worker không chạy vòng lặp gọi Redis ba lần trong một invocation. `VisibilityTimeout` là 360 giây, timeout worker là 60 giây.

Việc cập nhật `FAILED` diễn ra trong worker trước khi SQS chuyển message sang DLQ. Nếu Lambda bị timeout, không chạy được, hoặc ghi DynamoDB thất bại, message có thể vào DLQ trong khi outbox vẫn `IN_PROGRESS`. Code hiện chưa có tiến trình đối soát DLQ để sửa trường hợp này.

`FAILED` áp dụng cho toàn bộ event. Các task còn lại nhìn thấy trạng thái này sẽ bị worker từ chối và tiếp tục cơ chế retry/DLQ; các lần ghi Redis đã thành công trước đó không bị rollback.

## 5. Các trường dữ liệu

| Trường trên outbox | Ai ghi | Ý nghĩa |
|---|---|---|
| `status` | Producer | Trạng thái publish Kafka; giữ `PUBLISHED` khi fanout chạy |
| `fanoutStatus` | Consumer và Worker | `IN_PROGRESS`, `COMPLETED`, `FAILED`, `FANOUT_ON_READ` |
| `fanoutExpected` | Consumer | Tổng số SQS task, không phải tổng follower |
| `fanoutCompleted` | Worker | Số task đã ghi Redis thành công và được ghi nhận bằng transaction |
| `fanoutScheduled` | Consumer | `true` sau khi tất cả task được SQS chấp nhận |
| `fanoutPlanHash` | Consumer | Hash danh sách task để phát hiện kế hoạch thay đổi khi Kafka gửi lại |
| `fanoutFailedAt` | Worker | Thời điểm ghi nhận thất bại |
| `expiresAt` | Consumer hoặc Worker | Unix timestamp theo giây để DynamoDB TTL dọn outbox |

Mỗi task thành công tạo thêm một item receipt trong cùng bảng `outboxes`:

```text
PK        = FANOUT_TASK#{eventId}#{taskId}
expiresAt = thời điểm ghi receipt + 30 ngày
```

Transaction tạo receipt với điều kiện chưa tồn tại và tăng `fanoutCompleted` cùng lúc. Khi task gửi lại, receipt đã tồn tại sẽ ngăn bộ đếm tăng thêm. Worker vẫn có thể ghi Redis lại trước khi kiểm tra receipt: `ZADD` dùng cùng `postId` và điểm thời gian nên không tạo hai member cho cùng Post.

Redis và DynamoDB không nằm trong một transaction chung. Nếu Redis ghi xong nhưng DynamoDB lỗi, task sẽ retry; lần ghi Redis lặp lại là một phần của thiết kế này.

## 6. Ví dụ 1.001 follower

Consumer tạo ba task: `taskId=0` chứa 500 follower, `taskId=1` chứa 500 follower và `taskId=2` chứa 1 follower.

| Thời điểm | Expected | Completed | Scheduled | Fanout status |
|---|---:|---:|---|---|
| Consumer prepare | 3 | 0 | Chưa có | `IN_PROGRESS` |
| Worker hoàn thành task 0 sớm | 3 | 1 | Chưa có | `IN_PROGRESS` |
| Consumer gửi hết task | 3 | 1 | `true` | `IN_PROGRESS` |
| Worker hoàn thành task 2 | 3 | 2 | `true` | `IN_PROGRESS` |
| Task 0 được gửi lại | 3 | 2 | `true` | `IN_PROGRESS` |
| Worker hoàn thành task 1 | 3 | 3 | `true` | `COMPLETED` |

## 7. Giới hạn cần biết khi đọc sơ đồ

- Consumer dựng lại danh sách follower khi Kafka gửi lại event. Nếu danh sách thay đổi làm `fanoutPlanHash` khác, `prepare()` báo lỗi; code chưa lưu một snapshot task cố định để tự phục hồi trường hợp này.
- Task từ phiên bản consumer cũ chưa có `taskId` sẽ bị worker mới từ chối. Cần xử lý backlog cũ khi nâng cấp.
- Queue và DLQ hiện có retention một ngày; outbox thành công hoặc thất bại có TTL bảy ngày. `expiresAt` là mốc đủ điều kiện xóa, không bảo đảm item bị xóa ngay tại mốc đó.
- `COMPLETED` xác nhận các pipeline Redis và ghi nhận DynamoDB đã thành công tại thời điểm xử lý. Redis feed có thể bị trim, hết TTL hoặc mất do sự cố cache sau đó; Post gốc vẫn được lưu riêng trong DynamoDB.

Code tham chiếu: `lambdas/lambda_function_consumer.py`, `lambdas/lambda_function_worker.py`, `lambdas/fanout_state.py` và `infra/template.yml` tại [commit 3c6330c](https://github.com/haruyuki7121994/kafka-python-handler/commit/3c6330c).
