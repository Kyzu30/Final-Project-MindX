# Airport Operations ETL

Project cuối khóa **Data Processing & Automation**. Apache Airflow đọc dữ liệu chuyến bay từ CSV, làm sạch và nạp vào MySQL. Truy vấn SQL kiểm tra dữ liệu đã nạp và thống kê trễ khởi hành.

## 1. Mở và chạy bằng VS Code

1. Giải nén project. Mở **VS Code → File → Open Folder** và chọn thư mục có file `docker-compose.yaml`.
2. Mở Docker Desktop hoặc khởi động Docker Engine cùng Docker Compose V2. Trong VS Code, chọn **Terminal → New Terminal**.
3. Chạy các lệnh dưới đây từ thư mục project. Lần đầu Docker phải tải image và chuẩn bị cơ sở dữ liệu.

```bash
docker compose version
docker compose up -d --build
docker compose ps -a
```

Chờ `postgres`, `mysql`, `airflow-scheduler` và `airflow-webserver` chạy ổn định. `airflow-init` có trạng thái `Exited (0)` sau khi khởi tạo Airflow. PostgreSQL lưu metadata của Airflow. MySQL lưu dữ liệu nghiệp vụ và log ETL.

4. Mở [Airflow ở localhost:8080](http://localhost:8080). Tài khoản demo mặc định là **`airflow` / `airflow`**. Bật DAG **`airport_flight_departure_etl`**, bấm **Trigger DAG** và giữ `source_file` là `flights.csv`. Mở lần chạy ở tab **Grid** hoặc **Graph**.
5. Xem dữ liệu, KPI, log và lỗi từng dòng CSV bằng lệnh sau. Lệnh chạy được từ PowerShell và Bash.

```bash
docker compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u"$MYSQL_USER" "$MYSQL_DATABASE" < /opt/airport-sql/verify.sql'
```

DAG chạy theo lịch **09:00, giờ Việt Nam**, khi được bật. Lịch dùng `data/flights.csv` mặc định. Bạn có thể thay file đó bằng lô dữ liệu hiện tại trước lần chạy kế tiếp, hoặc chọn một CSV khác trong thư mục `data/` khi Trigger DAG thủ công. Các giờ chuyến bay trong dữ liệu mẫu cùng dùng giờ Việt Nam.

Muốn đổi cổng hoặc mật khẩu cho một lần thiết lập mới, sao chép `.env.example` thành `.env` và sửa rồi mới chạy `docker compose up`. Các giá trị mặc định dành cho demo trên máy cá nhân. Cổng giao diện được bind vào `127.0.0.1`. Mật khẩu của MySQL được áp dụng khi volume được khởi tạo lần đầu.

```bash
docker compose logs --tail 100 airflow-init airflow-scheduler mysql
docker compose down
```

Lệnh đầu giúp chẩn đoán lỗi khởi động. Lệnh thứ hai dừng project và giữ các volume dữ liệu.

### Khôi phục sau lỗi MySQL 1064 ở bản đầu

Nếu log MySQL báo lỗi tại `row_number` trong `01_schema.sql`, dùng bản project đã đổi cột đó thành `source_line`. MySQL chỉ chạy file khởi tạo lần đầu trên volume trống. Dùng tên Compose mới để khởi tạo volume mới và giữ dữ liệu lần chạy trước. Trong PowerShell tại thư mục project, chạy:

```powershell
docker compose stop
$env:COMPOSE_PROJECT_NAME = 'airport-operations-etl-fixed'
docker compose up -d --build
docker compose ps -a
```

Các lệnh Compose tiếp theo dùng cùng Terminal. Nếu mở Terminal mới, chạy lại dòng đặt `COMPOSE_PROJECT_NAME` trước khi kiểm tra hoặc dừng phiên đã khôi phục.

## 2. Luồng ETL

| Task | Xử lý | Minh chứng trong MySQL |
|---|---|---|
| `ingest` | Kiểm tra header, đọc CSV UTF-8 (có thể có BOM), lưu dữ liệu gốc theo mã lần chạy và số dòng | `flight_raw`, số dòng nguồn |
| `clean` | Chuẩn hóa mã và trạng thái, kiểm tra dữ liệu bắt buộc, kiểu ngày giờ, khóa ngoại và thời lượng theo lịch. Loại quan sát trùng và lưu lý do | `flight_clean`, `flight_reject`, số dòng hợp lệ, lỗi và trùng |
| `load` | Nạp nguyên tử vào `Flight` theo khóa `FlightID`. Cập nhật từ quan sát bằng hoặc mới hơn | `Flight`, số dòng đưa vào thao tác upsert |
| `check` | Đối chiếu số dòng, kiểm tra từng mã sạch đã có trong bảng đích cùng phiên bản hoặc phiên bản mới hơn, tính KPI theo lô | `flight_departure_metrics`, log số dòng đã kiểm tra |
| `finalize` | Ghi trạng thái toàn pipeline kể cả khi task trước đó thất bại. Một task ETL thất bại làm DAG run thất bại | `etl_log` với `step = 'pipeline'` |

Mỗi task nghiệp vụ ghi log `STARTED`, sau đó ghi `SUCCESS` hoặc `FAILED`. Log có mã lần chạy, số lần thử, chỉ số dạng JSON, thông báo và thời điểm UTC. Lỗi từng dòng được lưu để tra cứu, trong khi lỗi cấu trúc file hoặc lô không có dòng hợp lệ làm DAG run thất bại có thông báo. Nếu MySQL mất kết nối, xem Airflow task logs để chẩn đoán lỗi ghi log.

### Quy tắc dữ liệu

- `FlightID` là mã duy nhất của một chuyến bay cụ thể. Trong một file, giữ quan sát hợp lệ có `SourceUpdatedAt` mới nhất. Khi hai mốc bằng nhau, giữ dòng xuất hiện sau. Khi nạp một lô khác, phiên bản cũ hơn giữ nguyên bản ghi đích đã mới hơn. Mốc bằng nhau được xem là cùng phiên bản có thể gửi lại hoặc chỉnh sửa.
- `ActualDepartureTime` được phép rỗng khi trạng thái là `Scheduled`, `Delayed` hoặc `Cancelled`. `Departed` và `Arrived` cần giờ thực tế. Ngày phục vụ bằng ngày khởi hành dự kiến. Giờ đến dự kiến phải sau giờ khởi hành dự kiến.
- Một chuyến được tính **trễ** khi giờ khởi hành thực tế muộn hơn giờ dự kiến. Chuyến cất cánh sớm có số phút trễ bằng 0. Chuyến chưa cất cánh không có số phút trễ và không nằm trong mẫu số của tỷ lệ trễ. Trung bình số phút trễ tính trên các chuyến đã trễ.
- Các trạng thái, cột chuyến bay và quan hệ tham chiếu được dùng lại từ thiết kế Airport Operations. Project tạo schema demo riêng `airport_etl`, thêm `SourceUpdatedAt`, bảng ETL và các bản ghi danh mục mẫu để chạy độc lập.

## 3. Kết quả mẫu và trình tự demo

Với cơ sở dữ liệu mới, file `flights.csv` có **16 dòng dữ liệu, 8 dòng hợp lệ, 2 dòng trùng và 6 dòng lỗi**. Chạy lại cùng file để xác nhận bảng `Flight` vẫn có 8 mã chuyến bay duy nhất.

| Ngày | Số chuyến | Đã cất cánh | Trễ | Trễ trung bình |
|---|---:|---:|---:|---:|
| 20/11/2026 | 5 | 3 | 2 | 20 phút |
| 21/11/2026 | 3 | 2 | 1 | 50 phút |

File `flights_update.csv` cập nhật giờ cất cánh cho ETL003 và ETL007. Sau cập nhật, số chuyến trễ lần lượt là **3** và **2**, số phút trễ trung bình là **20** và **40**. Khi chạy lại `flights.csv`, log `check` báo `newer_target_rows = 2` và MySQL giữ hai quan sát mới hơn. Với `flights_bad_schema.csv`, task `ingest` báo thiếu cột, các bước kế tiếp không nạp dữ liệu và log pipeline có trạng thái `FAILED`.

Xem [kịch bản demo và danh sách ảnh cần chụp](docs/DEMO.md). [Ảnh kiểm tra phần làm sạch](docs/evidence/offline_validation.png) ghi lại kết quả kiểm thử Python trên dữ liệu đi kèm project.

## 4. Kiểm thử

Kiểm thử phần đọc và làm sạch CSV bằng Python chuẩn. Chạy `py -3` thay cho `python` nếu máy Windows dùng Python Launcher.

```bash
python -m unittest discover -s tests -v
```

Sau khi Docker đã chạy và Airflow hiển thị DAG, có thể dùng lệnh dưới đây để thử toàn bộ luồng. Script bật DAG, chạy các file demo, kiểm tra kết quả trong MySQL và thử một lô lỗi có chủ đích. Dùng dữ liệu mẫu đi kèm project. Thêm `--port 8081` nếu đổi cổng, hoặc `--password` nếu đổi mật khẩu demo.

```bash
python scripts/smoke_test.py
```

## 5. Các file khi nộp

| Thành phần | Đường dẫn |
|---|---|
| Docker Compose và Airflow image | `docker-compose.yaml`, `Dockerfile` |
| DAG và logic ETL | `dags/airport_operations_dag.py`, `dags/airport_etl/` |
| Dữ liệu đầu vào | `data/` |
| Khởi tạo MySQL và truy vấn kiểm tra | `scripts/sql/init/01_schema.sql`, `scripts/sql/verify.sql` |
| Kiểm thử | `tests/`, `scripts/smoke_test.py` |
| Tài liệu demo và minh chứng | `docs/DEMO.md`, `docs/evidence/` |

Mở terminal trong project và chạy `git log --oneline` để xem lịch sử commit. Xem trạng thái bằng `git status --short`.

### Tài liệu kỹ thuật

- [Apache Airflow 2.11.2](https://airflow.apache.org/docs/apache-airflow/2.11.2/)
- [MySQL 8.4 – INSERT ON DUPLICATE KEY UPDATE](https://dev.mysql.com/doc/refman/8.4/en/insert-on-duplicate.html)
