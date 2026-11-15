# -*- coding: utf-8 -*-
"""
StreapRous Transport Protocol Constants
Module: streaprous_socket.constants

Tập tin định nghĩa các hằng số tiêu chuẩn cho toàn bộ Tầng Giao vận (Transport Layer)
bao gồm các phân hệ: Truyền tin cậy (Selective Repeat ARQ), Kiểm soát luồng (Flow Control),
và Kiểm soát nghẽn (CUBIC RFC 8312).

Mọi module (cubic.py, flow_buffer.py, streaprous_raw_connection.py) đều tham chiếu
các hằng số từ tệp này để đảm bảo tính đồng bộ và tránh magic numbers.
"""

# ==============================================================================
# 1. KÍCH THƯỚC PHÂN ĐOẠN VÀ BỘ ĐỆM (SEGMENT & BUFFER SIZES)
# ==============================================================================

# Maximum Segment Size (bytes): Dung lượng tải tối đa của một gói tin DATA.
# - MTU Ethernet chuẩn = 1500 bytes. Trừ IP header (20B) và TCP header (20B) = 1460B.
# - Chọn 1024 bytes (1 KB chẵn) để tuyệt đối tránh phân mảnh IP (IP Fragmentation)
#   khi chạy qua mạng máy ảo/VPN (MTU có thể bị hạ xuống 1400-1492B).
# - Tạo mật độ gói tin lý tưởng cho các tệp thử nghiệm trong bài tập lớn.
MSS = 1024

# Kích thước tối đa của bộ đệm gửi (Send Window Size - N tính theo số gói tin).
# - Tổng dung lượng bộ đệm gửi = SND_WIN_SIZE * MSS = 32 * 1024 = 32 KB.
# - Tính toán theo Định luật Tích số Băng thông - Độ trễ (BDP = 15 Mbps * 15 ms ~ 28 KB).
# - Vừa đủ để lấp đầy đường truyền mạng ảo mà không làm quá tải CPU/GIL của Python.
SND_WIN_SIZE = 32

# Kích thước tối đa của bộ đệm nhận (Receive Window Size - M tính theo số gói tin).
# - Tổng dung lượng bộ đệm nhận = RCV_WIN_SIZE * MSS = 32 KB.
# - Dùng để giới hạn dung lượng lưu trữ các gói đến lệch thứ tự (out-of-order)
#   và tính toán cửa sổ nhận: rwnd = RCV_WIN_SIZE - occupied.
RCV_WIN_SIZE = 32


# ==============================================================================
# 2. THAM SỐ KIỂM SOÁT NGHẼN CUBIC (RFC 8312 & RFC 6928)
# ==============================================================================

# Cửa sổ nghẽn ban đầu (Initial Window - IW tính theo số gói MSS).
# - Chuẩn hóa theo RFC 6928 (Google IW10) và RFC 8312.
# - Cho phép gửi hầu hết các truy vấn/phản hồi Web HTTP (dưới 10 KB) chỉ trong 1 RTT.
INIT_CWND = 10.0

# Hằng số tỷ lệ co dãn đường cong CUBIC (CUBIC Scaling Factor C).
# - Quy định theo RFC 8312 (Mục 5.1).
# - Giá trị tối ưu hóa thực nghiệm đạt cân bằng giữa tốc độ mở rộng băng thông
#   và tính thân thiện với các luồng TCP Reno (TCP-friendliness).
CUBIC_C = 0.4

# Hệ số nhân giảm cửa sổ khi phát hiện mất gói (CUBIC Multiplicative Decrease Factor beta).
# - Quy định theo RFC 8312: ssthresh = CUBIC_BETA * cwnd.
# - Giữ lại 70% cửa sổ khi nghẽn (thay vì cắt giảm 50% sâu như Reno), giúp thông lượng
#   ổn định, không bị răng cưa sâu nhưng vẫn giải tỏa được hàng đợi router.
CUBIC_BETA = 0.7

# Các trạng thái của Cỗ máy trạng thái CUBIC
CC_STATE_SLOW_START = "SLOW_START"          # Khởi động chậm (tăng cwnd + 1 mỗi ACK)
CC_STATE_CUBIC = "CUBIC"                    # Tránh nghẽn theo hàm bậc 3 W_cubic(t)
CC_STATE_FAST_RECOVERY = "FAST_RECOVERY"    # Phục hồi nhanh khi có 3 DupACK


# ==============================================================================
# 3. THAM SỐ THỜI GIAN VÀ ĐỘ TIN CẬY (TIMERS & RELIABILITY - RFC 6298 & RFC 5681)
# ==============================================================================

# Thời gian Timeout truyền lại ban đầu (Initial Retransmission Timeout tính bằng giây).
# - Quy định theo RFC 6298 khi chưa đo được mẫu RTT đầu tiên.
# - Mức 1.0 giây đảm bảo an toàn tuyệt đối, tránh truyền lại giả (Spurious Retransmission)
#   do trễ điều phối CPU trên môi trường máy ảo hóa (VM / WSL2).
INITIAL_RTO = 1.0

# Ngưỡng RTO tối thiểu (giây) để tránh timer quá gấp gáp
MIN_RTO = 0.2

# Ngưỡng RTO tối đa (giây) theo chuẩn RFC 6298
MAX_RTO = 60.0

# Ngưỡng số gói ACK lệch thứ tự (out-of-order) cao hơn để kết luận mất gói (Fast Retransmit).
# - Tiêu chuẩn vàng theo RFC 5681 và RFC 6675.
# - Lọc bỏ hơn 99.9% hiện tượng xáo trộn thứ tự gói (Packet Reordering) thông thường.
DUP_ACK_THRESHOLD = 3


# ==============================================================================
# 4. THAM SỐ ĐIỀU KHIỂN LUỒNG VÀ CHỐNG DEADLOCK (FLOW CONTROL HEURISTICS)
# ==============================================================================

# Chu kỳ phát gói tin cập nhật cửa sổ định kỳ (Window Update Interval tính bằng giây).
# - Được kích hoạt bởi Window-Update Timer phía Receiver khi cửa sổ rwnd mở lại từ 0.
# - Đảm bảo triệt tiêu nguy cơ Zero-Window Deadlock nếu gói mở cửa sổ ban đầu bị rơi.
# - Mức 200 ms tương đương độ trễ tức thời thông thường của tương tác mạng.
WIN_UPDATE_INTERVAL = 0.2

# Mã trạng thái tiếp nhận gói tin tại Receiver Buffer Manager
ACTION_ACCEPT = "ACCEPT"                    # Chấp nhận gói tin mới hợp lệ vào buffer
ACTION_DUPLICATE = "DUPLICATE"              # Gói tin cũ hoặc trùng lặp (chỉ gửi lại ACK)
ACTION_OUT_OF_WINDOW = "OUT_OF_WINDOW"      # Gói tin rơi ngoài phạm vi cửa sổ nhận (bỏ qua)
ACTION_BUFFER_FULL = "BUFFER_FULL"          # Bộ đệm nhận đã đầy occupied >= M (rwnd = 0)


# ==============================================================================
# 5. CỜ TCP HEADER VÀ GIAO THỨC MẠNG (TCP FLAGS & NETWORK DEFAULTS)
# ==============================================================================

# Các bit cờ trong trường Flags của TCP Header (8-bit)
TH_FIN = 0x01   # Cờ kết thúc kết nối (Finish)
TH_SYN = 0x02   # Cờ đồng bộ thiết lập kết nối (Synchronize)
TH_RST = 0x04   # Cờ đặt lại kết nối (Reset)
TH_PUSH = 0x08  # Cờ đẩy dữ liệu lên ứng dụng (Push)
TH_ACK = 0x10   # Cờ xác nhận dữ liệu hợp lệ (Acknowledgment)
TH_URG = 0x20   # Cờ dữ liệu khẩn cấp (Urgent)

# Địa chỉ IP và Cổng mặc định của Raw Socket Server
DEFAULT_LISTEN_IP = "0.0.0.0"
DEFAULT_LISTEN_PORT = 22239
DEFAULT_BACKEND_PORT = 9000
DEFAULT_PROXY_PORT = 8080
