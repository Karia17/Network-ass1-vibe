# -*- coding: utf-8 -*-
"""
StreapRous Flow & Reliability Buffer Module
Tệp tin: streaprous_socket/flow_buffer.py
Phụ trách: Nhóm Flow Control & Nhóm Reliability
Đặc tả kỹ thuật: Selective Repeat ARQ & Dynamic Receiver Window (rwnd)

Tệp tin này quản lý toàn bộ bộ đệm gửi/nhận, cửa sổ trượt Selective Repeat
và cơ chế điều khiển luồng chống tràn bộ đệm nhận (Flow Control).
Hỗ trợ cả Byte Sequence Number (chuẩn TCP) lẫn Packet Index.
"""

import time
from typing import Tuple, List, Optional

try:
    from . import constants
except ImportError:
    import constants


class SendBufferManager:
    """
    Quản lý bộ đệm gửi (Send Buffer) và phân hệ Truyền tin cậy Selective Repeat phía Gửi.
    
    Attributes:
        mss (int): Kích thước phân đoạn tối đa (bytes).
        N (int): Kích thước tối đa của cửa sổ gửi (số gói).
        rto (float): Thời gian chờ timeout cho từng gói (giây).
        snd_base (int): Byte Sequence Number nhỏ nhất chưa được xác nhận ACK.
        next_seq (int): Byte Sequence Number tiếp theo mà ứng dụng nạp vào bộ đệm.
        seq_to_send (int): Byte Sequence Number kế tiếp sẵn sàng để phát ra mạng.
        InFlight (int): Số lượng gói tin đã gửi ra mạng nhưng chưa nhận được ACK.
        rwnd (int): Cửa sổ nhận do đối tác quảng bá qua gói ACK gần nhất.
        send_buffer (dict): Lưu trữ dữ liệu các gói tin chưa được giải phóng.
        timers (dict): Quản lý thời điểm hết hạn timeout cho từng gói đang bay.
        out_of_order_tracker (dict): Đếm số gói cao hơn vượt mặt gói k chưa ACK.
    """

    def __init__(self, initial_seq: int = 0, mss: int = constants.MSS, win_size: int = constants.SND_WIN_SIZE, rto: float = constants.INITIAL_RTO):
        """Khởi tạo bộ đệm gửi cho kết nối mới."""
        self.mss = mss
        self.N = win_size
        self.rto = rto
        
        self.snd_base = initial_seq
        self.next_seq = initial_seq
        self.seq_to_send = initial_seq
        self.InFlight = 0
        self.rwnd = win_size
        self.dup_ack_count = 0
        
        self.send_buffer = {}           # seq -> {'payload': bytes, 'length': int, 'is_acked': bool, 'retries': int}
        self.timers = {}                # seq -> expire_timestamp
        self.out_of_order_tracker = {}  # unacked_seq -> count of higher ACKs

    def push_data(self, payload: bytes) -> int:
        """
        Tiếp nhận chuỗi byte từ tầng ứng dụng (run_conn_send), phân đoạn thành các gói MSS
        và nạp vào send_buffer.
        
        Args:
            payload (bytes): Dữ liệu thô từ tầng ứng dụng.
            
        Returns:
            int: Số lượng gói tin mới được nạp vào bộ đệm.
        """
        if not payload:
            return 0
            
        chunks = [payload[i:i + self.mss] for i in range(0, len(payload), self.mss)]
        count = 0
        for chunk in chunks:
            self.send_buffer[self.next_seq] = {
                'payload': chunk,
                'length': len(chunk),
                'is_acked': False,
                'retries': 0,
            }
            self.next_seq += len(chunk)
            count += 1
        return count

    def can_send(self, cwnd: int) -> bool:
        """
        Kiểm tra Cổng kiểm soát phát gói (Transmission Gate) 3 trong 1.
        Gói tin chỉ được phép gửi khi thỏa mãn đồng thời cả 3 điều kiện:
        1. Có dữ liệu chưa gửi trong buffer (seq_to_send < next_seq)
        2. Không vượt quá giới hạn bộ đệm gửi N (seq_to_send - snd_base < N * mss)
        3. Không vượt quá cửa sổ nhận của đối tác (InFlight + 1 <= rwnd)
        4. Không vượt quá cửa sổ nghẽn mạng (InFlight + 1 <= cwnd)
        
        Args:
            cwnd (int): Kích thước cửa sổ nghẽn hiện thời do CUBIC cung cấp (tính theo số gói).
            
        Returns:
            bool: True nếu được phép phát gói tiếp theo ra mạng.
        """
        has_unsent_data = self.seq_to_send < self.next_seq
        within_send_window = (self.seq_to_send - self.snd_base) < (self.N * self.mss)
        within_flow_window = (self.InFlight + 1) <= self.rwnd
        within_cong_window = (self.InFlight + 1) <= cwnd

        return has_unsent_data and within_send_window and within_flow_window and within_cong_window

    def get_next_packet_to_send(self) -> Optional[Tuple[int, bytes]]:
        """
        Trích xuất gói tin tiếp theo cần phát, bật timer và cập nhật InFlight.
        
        Returns:
            tuple (seq, payload) hoặc None nếu không có gói nào được phép gửi.
        """
        if self.seq_to_send >= self.next_seq:
            return None
            
        seq = self.seq_to_send
        pkt_info = self.send_buffer[seq]
        payload = pkt_info['payload']
        
        # Bật timer cho gói tin
        self.timers[seq] = time.time() + self.rto
        self.InFlight += 1
        self.seq_to_send += len(payload)
        
        return seq, payload

    def on_ack_received(self, ack_seq: int, rcv_rwnd: int) -> Tuple[bool, Optional[int]]:
        """
        Xử lý khi nhận được gói tin ACK từ bên nhận.
        
        Args:
            ack_seq (int): Byte Sequence Number được xác nhận.
            rcv_rwnd (int): Kích thước cửa sổ nhận mới nhất mà đối tác báo về.
            
        Returns:
            tuple (is_new_ack: bool, loss_seq: Optional[int]):
                - is_new_ack: True nếu đây là ACK của dữ liệu mới hợp lệ.
                - loss_seq: Số hiệu gói bị mất nếu phát hiện đủ 3 Out-of-order ACKs.
        """
        self.rwnd = max(0, rcv_rwnd)
        loss_detected_seq = None
        is_new_ack = False
        
        # 1. Kiểm tra Duplicate ACK (RFC 5681 Fast Retransmit)
        if ack_seq == self.snd_base and self.InFlight > 0:
            self.dup_ack_count += 1
            if self.dup_ack_count == constants.DUP_ACK_THRESHOLD:
                loss_detected_seq = self.snd_base
        elif ack_seq > self.snd_base:
            self.dup_ack_count = 0

        # 2. Đánh dấu các gói đã hoàn tất ACK
        # Theo chuẩn TCP: ack_seq là số hiệu byte tiếp theo bên nhận mong đợi.
        # Nghĩa là mọi gói có (seq + length <= ack_seq) đều đã được nhận an toàn.
        for seq, pkt_data in list(self.send_buffer.items()):
            if not pkt_data['is_acked']:
                if seq + pkt_data['length'] <= ack_seq:
                    pkt_data['is_acked'] = True
                    self.timers.pop(seq, None)
                    self.out_of_order_tracker.pop(seq, None)
                    self.InFlight = max(0, self.InFlight - 1)
                    is_new_ack = True
                elif seq < ack_seq:
                    # Gói k có byte nằm trước ack_seq nhưng chưa trọn vẹn
                    self.out_of_order_tracker[seq] = self.out_of_order_tracker.get(seq, 0) + 1
                    if self.out_of_order_tracker[seq] == constants.DUP_ACK_THRESHOLD:
                        loss_detected_seq = seq
                        
        # 3. Trượt cửa sổ snd_base và giải phóng bộ đệm an toàn
        while self.snd_base in self.send_buffer and self.send_buffer[self.snd_base]['is_acked']:
            pkt_len = self.send_buffer[self.snd_base]['length']
            del self.send_buffer[self.snd_base]
            self.timers.pop(self.snd_base, None)
            self.out_of_order_tracker.pop(self.snd_base, None)
            self.snd_base += pkt_len
            self.dup_ack_count = 0
            
        return is_new_ack, loss_detected_seq

    def check_timeouts(self) -> List[Tuple[int, bytes]]:
        """
        Quét danh sách các gói tin đang bay để phát hiện gói bị hết hạn (Timeout).
        
        Returns:
            list[tuple(seq, payload)]: Danh sách các gói cần truyền lại ngay lập tức.
        """
        now = time.time()
        timed_out_packets = []
        
        for seq, expire_time in list(self.timers.items()):
            if now >= expire_time:
                # Gói bị timeout
                self.send_buffer[seq]['retries'] += 1
                # Reset timer với Exponential Backoff nhẹ
                self.timers[seq] = now + (self.rto * 1.5)
                timed_out_packets.append((seq, self.send_buffer[seq]['payload']))
                
        return timed_out_packets

    def get_inflight(self) -> int:
        """Trả về số lượng gói đang bay chưa được ACK."""
        return self.InFlight

    def has_unacked_data(self) -> bool:
        """Kiểm tra xem còn dữ liệu nào chưa được ACK hoàn tất hay không."""
        return len(self.send_buffer) > 0


class ReceiveBufferManager:
    """
    Quản lý bộ đệm nhận (Receive Buffer) và phân hệ Điều khiển luồng (Flow Control) phía Nhận.
    
    Attributes:
        M (int): Dung lượng tối đa của bộ đệm nhận (số gói tin).
        mss (int): Kích thước tối đa mỗi segment.
        rcv_nxt (int): Byte Sequence Number tiếp theo mong đợi gửi ACK ra mạng.
        read_ptr (int): Byte Sequence Number tiếp theo chờ ứng dụng đọc (read_stream).
        occupied (int): Số lượng gói tin hiện đang lưu trong bộ đệm chờ đọc.
        rcv_buffer (dict): Lưu trữ dữ liệu các gói tin nhận được {seq: payload_bytes}.
    """

    def __init__(self, initial_seq: int = 0, win_size: int = constants.RCV_WIN_SIZE, mss: int = constants.MSS):
        """Khởi tạo bộ đệm nhận cho kết nối mới."""
        self.M = win_size
        self.mss = mss
        self.rcv_nxt = initial_seq
        self.read_ptr = initial_seq
        self.occupied = 0
        self.rcv_buffer = {}

    @property
    def rcv_base(self) -> int:
        """Thuộc tính tương thích ngược cho read_ptr."""
        return self.read_ptr

    def get_rwnd(self) -> int:
        """
        Tính toán kích thước cửa sổ nhận còn trống hiện thời (tính theo số gói).
        rwnd = M - occupied
        """
        return max(0, self.M - self.occupied)

    def on_packet_arrived(self, seq: int, payload: bytes) -> Tuple[str, int]:
        """
        Xử lý khi một gói tin DATA(seq) cập bến từ card mạng.
        
        Args:
            seq (int): Byte Sequence Number của gói tin nhận được.
            payload (bytes): Dữ liệu tải của gói tin.
            
        Returns:
            tuple (action: str, current_rwnd: int):
                - action: 'ACCEPT', 'DUPLICATE', 'OUT_OF_WINDOW', 'BUFFER_FULL'.
                - current_rwnd: Cửa sổ nhận sau khi xử lý gói tin.
        """
        # 1. Kiểm tra gói cũ hoặc trùng lặp
        if seq < self.read_ptr or seq in self.rcv_buffer:
            return constants.ACTION_DUPLICATE, self.get_rwnd()

        # 2. Kiểm tra tràn bộ đệm nhận (Buffer Full)
        if self.occupied >= self.M:
            return constants.ACTION_BUFFER_FULL, 0

        # 3. Kiểm tra gói rơi ra ngoài phạm vi cửa sổ nhận (Out of Window)
        if seq >= (self.read_ptr + self.M * self.mss):
            return constants.ACTION_OUT_OF_WINDOW, self.get_rwnd()

        # 4. Gói tin hợp lệ nằm trong cửa sổ: Lưu vào buffer
        self.rcv_buffer[seq] = payload
        self.occupied += 1
        
        # Cập nhật rcv_nxt (tiến qua các segment liên tục đã nhận)
        while self.rcv_nxt in self.rcv_buffer:
            self.rcv_nxt += len(self.rcv_buffer[self.rcv_nxt])
        
        return constants.ACTION_ACCEPT, self.get_rwnd()

    def read_stream(self) -> bytes:
        """
        Rút tuần tự các gói tin liên tục từ read_ptr để bàn giao cho tầng ứng dụng (run_conn_recv).
        Mỗi gói được rút ra sẽ giải phóng bộ đệm (occupied--) giúp rwnd tăng trở lại.
        
        Returns:
            bytes: Chuỗi byte liên tục sẵn sàng giao cho HTTP Adapter hoặc Chat Engine.
        """
        stream_bytes = b""
        
        while self.read_ptr in self.rcv_buffer:
            payload = self.rcv_buffer.pop(self.read_ptr)
            stream_bytes += payload
            self.read_ptr += len(payload)
            self.occupied = max(0, self.occupied - 1)
            
        return stream_bytes

    def get_next_ack(self) -> int:
        """Trả về Sequence Number tiếp theo mà bên nhận mong đợi nhận được (Ack Number)."""
        return self.rcv_nxt
