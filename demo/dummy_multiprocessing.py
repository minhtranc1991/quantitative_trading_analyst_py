import multiprocessing
import time
import os
import signal

def dummy():
    while True:
        pass  # hoặc time.sleep(10) nếu muốn tiết kiệm CPU

if __name__ == '__main__':
    processes = []
    try:
        while True:
            p = multiprocessing.Process(target=dummy)
            p.start()
            processes.append(p)
            print(f"Số tiến trình đang chạy: {len(processes)}")
    except Exception as e:
        print("Không thể tạo thêm tiến trình:", e)
        print("Đang dừng tất cả tiến trình đã tạo...")
        for p in processes:
            try:
                p.terminate()
                p.join(timeout=1)
            except Exception as kill_err:
                print(f"Lỗi khi dừng tiến trình: {kill_err}")
        print("Đã dừng tất cả tiến trình.")
