import time
import threading
from collections import deque


class Throttle:
    def __init__(self, fn, max_calls, interval_ms, mode="drop", trailing=False):

        self.fn = fn
        self.max_calls = max_calls
        self.interval = interval_ms / 1000.0
        self.mode = mode
        self.trailing = trailing

        self.call_times = []  # Історія викликів у поточному вікні
        self.lock = threading.Lock()

        self.queue = deque()
        self._timer = None
        self._trailing_args = None

    def _clean_old_calls(self, current_time):
        """Видаляє з історії виклики, які вже вийшли за межі часового вікна."""
        self.call_times = [t for t in self.call_times if current_time - t < self.interval]

    def __call__(self, *args, **kwargs):
        now = time.time()
        with self.lock:
            self._clean_old_calls(now)

            # Якщо є вільний "слот" (Leading Edge)
            if len(self.call_times) < self.max_calls:
                self.call_times.append(now)
                self.fn(*args, **kwargs)
            else:
                # Ліміт вичерпано
                if self.mode == "queue":
                    # Зберігаємо аргументи і ставимо в чергу
                    self.queue.append((args, kwargs))
                    self._schedule_next_process()
                elif self.mode == "drop":
                    # Просто ігноруємо, але якщо увімкнено trailing - запам'ятовуємо цей виклик
                    if self.trailing:
                        self._trailing_args = (args, kwargs)
                        self._schedule_next_process()

    def _schedule_next_process(self):
        """Планує запуск таймера, коли звільниться найближче місце у вікні."""
        if self._timer is None:
            # Час очікування = Інтервал мінус (Скільки часу пройшло з найстарішого виклику)
            wait_time = self.interval - (time.time() - self.call_times[0])
            self._timer = threading.Timer(wait_time, self._process_delayed)
            self._timer.start()

    def _process_delayed(self):
        """Обробляє чергу або trailing-виклик, коли звільняється місце."""
        with self.lock:
            self._timer = None
            now = time.time()
            self._clean_old_calls(now)

            if self.mode == "queue":
                # Виконуємо стільки, скільки дозволяє ліміт, поки черга не пуста
                while self.queue and len(self.call_times) < self.max_calls:
                    args, kwargs = self.queue.popleft()
                    self.call_times.append(time.time())
                    self.fn(*args, **kwargs)

                # Якщо черга ще не порожня, знову запускаємо таймер
                if self.queue:
                    self._schedule_next_process()

            elif self.mode == "drop" and self.trailing and self._trailing_args:
                # Виконуємо запам'ятований останній виклик
                args, kwargs = self._trailing_args
                self._trailing_args = None
                self.call_times.append(time.time())
                self.fn(*args, **kwargs)


# ==========================================
# БЛОК ТЕСТУВАННЯ
# ==========================================
if __name__ == "__main__":
    def print_msg(msg):
        # Виводимо поточну секунду та мілісекунди
        print(f"[{time.strftime('%S')}.{str(time.time()).split('.')[1][:2]}] * {msg}")


    print("--- Початок тестування Throttle ---")

    # [Тест 1] Режим Drop: 10 викликів, ліміт 3/сек
    print("\n[Тест 1] Drop (Відсікання). Ліміт: 3 виклики на секунду.")
    throttle_drop = Throttle(print_msg, max_calls=3, interval_ms=1000, mode="drop")

    for i in range(10):
        throttle_drop(f"Виклик {i + 1} (Drop)")
        time.sleep(0.05)  # 10 викликів пройдуть за пів секунди

    print("Чекаємо 1.5 сек...")
    time.sleep(1.5)

    # [Тест 2] Режим Drop + Trailing
    print("\n[Тест 2] Drop + Trailing. Ліміт: 2 виклики на секунду. Останній відкинутий має виконатися в кінці.")
    throttle_trailing = Throttle(print_msg, max_calls=2, interval_ms=1000, mode="drop", trailing=True)

    for i in range(5):
        throttle_trailing(f"Виклик {i + 1} (Trailing)")
        time.sleep(0.05)

    print("Чекаємо 1.5 сек (в цей час має спрацювати trailing-виклик)...")
    time.sleep(1.5)

    # [Тест 3] Режим Queue (Черга)
    print("\n[Тест 3] Queue (Черга). Ліміт: 2 виклики на секунду. Всі 5 викликів мають виконатися із затримками.")
    throttle_queue = Throttle(print_msg, max_calls=2, interval_ms=1000, mode="queue")

    for i in range(5):
        throttle_queue(f"Виклик {i + 1} (Queue)")
        time.sleep(0.05)  # Робимо 5 швидких викликів

    print("Всі запити надіслані. Чекаємо розгрібання черги (близько 2-3 сек)...")
    time.sleep(3.5)