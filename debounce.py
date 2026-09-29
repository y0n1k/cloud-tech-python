import threading
import time


class Debounce:
    def __init__(self, fn, delay_ms, leading=False, trailing=True):
        self.fn = fn
        self.delay = delay_ms / 1000.0  # Переводимо мілісекунди в секунди для таймера
        self.leading = leading
        self.trailing = trailing

        self._timer = None
        self._lock = threading.Lock()  # Захист від одночасного доступу з різних потоків
        self._called_after_leading = False

    def __call__(self, *args, **kwargs):
        with self._lock:
            # 1. Якщо таймер уже був запущений — скасовуємо його (відкладаємо виконання знову)
            if self._timer is not None:
                self._timer.cancel()

            is_first_call = (self._timer is None)

            # 2. Обробка опції leading (миттєвий перший виклик)
            if self.leading and is_first_call:
                self.fn(*args, **kwargs)
                self._called_after_leading = False
            else:
                self._called_after_leading = True

            # 3. Запускаємо новий таймер, який спрацює через delay
            self._timer = threading.Timer(self.delay, self._on_timeout, args=args, kwargs=kwargs)
            self._timer.start()

    def _on_timeout(self, *args, **kwargs):
        with self._lock:
            self._timer = None  # Скидаємо стан таймера
        # Обробка опції trailing (виклик після завершення серії подій)
        if self.trailing and self._called_after_leading:
            self.fn(*args, **kwargs)
        self._called_after_leading = False

    def dispose(self):
        """Скасовує таймер і очищує стан."""
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            self._called_after_leading = False
            print("[Dispose] Таймер примусово скасовано.")


# ==========================================
# БЛОК ТЕСТУВАННЯ
# ==========================================
if __name__ == "__main__":
    def print_msg(msg):
        # Виводимо поточний час, щоб було видно, КОЛИ саме спрацювала функція
        print(f"[{time.strftime('%S')}.{str(time.time()).split('.')[1][:2]}] 🟢 Виконано: {msg}")


    print("--- Початок тестування Debounce ---")

    # Тест 1: Стандартний Debounce (trailing=True, leading=False)
    print("\n[Тест 1] Серія швидких викликів (trailing=True). Очікуємо ЄДИНИЙ виклик в кінці.")
    debounced_fn = Debounce(print_msg, delay_ms=500, leading=False, trailing=True)

    for i in range(5):
        debounced_fn(f"Тест 1 (Спроба {i + 1})")
        print(f"  Клік {i + 1}...")
        time.sleep(0.1)  # Швидко "клікаємо" 5 разів поспіль

    time.sleep(1)  # Чекаємо завершення таймера (500мс)

    # Тест 2: Leading = True (Миттєвий перший + придушення наступних)
    print("\n[Тест 2] leading=True, trailing=False. Очікуємо ТІЛЬКИ миттєвий перший виклик.")
    debounced_leading = Debounce(print_msg, delay_ms=500, leading=True, trailing=False)

    for i in range(5):
        debounced_leading(f"Тест 2 (Спроба {i + 1})")
        print(f"  Клік {i + 1}...")
        time.sleep(0.1)

    time.sleep(1)

    # Тест 3: Комбінація Leading + Trailing
    print("\n[Тест 3] leading=True, trailing=True. Очікуємо ПЕРШИЙ миттєвий + ОСТАННІЙ після паузи.")
    debounced_both = Debounce(print_msg, delay_ms=500, leading=True, trailing=True)

    for i in range(5):
        debounced_both(f"Тест 3 (Спроба {i + 1})")
        print(f"  Клік {i + 1}...")
        time.sleep(0.1)

    time.sleep(1)

    # Тест 4: Dispose (Скасування таймера)
    print("\n[Тест 4] Перевірка dispose(). Робимо клік і одразу скасовуємо.")
    debounced_dispose = Debounce(print_msg, delay_ms=500, leading=False, trailing=True)

    debounced_dispose("Тест 4 (Спроба, що має скасуватись)")
    print("  Клік зроблено. Миттєво викликаємо dispose()...")
    debounced_dispose.dispose()

    time.sleep(1)
    print("  Час вийшов. Якщо не було повідомлення 'Виконано' — тест успішний.")