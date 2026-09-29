import time
import concurrent.futures


class CircuitBreaker:
    def __init__(self, failure_threshold, half_open_max_calls, open_state_duration, timeout_per_call):
        # Конфігураційні параметри
        self.failure_threshold = failure_threshold
        self.half_open_max_calls = half_open_max_calls
        self.open_state_duration = open_state_duration
        self.timeout_per_call = timeout_per_call

        # Внутрішній стан
        self._state = "CLOSED"
        self._failure_count = 0
        self._last_open_time = 0

        self._half_open_calls_made = 0
        self._half_open_success_count = 0

    def state(self):
        """Повертає поточний стан. Якщо час очікування в стані OPEN минув, переводить у HALF_OPEN."""
        if self._state == "OPEN":
            # Перевіряємо, чи пройшов достатній час для переходу в HALF_OPEN
            if (time.time() - self._last_open_time) >= self.open_state_duration:
                self._state = "HALF_OPEN"
                self._half_open_calls_made = 0
                self._half_open_success_count = 0
        return self._state

    def call(self, fn, *args, **kwargs):
        """Виконує передану функцію fn з урахуванням стану розмикача та тайм-аутів."""
        current_state = self.state()

        if current_state == "OPEN":
            raise RuntimeError("Circuit Breaker is OPEN. Виклик заблоковано.")

        if current_state == "HALF_OPEN":
            if self._half_open_calls_made >= self.half_open_max_calls:
                raise RuntimeError("Circuit Breaker is HALF_OPEN. Ліміт тестових викликів вичерпано.")
            self._half_open_calls_made += 1

        try:
            # Виконання функції з тайм-аутом
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(fn, *args, **kwargs)
                result = future.result(timeout=self.timeout_per_call)

            # Якщо виклик успішний
            self._on_success()
            return result

        except Exception as e:  # Ловимо і тайм-аути, і помилки самої функції
            self._on_failure()
            # Прокидаємо помилку далі, щоб програма знала, що виклик не вдався
            raise e

    def _on_success(self):
        if self._state == "CLOSED":
            self._failure_count = 0  # Скидаємо лічильник помилок
        elif self._state == "HALF_OPEN":
            self._half_open_success_count += 1
            # Якщо всі тестові виклики успішні, повертаємось у CLOSED
            if self._half_open_success_count >= self.half_open_max_calls:
                self._state = "CLOSED"
                self._failure_count = 0

    def _on_failure(self):
        if self._state == "CLOSED":
            self._failure_count += 1
            if self._failure_count >= self.failure_threshold:
                self._trip_to_open()
        elif self._state == "HALF_OPEN":
            # Будь-яка помилка в HALF_OPEN миттєво повертає нас у OPEN
            self._trip_to_open()

    def _trip_to_open(self):
        self._state = "OPEN"
        self._last_open_time = time.time()


# ==========================================
# БЛОК ТЕСТУВАННЯ (МОКИ ТА СЦЕНАРІЇ)
# ==========================================

# Мокові (фейкові) функції для симуляції різних відповідей сервісу
def mock_success():
    return "200 OK"


def mock_fail():
    raise ValueError("Internal Server Error 500")


def mock_hang():
    time.sleep(3)  # Симулюємо зависання сервісу
    return "Too late"


if __name__ == "__main__":
    print("--- Початок тестування Circuit Breaker ---")

    cb = CircuitBreaker(
        failure_threshold=2,
        half_open_max_calls=2,
        open_state_duration=2,  # 2 секунди для швидкого тесту
        timeout_per_call=1
    )

    # 1. Перехід у Open після N фейлів
    print("\n[Тест 1] Перехід у OPEN після помилок")
    try:
        cb.call(mock_fail)
    except Exception as e:
        print(f"Виклик 1: {type(e).__name__}")

    try:
        cb.call(mock_fail)
    except Exception as e:
        print(f"Виклик 2: {type(e).__name__}")

    print(f"Стан після 2 помилок: {cb.state()} (Очікується: OPEN)")

    # 2. Блокування викликів у Open
    print("\n[Тест 2] Блокування виклику в стані OPEN")
    try:
        cb.call(mock_success)
    except Exception as e:
        print(f"Спроба виклику заблокована: {e}")

    # 3. Перехід у HalfOpen та успішне відновлення (Closed)
    print("\n[Тест 3] Поведінка HalfOpen (Успішне відновлення)")
    print("Чекаємо 2.1 сек, щоб розмикач перейшов у HALF_OPEN...")
    time.sleep(2.1)
    print(f"Стан після очікування: {cb.state()} (Очікується: HALF_OPEN)")

    print("Робимо 2 успішні тестові виклики:")
    print("Тест-виклик 1:", cb.call(mock_success))
    print("Тест-виклик 2:", cb.call(mock_success))
    print(f"Кінцевий стан: {cb.state()} (Очікується: CLOSED)")

    # 4. Перехід назад у Open при збої в HalfOpen
    print("\n[Тест 4] Збій під час HalfOpen")
    # Штучно ламаємо знову
    try:
        cb.call(mock_fail)
    except:
        pass
    try:
        cb.call(mock_fail)
    except:
        pass

    time.sleep(2.1)  # Чекаємо до HALF_OPEN
    print(f"Стан: {cb.state()} (Очікується: HALF_OPEN)")
    try:
        cb.call(mock_fail)  # Перший же тестовий виклик падає
    except:
        print("Тест-виклик впав.")

    print(f"Стан після падіння: {cb.state()} (Очікується: OPEN)")

    # 5. Тестування тайм-ауту
    print("\n[Тест 5] Врахування тайм-ауту")
    cb = CircuitBreaker(failure_threshold=1, half_open_max_calls=1, open_state_duration=2, timeout_per_call=1)
    print(f"Стан до зависання: {cb.state()}")
    try:
        print("Викликаємо функцію, що зависає на 3 сек (тайм-аут 1 сек)...")
        cb.call(mock_hang)
    except Exception as e:
        print(f"Виклик перервано: {type(e).__name__}")

    print(f"Стан після тайм-ауту: {cb.state()} (Очікується: OPEN)")