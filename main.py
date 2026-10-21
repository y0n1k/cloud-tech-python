import threading
import time


class FutureTimeoutError(Exception):
    pass


class Future:
    def __init__(self):
        self._state = "PENDING"  # Стани: PENDING, FULFILLED, REJECTED
        self._value = None
        self._error = None

        self._event = threading.Event()
        self._then_callbacks = []
        self._catch_callbacks = []
        self._lock = threading.Lock()

    def _resolve(self, value):
        """Встановлює Future у стан FULFILLED. Тільки один раз!"""
        with self._lock:
            if self._state != "PENDING":
                return  # Ігноруємо, якщо вже завершено
            self._state = "FULFILLED"
            self._value = value

        self._event.set()  # Розблоковуємо всі виклики get()
        for cb in self._then_callbacks:
            cb(self._value)

    def _reject(self, error):
        """Встановлює Future у стан REJECTED. Тільки один раз!"""
        with self._lock:
            if self._state != "PENDING":
                return
            self._state = "REJECTED"
            self._error = error

        self._event.set()
        for cb in self._catch_callbacks:
            cb(self._error)

    def then(self, callback):
        """Додає колбек для успішного виконання."""
        with self._lock:
            if self._state == "FULFILLED":
                callback(self._value)
            elif self._state == "PENDING":
                self._then_callbacks.append(callback)
        return self  # Повертаємо self для чейнінгу: f.then().catch()

    def catch(self, callback):
        """Додає колбек для помилки."""
        with self._lock:
            if self._state == "REJECTED":
                callback(self._error)
            elif self._state == "PENDING":
                self._catch_callbacks.append(callback)
        return self

    def get(self, timeout=None):
        """Синхронно чекає на результат, блокуючи поточний потік."""
        if not self._event.wait(timeout):
            raise FutureTimeoutError(f"Тайм-аут очікування Future ({timeout} сек)")

        if self._state == "REJECTED":
            raise self._error
        return self._value

    @staticmethod
    def submit(fn, *args, **kwargs):
        """Фабричний метод: запускає fn у фоні та повертає Future."""
        future = Future()

        def worker():
            try:
                result = fn(*args, **kwargs)
                future._resolve(result)
            except Exception as e:
                future._reject(e)

        threading.Thread(target=worker, daemon=True).start()
        return future

    @staticmethod
    def all(futures):
        """Комбінатор: чекає завершення ВСІХ Future."""
        result_future = Future()

        def worker():
            results = []
            try:
                # Чекаємо на кожен future по черзі
                for f in futures:
                    results.append(f.get())
                result_future._resolve(results)
            except Exception as e:
                # Якщо хоч один впав, відхиляємо весь all
                result_future._reject(e)

        threading.Thread(target=worker, daemon=True).start()
        return result_future

    @staticmethod
    def race(futures):
        """Комбінатор: бере результат ПЕРШОГО завершеного Future."""
        result_future = Future()
        for f in futures:
            f.then(lambda val: result_future._resolve(val))
            f.catch(lambda err: result_future._reject(err))
        return result_future


# ==========================================
# БЛОК ТЕСТУВАННЯ
# ==========================================
if __name__ == "__main__":
    def log(msg):
        print(f"[{time.strftime('%S')}.{str(time.time()).split('.')[1][:2]}] {msg}")


    print("--- Початок тестування Future / Promise ---")

    # [Тест 1] Успішне виконання та отримання значення
    print("\n[Тест 1] Успішна операція:")


    def slow_success():
        time.sleep(0.5)
        return "Дані з БД"


    f1 = Future.submit(slow_success)
    f1.then(lambda val: log(f"Колбек then() отримав: {val}"))
    log("Запит відправлено у фон. Чекаємо через get()...")
    res1 = f1.get()
    log(f"get() повернув: {res1}")

    # [Тест 2] Пропагація помилок
    print("\n[Тест 2] Обробка помилок:")


    def slow_fail():
        time.sleep(0.3)
        raise ValueError("Сервер недоступний")


    f2 = Future.submit(slow_fail)
    f2.catch(lambda err: log(f"Колбек catch() зловив: {err}"))
    try:
        f2.get()
    except Exception as e:
        log(f"get() викинув виключення: {type(e).__name__} - {e}")

    # [Тест 3] Комбінатор ALL (чекає на всі)
    print("\n[Тест 3] Комбінатор Future.all:")
    f_a = Future.submit(lambda: (time.sleep(0.2), "A")[1])
    f_b = Future.submit(lambda: (time.sleep(0.4), "B")[1])
    f_c = Future.submit(lambda: (time.sleep(0.1), "C")[1])

    f_all = Future.all([f_a, f_b, f_c])
    log("Чекаємо на завершення всіх 3 задач...")
    log(f"all() повернув масив: {f_all.get()}")  # Займе ~0.4 сек (найдовший)

    # [Тест 4] Комбінатор RACE (перемагає найшвидший)
    print("\n[Тест 4] Комбінатор Future.race:")
    f_slow = Future.submit(lambda: (time.sleep(0.5), "Повільний")[1])
    f_fast = Future.submit(lambda: (time.sleep(0.1), "Швидкий")[1])

    f_race = Future.race([f_slow, f_fast])
    log(f"race() повернув результат першого завершеного: {f_race.get()}")

    # [Тест 5] Тайм-аут у get()
    print("\n[Тест 5] Перевищення часу get(timeout):")
    f_very_slow = Future.submit(lambda: time.sleep(2))
    try:
        f_very_slow.get(timeout=0.5)
    except FutureTimeoutError as e:
        log(f"Зловлено тайм-аут: {e}")