import time
import threading
import concurrent.futures


# Власна помилка для відстеження тайм-аутів
class TimeoutException(Exception):
    pass


class Timeout:
    def __init__(self, timeout_ms):
        self.timeout_sec = timeout_ms / 1000.0

    def withTimeout(self, fn, *args, **kwargs):
        """
        Виконує функцію fn. Якщо виконання перевищує timeout_sec,
        викидає TimeoutException.
        """
        # Створюємо пул потоків
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = executor.submit(fn, *args, **kwargs)

        try:
            # Чекаємо результат вказаний час
            return future.result(timeout=self.timeout_sec)

        except concurrent.futures.TimeoutError:
            # Якщо час вийшов, генеруємо нашу помилку
            raise TimeoutException(f"Тайм-аут! Операція перервана ({self.timeout_sec} сек).")

        finally:
            executor.shutdown(wait=False)


# ==========================================
# БЛОК ТЕСТУВАННЯ (МОКИ ТА СЦЕНАРІЇ)
# ==========================================
if __name__ == "__main__":
    print("--- Початок тестування Timeout ---")

    # [Тест 1] Успішна операція до тайм-ауту
    print("\n[Тест 1] Успішна операція (встигає до тайм-ауту):")


    def fast_operation():
        time.sleep(0.2)  # Швидка відповідь
        return "200 OK"


    timeout_500 = Timeout(timeout_ms=500)
    try:
        result = timeout_500.withTimeout(fast_operation)
        print(f"  Результат: {result}")
    except Exception as e:
        print(f"  Помилка: {e}")

    # [Тест 2] Перевищення часу + Відміна (Cancellation)
    print("\n[Тест 2] Перевищення тайм-ауту + Відміна побічних ефектів:")

    # Використовуємо threading.Event як сигнал для відміни "важкої" роботи бази даних
    cancel_signal = threading.Event()


    def slow_db_transaction(cancel_event):
        print("  [Сервер БД] Починаю запис 1 мільйона рядків...")
        for i in range(10):
            # Перевіряємо кожну мілісекунду, чи не скасував клієнт запит
            if cancel_event.is_set():
                print("  [Сервер БД] Отримано сигнал скасування! Відкат транзакції (Rollback).")
                return "Відмінено"
            time.sleep(0.1)  # Симуляція важкої роботи
        return "Дані записано"


    try:
        # Передаємо cancel_signal у функцію
        timeout_500.withTimeout(slow_db_transaction, cancel_signal)
    except TimeoutException as e:
        print(f"  Клієнт: {e}")
        # Одразу після тайм-ауту надсилаємо серверу сигнал "СТОП"
        cancel_signal.set()
        # Даємо серверу мікросекунду на вивід логу про відкат транзакції
        time.sleep(5)

        # [Тест 3] Комбінування з Retry
    print("\n[Тест 3] Комбінування з Retry (Запобігання безконтрольному очікуванню):")


    def hanging_network_call():
        time.sleep(5)  # Сервіс "мертво" завис на 5 секунд
        return "Ніколи не виконається"


    max_attempts = 3
    timeout_300 = Timeout(timeout_ms=300)  # Тайм-аут 0.3 сек
    start_time = time.time()

    print(f"  Робимо {max_attempts} спроби достукатися до завислого сервісу...")
    for attempt in range(max_attempts):
        try:
            # Обертаємо завислий запит у тайм-аут
            timeout_300.withTimeout(hanging_network_call)
        except TimeoutException:
            print(f"  Спроба {attempt + 1} впала по тайм-ауту.")

    total_time = time.time() - start_time
    # Якби не Timeout, 3 спроби по 5 сек зайняли б 15 секунд!
    print(f"  Усі спроби завершено. Загальний час очікування: {total_time:.2f} сек.")