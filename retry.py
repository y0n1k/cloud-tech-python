import time
import random


# Створюємо власні класи помилок для імітації різних відповідей сервера
class NetworkError(Exception): pass


class TimeoutError(Exception): pass


class DataValidationError(Exception): pass  # Цю помилку ми повторювати не будемо


class Retry:
    def __init__(self, max_attempts, strategy="constant", base_delay_ms=100, retry_on=None):
        self.max_attempts = max_attempts
        self.strategy = strategy
        self.base_delay_ms = base_delay_ms
        # Якщо список помилок не передано, повторюємо на будь-якій помилці (Exception)
        self.retry_on = retry_on if retry_on is not None else (Exception,)

    def _calculate_delay(self, attempt):
        """Обчислює паузу залежно від обраної стратегії."""
        if self.strategy == "constant":
            # Стала затримка: 100, 100, 100...
            delay_ms = self.base_delay_ms
        elif self.strategy == "exponential":
            # Експоненційна затримка: 100, 200, 400, 800...
            delay_ms = self.base_delay_ms * (2 ** attempt)
        elif self.strategy == "exponentialJitter":
            # Експоненційна + випадковий шум (щоб уникнути синхронних повторів від багатьох клієнтів)
            base_exp = self.base_delay_ms * (2 ** attempt)
            jitter = random.uniform(0, base_exp * 0.2)  # додаємо до 20% розкиду
            delay_ms = base_exp + jitter
        else:
            delay_ms = 0

        return delay_ms / 1000.0  # Повертаємо в секундах для time.sleep()

    def execute(self, fn, *args, **kwargs):
        """Виконує функцію з повторами."""
        for attempt in range(self.max_attempts):
            try:
                # Намагаємося виконати функцію
                return fn(*args, **kwargs)

            except Exception as e:
                # Перевіряємо, чи належить помилка до тих, на яких треба робити повтор
                if not isinstance(e, self.retry_on):
                    print(f"  [Фатально] Помилка {type(e).__name__} не підлягає повтору. Перериваємо.")
                    raise e

                # Якщо це була остання спроба
                if attempt == self.max_attempts - 1:
                    print(f"  [Провал] Вичерпано всі {self.max_attempts} спроб. Відмова.")
                    raise e

                # Рахуємо затримку і чекаємо
                delay = self._calculate_delay(attempt)
                print(f"  [Помилка {type(e).__name__}] Спроба {attempt + 1} невдала. Чекаємо {delay:.3f} сек...")
                time.sleep(delay)


# ==========================================
# БЛОК ТЕСТУВАННЯ (МОКИ)
# ==========================================

# Створюємо клас-мок, який дозволяє налаштувати, скільки разів він впаде перед успіхом
class MockService:
    def __init__(self, fails_before_success, error_to_raise=NetworkError):
        self.fails_before_success = fails_before_success
        self.error_to_raise = error_to_raise
        self.attempts = 0

    def call(self):
        self.attempts += 1
        if self.attempts <= self.fails_before_success:
            raise self.error_to_raise("Збій сервісу")
        return "200 OK (Успіх)"


if __name__ == "__main__":
    print("--- Початок тестування Retry ---")

    # [Тест 1] Успіх з 1-ї спроби
    print("\n[Тест 1] Успіх з першої спроби:")
    retry = Retry(max_attempts=3)
    mock = MockService(fails_before_success=0)
    result = retry.execute(mock.call)
    print(f"Результат: {result}")

    # [Тест 2] Успіх з n-тої спроби (Експоненційна стратегія)
    print("\n[Тест 2] Успіх з 3-ї спроби (Стратегія: exponential):")
    retry_exp = Retry(max_attempts=5, strategy="exponential", base_delay_ms=200)
    mock2 = MockService(fails_before_success=2, error_to_raise=TimeoutError)
    result2 = retry_exp.execute(mock2.call)
    print(f"Результат: {result2}")

    # [Тест 3] Стоп після maxAttempts (Стратегія: exponentialJitter)
    print("\n[Тест 3] Всі спроби вичерпано (Стратегія: exponentialJitter):")
    retry_jitter = Retry(max_attempts=4, strategy="exponentialJitter", base_delay_ms=100)
    mock3 = MockService(fails_before_success=10)  # Буде падати завжди
    start_time = time.time()
    try:
        retry_jitter.execute(mock3.call)
    except Exception as e:
        total_time = time.time() - start_time
        print(f"Викинуто виключення: {type(e).__name__}. Загальний час очікування: {total_time:.3f} сек.")

    # [Тест 4] Не повторюємо на non-retryable помилках
    print("\n[Тест 4] Переривання на non-retryable помилці (DataValidationError):")
    retry_selective = Retry(
        max_attempts=5,
        retry_on=(NetworkError, TimeoutError)  # Повторювати ТІЛЬКИ на цих двох
    )
    mock4 = MockService(fails_before_success=1, error_to_raise=DataValidationError)
    try:
        retry_selective.execute(mock4.call)
    except Exception as e:
        print(f"Викинуто виключення одразу: {type(e).__name__}")