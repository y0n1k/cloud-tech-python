import asyncio
import time
from typing import Any, Optional


class Result:
    """Обгортка Result<T, E> для передачі даних або помилок без зупинки вихідного потоку."""

    def __init__(self, value: Any = None, error: Optional[Exception] = None):
        self.value = value
        self.error = error

    @property
    def is_ok(self) -> bool:
        return self.error is None

    def __repr__(self):
        if self.is_ok:
            return f"Result(Ok: {self.value})"
        return f"Result(Err: {type(self.error).__name__} - {self.error})"


# Спеціальний маркер для позначення кінця потоку
END_OF_STREAM = object()


async def fan_in(inputs: list[asyncio.Queue]) -> asyncio.Queue:
    """
    Мультиплексор (Fan-In): приймає список вхідних асинхронних черг (потоків)
    і зливає їх у єдину вихідну чергу.
    """
    output_queue = asyncio.Queue()

    async def worker(input_q: asyncio.Queue, stream_id: int):
        """Воркер для зчитування даних з одного конкретного вхідного потоку."""
        while True:
            try:
                item = await input_q.get()

                # Якщо отримано сигнал завершення даного вхідного потоку
                if item is END_OF_STREAM:
                    input_q.task_done()
                    break

                # Якщо елемент — це помилка, обгортаємо її в Result(error=...)
                if isinstance(item, Exception):
                    await output_queue.put(Result(error=item))
                else:
                    await output_queue.put(Result(value=item))

                input_q.task_done()

            except Exception as e:
                # Обробка несподіваних помилок зчитування
                await output_queue.put(Result(error=e))

    async def monitor():
        """Фонове завдання, яке чекає на закриття УСІХ вхідних потоків."""
        # Запускаємо воркер для кожного вхідного потоку
        workers = [asyncio.create_task(worker(q, i)) for i, q in enumerate(inputs)]

        # Чекаємо завершення всіх воркерів
        await asyncio.gather(*workers)

        # Лише коли ВСІ входи закрилися, сигналізуємо про закриття вихідного потоку
        await output_queue.put(END_OF_STREAM)

    # Запускаємо монітор у фоновому режимі
    asyncio.create_task(monitor())

    return output_queue


# ==========================================
# БЛОК ТЕСТУВАННЯ
# ==========================================

async def main():
    print("--- Початок тестування Fan-In (Мультиплексор) ---")

    # Створюємо 3 вхідні потоки (черги)
    stream_a = asyncio.Queue()
    stream_b = asyncio.Queue()
    stream_c = asyncio.Queue()

    # [Тест 1] Злиття кількох потоків + Збереження порядку всередині кожного потоку
    print("\n[Тест 1 та 3] Злиття 3 потоків + обробка помилки в одному з них:")

    # Наповнюємо Потік A (Повільний)
    async def produce_a():
        await asyncio.sleep(0.1)
        await stream_a.put("A1")
        await asyncio.sleep(0.2)
        await stream_a.put("A2")
        await stream_a.put(END_OF_STREAM)

    # Наповнюємо Потік B (Швидкий + містить помилку)
    async def produce_b():
        await stream_b.put("B1")
        # Помилка в Потоці B не повинна зупинити A і C!
        await stream_b.put(ValueError("Збій датчика у Потоці B"))
        await stream_b.put("B2")
        await stream_b.put(END_OF_STREAM)

    # Наповнюємо Потік C (Середній)
    async def produce_c():
        await asyncio.sleep(0.05)
        await stream_c.put("C1")
        await stream_c.put(END_OF_STREAM)

    # Запускаємо мультиплексор Fan-In
    out_queue = await fan_in([stream_a, stream_b, stream_c])

    # Запускаємо генератори даних паралельно
    asyncio.create_task(produce_a())
    asyncio.create_task(produce_b())
    asyncio.create_task(produce_c())

    # Зчитуємо об'єднаний потік даних
    received_items = []
    start_time = time.time()

    while True:
        res = await out_queue.get()
        if res is END_OF_STREAM:
            print("\n[Тест 2] Усі вхідні потоки завершено. Вихідний потік закрито.")
            break

        received_items.append(res)
        elapsed = time.time() - start_time
        print(f"  [{elapsed:.2f}s] Отримано елемент: {res}")

    print("\nПідсумок вичитаних даних:")
    for item in received_items:
        print(f"  - {item}")


if __name__ == "__main__":
    asyncio.run(main())