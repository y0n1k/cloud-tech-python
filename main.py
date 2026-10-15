import asyncio
import time
from typing import Callable, Any

class FanOut:
    def __init__(self, process_fn: Callable, num_workers: int):
        self.process_fn = process_fn
        self.num_workers = num_workers

        self.task_queue = asyncio.Queue()     # Черга вхідних задач
        self.result_queue = asyncio.Queue()   # Черга для збору результатів (агрегатор)
        self.workers = []
        self._is_started = False

    async def start(self):
        """Запускає N паралельних воркерів у фоновому режимі."""
        if not self._is_started:
            self.workers = [
                asyncio.create_task(self._worker(worker_id=i + 1))
                for i in range(self.num_workers)
            ]
            self._is_started = True

    async def _worker(self, worker_id: int):
        """Внутрішній цикл воркера: бере задачу з черги, виконує та зберігає результат."""
        while True:
            task = await self.task_queue.get()

            # Маркер зупинки (Sentinel Value) для бепечного завершення (Graceful Shutdown)
            if task is None:
                self.task_queue.task_done()
                break

            try:
                # Підтримка як асинхронних, так і звичайних синхронних функцій
                if asyncio.iscoroutinefunction(self.process_fn):
                    result = await self.process_fn(task)
                else:
                    result = self.process_fn(task)

                await self.result_queue.put((task, result, None)) # (Задача, Результат, Помилка)
            except Exception as e:
                await self.result_queue.put((task, None, e))
            finally:
                self.task_queue.task_done()

    async def submit(self, task: Any):
        """Додає задачу в чергу на обробку."""
        await self.task_queue.put(task)

    async def shutdown(self):
        """
        Graceful Shutdown: чекає виконання всіх доданих задач,
        надсилає сигнал зупинки воркерам і коректно завершує роботу.
        """
        # 1. Чекаємо, поки всі задачі з task_queue будуть повністю оброблені
        await self.task_queue.join()

        # 2. Надсилаємо N маркерів None (по одному для кожного воркера)
        for _ in range(self.num_workers):
            await self.task_queue.put(None)

        # 3. Чекаємо офіційного завершення роботи всіх воркерів
        await asyncio.gather(*self.workers)

    async def get_results(self) -> list:
        """Повертає всі зібрані результати з черги агрегатора."""
        results = []
        while not self.result_queue.empty():
            results.append(await self.result_queue.get())
        return results


# ==========================================
# БЛОК ТЕСТУВАННЯ
# ==========================================

# Мокова "важка" функція обробки даних (наприклад, завантаження або конвертація відео)
async def slow_process_task(task_id: int):
    await asyncio.sleep(0.1) # Штучна затримка 100 мс на кожну задачу
    return f"Processed_{task_id}"

async def main():
    print("--- Початок тестування Fan-Out (Демультиплексор) ---")

    # [Тест 1] Перевірка паралельності (Прискорення з N воркерами)
    print("\n[Тест 1] Перевірка паралельності та швидкості:")
    num_tasks = 10

    # Сценарій A: 1 воркер
    fan_out_1 = FanOut(slow_process_task, num_workers=1)
    await fan_out_1.start()
    for i in range(num_tasks):
        await fan_out_1.submit(i)

    start_time = time.time()
    await fan_out_1.shutdown()
    time_1_worker = time.time() - start_time
    print(f"  Час обробки {num_tasks} задач з 1 воркером: {time_1_worker:.2f} сек.")

    # Сценарій B: 5 воркерів
    fan_out_5 = FanOut(slow_process_task, num_workers=5)
    await fan_out_5.start()
    for i in range(num_tasks):
        await fan_out_5.submit(i)

    start_time = time.time()
    await fan_out_5.shutdown()
    time_5_workers = time.time() - start_time
    print(f"  Час обробки {num_tasks} задач з 5 воркерами: {time_5_workers:.2f} сек.")
    print(f"  Прискорення: у {time_1_worker / time_5_workers:.1f} разів!")

    # [Тест 2 & 3] Безпека даних (відсутність подвійної обробки) та Graceful Shutdown
    print("\n[Тест 2 & 3] Безпека даних та Graceful Shutdown:")
    fan_out_test = FanOut(slow_process_task, num_workers=3)
    await fan_out_test.start()

    tasks_to_send = list(range(100, 120)) # 20 задач
    for t in tasks_to_send:
        await fan_out_test.submit(t)

    # Завершуємо роботу (метод чекає завершення всіх 20 задач)
    await fan_out_test.shutdown()

    results = await fan_out_test.get_results()
    processed_tasks = [r[0] for r in results]

    print(f"  Надіслано задач: {len(tasks_to_send)}")
    print(f"  Отримано результатів: {len(results)}")

    # Перевірка на дублікати та втрати
    has_no_duplicates = len(processed_tasks) == len(set(processed_tasks))
    all_tasks_done = set(processed_tasks) == set(tasks_to_send)

    print(f"  Усі задачі виконано без втрат: {all_tasks_done}")
    print(f"  Подвійні обробки/гонки відсутні: {has_no_duplicates}")

if __name__ == "__main__":
    asyncio.run(main())