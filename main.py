import hashlib
import bisect
import uuid


class ConsistentHashingCluster:
    def __init__(self, vnodes_per_shard=100):
        """
        :param vnodes_per_shard: Кількість віртуальних вузлів для одного фізичного шарда.
        Це необхідно для того, щоб рівномірно "розмазати" шарди по хеш-кільцю.
        """
        self.vnodes_per_shard = vnodes_per_shard
        self.ring = []  # Хеш-кільце: масив кортежів (hash_value, shard_name)
        self.shards = {}  # In-memory сховище: shard_name -> dict()

    def _hash(self, key):
        """Хеш-функція (MD5), яка перетворює рядок на велике ціле число."""
        h = hashlib.md5(str(key).encode('utf-8')).hexdigest()
        return int(h, 16)

    def add_shard(self, shard_name):
        """Додає новий шард у кластер (розміщує його віртуальні вузли на кільці)."""
        if shard_name not in self.shards:
            self.shards[shard_name] = {}

        for i in range(self.vnodes_per_shard):
            vnode_key = f"{shard_name}-vnode-{i}"
            hash_val = self._hash(vnode_key)
            # Вставляємо на кільце зі збереженням сортування (бінарне дерево/масив)
            bisect.insort(self.ring, (hash_val, shard_name))

    def _get_target_shard(self, key):
        """Знаходить, якому шарду належить ключ (йдемо по кільцю за годинниковою стрілкою)."""
        if not self.ring:
            raise ValueError("Кластер порожній!")

        hash_val = self._hash(key)
        # Бінарний пошук найближчого вузла, хеш якого більший за хеш ключа
        idx = bisect.bisect(self.ring, (hash_val, ""))

        # Якщо дійшли до кінця масиву, замикаємо кільце (переходимо на початок)
        if idx == len(self.ring):
            idx = 0

        return self.ring[idx][1]

    def put(self, key, value):
        """Записує ключ-значення у відповідний шард."""
        target = self._get_target_shard(key)
        self.shards[target][key] = value

    def get(self, key):
        """Зчитує значення з відповідного шарда."""
        target = self._get_target_shard(key)
        return self.shards[target].get(key)

    def rebalance(self):
        """Емулює перенесення даних при додаванні/видаленні шардів."""
        all_data = {}
        # 1. Збираємо всі дані
        for s in self.shards:
            all_data.update(self.shards[s])
            self.shards[s] = {}  # Очищаємо шарди
        # 2. Перезаписуємо за новими правилами кільця
        for k, v in all_data.items():
            self.put(k, v)

    def print_distribution(self):
        """Візуалізація розподілу ключів по шардах."""
        total_keys = sum(len(data) for data in self.shards.values())
        print(f"Загалом ключів: {total_keys}")
        for shard_name, data in sorted(self.shards.items()):
            percentage = (len(data) / total_keys * 100) if total_keys else 0
            print(f"  {shard_name}: {len(data)} ключів ({percentage:.1f}%)")


# ==========================================
# БЛОК ТЕСТУВАННЯ
# ==========================================
if __name__ == "__main__":
    print("--- Початок тестування Sharding (Consistent Hashing) ---")

    cluster = ConsistentHashingCluster(vnodes_per_shard=100)

    # Ініціалізуємо кластер із 3 шардів
    cluster.add_shard("Shard-A")
    cluster.add_shard("Shard-B")
    cluster.add_shard("Shard-C")

    # [Тест 1] Коректність get після put
    print("\n[Тест 1] Коректність запису/читання (get після put):")
    cluster.put("user_101", "{name: 'Alice', age: 25}")
    result = cluster.get("user_101")
    target = cluster._get_target_shard("user_101")
    print(f"  Дані користувача 'user_101' знайдено у {target}: {result}")

    # [Тест 2] Рівномірність розподілу
    print("\n[Тест 2] Статистична перевірка рівномірності (генерація 10 000 ключів):")
    keys_to_insert = [str(uuid.uuid4()) for _ in range(10000)]

    for k in keys_to_insert:
        cluster.put(k, "some_data")

    cluster.print_distribution()
    # Якщо vnodes працюють правильно, кожен шард матиме приблизно ~33.3%

    # [Тест 3] Стабільність Consistent Hashing (Додавання нового шарда)
    print("\n[Тест 3] Стабільність при ребалансуванні (додаємо Shard-D):")

    # Запам'ятовуємо, де лежав кожен ключ ДО ребалансування
    mapping_before = {k: cluster._get_target_shard(k) for k in keys_to_insert}

    # Додаємо 4-й сервер
    cluster.add_shard("Shard-D")
    cluster.rebalance()
    cluster.print_distribution()

    # Перевіряємо, скільки ключів фізично переїхало на інші сервери
    mapping_after = {k: cluster._get_target_shard(k) for k in keys_to_insert}

    keys_moved = 0
    for k in keys_to_insert:
        if mapping_before[k] != mapping_after[k]:
            keys_moved += 1

    moved_percentage = (keys_moved / len(keys_to_insert)) * 100

    print(f"\n  Ключів, що змінили свій шард: {keys_moved} із {len(keys_to_insert)}")
    print(f"  Відсоток перемаплених ключів: {moved_percentage:.1f}%")
    print("  (Очікується ~25%. Якби ми використовували простий mod N, перемістилося б ~75%!)")