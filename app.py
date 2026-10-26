from flask import Flask
import redis
import os

app = Flask(__name__)

# Підключаємося до Redis. Хост беремо зі змінної оточення REDIS_HOST
redis_host = os.environ.get('REDIS_HOST', 'localhost')
cache = redis.Redis(host=redis_host, port=6379)

@app.route('/')
def hello():
    # Збільшуємо лічильник у базі Redis при кожному запиті
    count = cache.incr('hits')
    return f"Привіт! Ти відкрив цю сторінку {count} разів. (Дані збережено в Redis)\n"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)