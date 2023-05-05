import threading
import sys
import time


def thread_job(number):
    print('Hello {}'.format(number))

    sys.stdout.flush()


def run_threads(count):
    threads = [threading.Thread(target=thread_job, args=(i,)) for i in range(0, count)
    ]
    for thread in threads:
        thread.start()  # каждый поток должен быть запущен
    for thread in threads:
        thread.join()  # дожидаемся исполнения всех потоков


run_threads(6)
print(finish)