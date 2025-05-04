from data import run_task

# Define your parameters for the Bybit data extraction
symbol = "BTCUSDT"
category = "inverse"
start_date = "2024-12-01 00:00"
end_date = "2024-12-31 23:45"

# Run the task asynchronously using Celery
# Celery allows running multiple tasks in parallel using multiprocessing.
# This is more efficient and scalable than traditional threading in Python,
# especially for I/O-bound or network-heavy tasks like API data fetching.
# Each call to `run_task()` sends the job to a background worker,
# which can process many such jobs concurrently.
print(run_task(start_date, end_date, symbol, category))


"""
Why use Celery?
---------------
- Celery is a distributed task queue system that uses multiprocessing under the hood.
- It avoids Python's GIL limitations (which affect threading) by using multiple processes.
- This makes it suitable for high-performance or time-consuming background jobs.
- You can run multiple instances of this script with different parameters and all tasks will be queued and processed in parallel by workers.

Example: You could call `run_task()` for multiple symbols like so:

    run_task(start1, end1, "BTCUSDT", "inverse")
    run_task(start2, end2, "ETHUSDT", "inverse")
    run_task(start3, end3, "XRPUSDT", "inverse")

Each of these will be processed concurrently if you have multiple Celery workers running.

Make sure your Celery worker is running using:
    celery -A data worker --loglevel=info

"""
