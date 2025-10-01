import pprint

from matplotlib.style.core import available

from utils.crf_timestamp_solver_time_conversion import solver_time_to_timestamp

if __name__ == '__main__':
    start_timestamp = 1759125600  # corresponds to Monday 2025-09-29 06:00:00
    # shift 1 workers (06:00 - 14:00), 15 workers in total, indexed from 1 to 15
    shift_one_workers = [f"w_{idx}" for idx in range(1, 16)]
    # shift 2 workers (14:00 - 22:00), 14 workers in total, indexed from 16 to 29
    shift_two_workers = [f"w_{idx}" for idx in range(16, 30)]
    # shift 3 workers (22:00 - 06:00), 14 workers in total, indexed from 30 to 43
    shift_three_workers = [f"w_{idx}" for idx in range(30, 44)]

    worker_availabilities = []
    for worker in shift_one_workers:
        monday_entry = {
            "date": "2025-09-29", # yyyy-mm-dd
            "from_timestamp": solver_time_to_timestamp(solver_time=0, start_timestamp=start_timestamp),
            "end_timestamp": solver_time_to_timestamp(solver_time=480, start_timestamp=start_timestamp),
            "worker": worker,
        }
        tuesday_entry = {
            "date": "2025-09-30", # yyyy-mm-dd
            "from_timestamp": solver_time_to_timestamp(solver_time=1440, start_timestamp=start_timestamp),
            "end_timestamp": solver_time_to_timestamp(solver_time=1920, start_timestamp=start_timestamp),
            "worker": worker,
        }
        # add to worker availabilities

        worker_availabilities.extend([monday_entry, tuesday_entry])

    for worker in shift_two_workers:
        monday_entry = {
            "date": "2025-09-29", # yyyy-mm-dd
            "from_timestamp": solver_time_to_timestamp(solver_time=480, start_timestamp=start_timestamp),
            "end_timestamp": solver_time_to_timestamp(solver_time=960, start_timestamp=start_timestamp),
            "worker": worker,
        }
        tuesday_entry = {
            "date": "2025-09-30", # yyyy-mm-dd
            "from_timestamp": solver_time_to_timestamp(solver_time=1920, start_timestamp=start_timestamp),
            "end_timestamp": solver_time_to_timestamp(solver_time=2400, start_timestamp=start_timestamp),
            "worker": worker,
        }
        # add to worker availabilities
        worker_availabilities.extend([monday_entry, tuesday_entry])

    for worker in shift_three_workers:
        monday_entry = {
            "date": "2025-09-29", # yyyy-mm-dd
            "from_timestamp": solver_time_to_timestamp(solver_time=960, start_timestamp=start_timestamp),
            "end_timestamp": solver_time_to_timestamp(solver_time=1440, start_timestamp=start_timestamp),
            "worker": worker,
        }
        # add to worker availabilities
        worker_availabilities.append(monday_entry)

    print(pprint.pformat(worker_availabilities))