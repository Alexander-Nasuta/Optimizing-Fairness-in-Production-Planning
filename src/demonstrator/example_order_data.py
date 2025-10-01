from demonstrator.example_start_timestamp import start_timestamp
from utils.crf_timestamp_solver_time_conversion import solver_time_to_timestamp

order_data = [
    {
        'amount': 100 * 19,
        'deadline': solver_time_to_timestamp(solver_time=2400, start_timestamp=start_timestamp),
        'geometry': 'Geometry 1',
        'mold': 4,
        'order': 'Order 1',
        'priority': 'false'
    },
    {
        'amount': 100 * 7,
        'deadline': solver_time_to_timestamp(solver_time=2400, start_timestamp=start_timestamp),
        'geometry': 'Geometry 6',
        'mold': 4,
        'order': 'Order 1',
        'priority': 'false'
    },
    {
        'amount': 100 * 15,
        'deadline': solver_time_to_timestamp(solver_time=2400, start_timestamp=start_timestamp),
        'geometry': 'Geometry 7',
        'mold': 4,
        'order': 'Order 1',
        'priority': 'false'
    },
    {
        'amount': 100 * 7,
        'deadline': solver_time_to_timestamp(solver_time=480, start_timestamp=start_timestamp),
        'geometry': 'Geometry 2',
        'mold': 4,
        'order': 'Order 2',
        'priority': 'false'
    },
    {
        'amount': 100 * 23,
        'deadline': solver_time_to_timestamp(solver_time=480, start_timestamp=start_timestamp),
        'geometry': 'Geometry 8',
        'mold': 4,
        'order': 'Order 2',
        'priority': 'false'
    },
    {
        'amount': 100 * 11,
        'deadline': solver_time_to_timestamp(solver_time=1920, start_timestamp=start_timestamp),
        'geometry': 'Geometry 3',
        'mold': 4,
        'order': 'Order 3',
        'priority': 'false'
    },
    {
        'amount': 100 * 15,
        'deadline': solver_time_to_timestamp(solver_time=900, start_timestamp=start_timestamp),
        'geometry': 'Geometry 4',
        'mold': 4,
        'order': 'Order 4',
        'priority': 'false'
    },
    {
        'amount': 100 * 15,
        'deadline': solver_time_to_timestamp(solver_time=2000, start_timestamp=start_timestamp),
        'geometry': 'Geometry 5',
        'mold': 4,
        'order': 'Order 5',
        'priority': 'false'
    },
]