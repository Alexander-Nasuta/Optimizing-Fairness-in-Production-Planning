from typing import List, Any

import pandas
import pprint
import pandas as pd

from utils.crf_timestamp_solver_time_conversion import solver_time_to_timestamp
from utils.logger import log

import collections

from jsp_vis.console import gantt_chart_console
from ortools.sat.python import cp_model


start_timestamp = 1759125600  # corresponds to Monday 2025-09-29 06:00:00

line_id_mapping_to_line_str_mapping ={
    0: "Line 1",
    1: "Line 2",
    2: "Line 3",
}

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
        'order': 'Order 5',
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

task_idx_to_task_key_mapping = {
    0: "Order 1 × Geometry 1",
    1: "Order 2 × Geometry 2",
    2: "Order 3 × Geometry 3",
    3: "Order 4 × Geometry 4",
    4: "Order 5 × Geometry 5",
    5: "Order 1 × Geometry 6",
    6: "Order 1 × Geometry 7",
    7: "Order 2 × Geometry 8",
}


# Format of each order alternative tuple:
# alternative: (duration [min], line_id, priority, due_date [min])
EXAMPLE_ORDER_LINE_INSTANCE = [
    # Geometry 1
    [(1200, 2, 0, 2400)],
    # Geometry 2
    [(480, 2, 0, 480), (540, 0, 0, 480)],
    # Geometry 3
    [(720, 2, 0, 1920), (1000, 1,0, 1920)],
    # Geometry 4
    [(960, 1, 0, 900), (1200, 0, 0, 900)],
    # Geometry 5
    [(960, 1, 0, 2000), (1000, 2, 0, 2000)],
    # Geometry 6
    [(480, 1, 0, 2400)],
    # Geometry 7
    [(960, 0, 0, 2400)],
    # Geometry 8
    [(1440, 0, 0, 480), (1200, 2, 0, 480)],
]


class SolutionPrinter(cp_model.CpSolverSolutionCallback):
    """Print intermediate solutions."""

    def __init__(self):
        cp_model.CpSolverSolutionCallback.__init__(self)
        self.__solution_count = 0

    def on_solution_callback(self):
        """Called at each new solution."""
        log.debug(f"Solution {self.__solution_count}, time = {self.wall_time:.2f} s, cost = {self.objective_value:.2f}")
        self.__solution_count += 1


def main(makespan_weight: int = 1, tardiness_weight: int = 1, hours_per_day: int = 16,
         order_list: List[Any] = None) -> (pandas.DataFrame, dict):
    log.info("Running main function")
    log.info(f"makespan_weight: {makespan_weight}, tardiness_weight: {tardiness_weight}")

    # Model the crf line allocation problem.
    model = cp_model.CpModel()

    # 'orders' can be calculated from data in the database
    # using EXAMPLE_ORDER_INSTANCE for now
    orders = order_list

    # calculate horizon
    # for each oder add up the duration of the longest alternative
    horizon = sum(
        max((task_tuple[0] for task_tuple in order_alternatives_list), default=0)  # index 0 is the duration
        for order_alternatives_list in orders
    )
    log.debug(f"Horizon = {horizon}")

    line_set = set()
    for order in orders:
        for alternative in order:
            line_set.add(alternative[1])
    n_machines = len(line_set)  # used for gantt chart

    # Global storage of variables.
    intervals_per_resources = collections.defaultdict(list)
    starts_order_line = {}  # indexed by (order_idx, line_id).
    starts_order = {}  # indexed by (order_idx).
    tradiness = {}  # indexed by (order_idx).
    presences = {}  # indexed by (order_idx, alternative_idx).
    order_ends = {}  # indexed by (order_idx).

    priority_starts = []
    non_priority_starts = []

    # create alternative allocation intervals for each order
    for order_idx, order in enumerate(orders):

        order_line_allocation_presences = []

        order_start = model.new_int_var(0, horizon, f"order_{order_idx}_start")
        order_end = model.new_int_var(0, horizon, f"order_{order_idx}_end")

        order_ends[order_idx] = order_end
        starts_order[order_idx] = order_start

        # handle priority and non-priority starts
        order_is_priority = order[0][2] == 1  # index 2 is the priority
        if order_is_priority:
            priority_starts.append(order_start)
        else:
            non_priority_starts.append(order_start)

        order_tardiness = model.new_int_var(0, horizon, f"order_{order_idx}_tardiness")
        tradiness[order_idx] = order_tardiness

        for alternative in order:
            # unpack alternative tuple
            _duration, _line_id, _priority, _due_date = alternative

            suffix_str = f"order_{order_idx}_line_{_line_id}"
            alt_presence = model.new_bool_var(f"presence_{suffix_str}")
            alt_start = model.new_int_var(0, horizon, f"start_{suffix_str}")
            alt_end = model.new_int_var(0, horizon, f"end_{suffix_str}")

            alt_interval = model.new_optional_interval_var(
                alt_start, _duration, alt_end, alt_presence, f"interval_{suffix_str}"
            )

            alt_task_tardiness = model.new_int_var(0, horizon, f"tardiness_{suffix_str}")
            # Add a constraint that max_tardiness is equal to the maximum of 0 and (alt_end - _due_date)
            model.add_max_equality(alt_task_tardiness, [0, alt_end - _due_date])

            # set the end of the order to the end of the selected alternative
            model.add(order_end == alt_end).only_enforce_if(alt_presence)
            model.add(order_start == alt_start).only_enforce_if(alt_presence)
            model.add(order_tardiness == alt_task_tardiness).only_enforce_if(alt_presence)

            # Add the local interval to the right machine.
            intervals_per_resources[_line_id].append(alt_interval)

            # Add to presences
            presences[(order_idx, _line_id)] = alt_presence
            starts_order_line[(order_idx, _line_id)] = alt_start

            # group all alternative presences for an order in a list
            # to enforce only one alternative after the loop
            order_line_allocation_presences.append(alt_presence)

        # only one alternative/ one allocation can be selected
        model.add_exactly_one(order_line_allocation_presences)

    # line allocation constraints
    # a line can only process one order at a time
    for line_id, line_intervals in intervals_per_resources.items():
        model.add_no_overlap(line_intervals)


    # priority starts before non-priority constraints
    for priority_start in priority_starts:
        for non_priority_start in non_priority_starts:
            model.add(priority_start <= non_priority_start)

    # objective function
    # the objective function is to minimize the makespan and the tardiness weighted 1:1

    # makespan = end time of the last order
    makespan = model.new_int_var(0, horizon, "makespan")
    model.add_max_equality(makespan, [oe for oe in order_ends.values()])

    # total tardiness
    total_tardiness = model.new_int_var(0, horizon * len(orders), "total_tardiness")
    # sum up tardiness of all orders
    model.add(total_tardiness == sum(tradiness[order_idx] for order_idx in range(len(orders))))

    # defining a variable for the objective function
    # minimize makespan and tardiness weighted by 1:1 by default
    objective_var = model.new_int_var(0, horizon * len(orders), "objective")
    model.add(objective_var == makespan_weight * makespan + tardiness_weight * total_tardiness)
    log.info(f"Cost function: cost = {makespan_weight} * makespan + {tardiness_weight} * total_tardiness")

    model.minimize(objective_var)

    # Solve model.
    solver = cp_model.CpSolver()
    solution_printer = SolutionPrinter()
    status = solver.solve(model, solution_printer)

    log.info(f"""
Solution found: {status == cp_model.OPTIMAL or status == cp_model.FEASIBLE}
Solution is optimal: {status == cp_model.OPTIMAL}

Makespan: {solver.Value(makespan)} minutes
Total Tardiness: {solver.Value(total_tardiness)} minutes (sum of all tardiness values of the orders)
Cost: {solver.Value(objective_var)} (measures the quality of the solution based on the given weights of the cost function)
    """)

    makespan = solver.Value(makespan)

    log.info(f"Gantt chart (time window: 0-{makespan})")
    gantt_data = []
    for orders_idx, order_alternatives_list in enumerate(orders):
        try:
            line_idx = [
                line_id for _, line_id, *_ in order_alternatives_list
                if solver.Value(presences[(orders_idx, line_id)])
            ][0]
        except IndexError:
            # Fallback to 0 if the list is empty
            line_idx = next((line_id for _, line_id, *_ in order_alternatives_list), 0)
        gantt_data.append({
            'Task': orders_idx,
            'Start': solver.Value(starts_order[orders_idx]),
            'Finish': solver.Value(order_ends[orders_idx]),
            'Resource': line_idx
        })

    return gantt_data


if __name__ == '__main__':
    solution_dict = main(
        order_list=EXAMPLE_ORDER_LINE_INSTANCE,
        makespan_weight=0,
        tardiness_weight=1,
    )
    dict_for_gantt = [
        elem | {
            'Resource': f'Line {elem["Resource"]}'
        } for elem in solution_dict
    ]
    print(pprint.pformat(dict_for_gantt))
    gantt_chart_console(pd.DataFrame(dict_for_gantt), n_machines=3, resource_naming='Line')

    remapped_solution_dict = [
        {
            "Start": solver_time_to_timestamp(solver_time=elem["Start"], start_timestamp=start_timestamp),
            "Finish": solver_time_to_timestamp(solver_time=elem["Finish"], start_timestamp=start_timestamp),
            "Resource": line_id_mapping_to_line_str_mapping[elem["Resource"]],
            "Task": task_idx_to_task_key_mapping[elem["Task"]],
            "Order": task_idx_to_task_key_mapping[elem["Task"]].split(" × ")[0],
            "geometry": task_idx_to_task_key_mapping[elem["Task"]].split(" × ")[1],
        }
        for elem in solution_dict
    ]

    print("\nRemapped solution dict with timestamps and line strings:")
    print(pprint.pformat(remapped_solution_dict))


