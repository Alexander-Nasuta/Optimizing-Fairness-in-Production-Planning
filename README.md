![FAIRWork Logo](https://fairwork-project.eu/assets/images/2022.09.15-FAIRWork-Logo-V1.0-color.svg)
# Optimizing Fairness in Production Planning: A Human-Centric Approach to Machine and Workforce Allocation

## Setup 
Most scripts contain a `if __name__ == "__main__":` block, which can be used to run the script as a standalone script. 
This block showcases the main functionality of the script and can be used to test the script.
For local development the REST API can be run locally.

1. Clone the repository `git clone https://github.com/Alexander-Nasuta/Optimizing-Fairness-in-Production-Planning.git`
2. Install the project locally by running `pip install -e .`

## Examples

Below you can find example scripts for the different assignment problems solved in this repository.
Each example is also presented in the `demonstrator` folder within the `if __name__ == '__main__':`-block, so that you can run each example script standalone, without copy pasting the code.

### Order-Line Assignment

This example performs the first layer of the scheduling problem, the order-line assignment.

```python
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
```

## Worker-Line Assignment

This example performs the second layer of the scheduling problem, the worker-line assignment.
Make sure to run this example with `emulate terminal` enabled in your IDE, as it uses `inquirer` for manual input or start it 

### Manual Assignment
```python
from demonstrator.example_geometry_line_mapping import geometry_line_mapping
from demonstrator.example_human_factors import human_factor_data
from demonstrator.example_order_data import order_data
from demonstrator.example_start_timestamp import start_timestamp
from demonstrator.example_worker_availabilities import worker_availabilities
from demonstrator.layer_two_worker_line_mdp import CrfWorkerAllocationEnv
from demonstrator.layer_two_worker_line_mdp import CrfWorkerAllocationEnv, throughput_mapping
from demonstrator.example_layer_one_output import layer_one_output

import pprint
import inquirer

if __name__ == '__main__':
    env = CrfWorkerAllocationEnv(
        previous_step_output=layer_one_output,
        worker_availabilities=worker_availabilities,
        geometry_line_mapping=geometry_line_mapping,
        human_factor_data=human_factor_data,
        start_timestamp=start_timestamp,
        allocate_workers_on_the_same_line_if_possible=False,
        order_data=order_data,
        throughput_mapping=throughput_mapping,
    )
    env.reset()

    done = False
    print("each task/node corresponds to an action")

    while not done:
        env.render()
        allocations_dict = env.get_worker_allocation(filter_no_workers_assigned=True)
        current_interval_from = None
        current_interval_to = None

        # get the current interval by getting a row where the col 'is_current_interval' is 1
        for row in range(env.get_state().shape[0]):
            if env.get_state().at[row, 'is_current_interval'] == 1:
                current_interval_from = env.get_state().at[row, 'interval_start']
                current_interval_to = env.get_state().at[row, 'interval_end']
                break

        questions = [
            inquirer.List(
                "next_action",
                message=f"Which Worker should be scheduled next? The current interval is from {current_interval_from} to {current_interval_to}",
                choices=[
                    (f"Allocate Worker '{env._idx_to_worker_map[worker].replace('worker_', '')}' ({env.get_state().at[row, env._idx_to_worker_map[worker]]}) on line '{env.get_state().at[row, 'line']}' (Row with idx={row})",
                     (row, worker))
                    for (row, worker) in env.valid_action_tuples()
                ],
            ),
        ]
        next_action_tuple = inquirer.prompt(questions)["next_action"]
        next_action = env.action_tuple_to_action_idx(next_action_tuple)

        msg = (f"Allocating worker '{env._idx_to_worker_map[next_action_tuple[1]].replace('worker_', '')}' "
               f"on line '{env.get_state().at[next_action_tuple[0], 'line']}' for task {env.get_state().at[next_action_tuple[0], 'Task']}")
        print(msg)

        _, reward, done, _, _ = env.step(next_action)

    allocations_dict = env.get_worker_allocation(filter_no_workers_assigned=True)
    print(f"Allocations: \n {pprint.pformat(allocations_dict)}")
```

### Reinforcement Learning based Assignment
```python
from demonstrator.example_geometry_line_mapping import geometry_line_mapping
from demonstrator.example_human_factors import human_factor_data
from demonstrator.example_order_data import order_data
from demonstrator.example_start_timestamp import start_timestamp
from demonstrator.example_worker_availabilities import worker_availabilities
from demonstrator.layer_two_worker_line_mdp import CrfWorkerAllocationEnv, throughput_mapping
from demonstrator.example_layer_one_output import layer_one_output


from sb3_contrib.common.maskable.policies import MaskableActorCriticPolicy
from sb3_contrib.common.wrappers import ActionMasker
from stable_baselines3.common.env_util import make_vec_env
from stable_baselines3.common.monitor import Monitor
from stable_baselines3.common.vec_env import DummyVecEnv

import sb3_contrib
import gymnasium as gym
import numpy as np
import pprint


RESILIENCE_WEIGHT = 1
PREFERENCE_WEIGHT = 1
EXPERIENCE_WEIGHT = 1
FAIRNESS_WEIGHT = 1

def make_env():
    env = CrfWorkerAllocationEnv(
        previous_step_output=layer_one_output,
        worker_availabilities=worker_availabilities,
        geometry_line_mapping=geometry_line_mapping,
        human_factor_data=human_factor_data,
        start_timestamp=start_timestamp,
        allocate_workers_on_the_same_line_if_possible=False,
        order_data=order_data,
        throughput_mapping=throughput_mapping,
        resilience_weight=RESILIENCE_WEIGHT,
        preference_weight=PREFERENCE_WEIGHT,
        experience_weight=EXPERIENCE_WEIGHT,
        fairness_weight=FAIRNESS_WEIGHT,
    )

    def mask_fn(env: gym.Env) -> np.ndarray:
        return env.unwrapped.valid_action_mask()

    env = ActionMasker(env, mask_fn)

    env = Monitor(env)
    return env


if __name__ == '__main__':

    # Create the vectorized environment
    vec_env = make_vec_env(make_env, n_envs=4, vec_env_cls=DummyVecEnv)

    model = sb3_contrib.MaskablePPO(MaskableActorCriticPolicy, vec_env, device="auto")

    print("training the model. This may take a while...")
    model.learn(total_timesteps=1_000) # feel free to increase the number of timesteps
    model.save(f"crf_rl_model-action-{vec_env.action_space.n}_obs-{vec_env.observation_space.shape}"
               f"_resilience-{RESILIENCE_WEIGHT}"
               f"_experience-{EXPERIENCE_WEIGHT}"
               f"_preference-{PREFERENCE_WEIGHT}.zip")

    test_env = CrfWorkerAllocationEnv(
        previous_step_output=layer_one_output,
        worker_availabilities=worker_availabilities,
        geometry_line_mapping=geometry_line_mapping,
        human_factor_data=human_factor_data,
        start_timestamp=start_timestamp,
        allocate_workers_on_the_same_line_if_possible=False,
        order_data=order_data,
        throughput_mapping=throughput_mapping,
        resilience_weight=RESILIENCE_WEIGHT,
        preference_weight=PREFERENCE_WEIGHT,
        experience_weight=EXPERIENCE_WEIGHT,
        fairness_weight=FAIRNESS_WEIGHT,
    )
    obs, start_timestamp = test_env.reset()
    done = False
    while not done:
        masks = test_env.valid_action_mask()
        action, _ = model.predict(observation=obs, deterministic=True, action_masks=masks)
        obs, rew, done, turn, info = test_env.step(action)
        print(f"Action: {action}, Reward: {rew}")

    allocations_dict = test_env.get_worker_allocation(filter_no_workers_assigned=True)
    print(f"Allocations: \n {pprint.pformat(allocations_dict)}")
```

### Greedy Assignment
```python
import pprint

from demonstrator.example_geometry_line_mapping import geometry_line_mapping
from demonstrator.example_human_factors import human_factor_data
from demonstrator.example_order_data import order_data
from demonstrator.example_start_timestamp import start_timestamp
from demonstrator.example_worker_availabilities import worker_availabilities
from demonstrator.layer_two_worker_line_mdp import CrfWorkerAllocationEnv, throughput_mapping
from demonstrator.example_layer_one_output import layer_one_output


RESILIENCE_WEIGHT = 1
PREFERENCE_WEIGHT = 1
EXPERIENCE_WEIGHT = 1
FAIRNESS_WEIGHT = 1


if __name__ == '__main__':
    env = CrfWorkerAllocationEnv(
        previous_step_output=layer_one_output,
        worker_availabilities=worker_availabilities,
        geometry_line_mapping=geometry_line_mapping,
        human_factor_data=human_factor_data,
        start_timestamp=start_timestamp,
        allocate_workers_on_the_same_line_if_possible=False,
        order_data=order_data,
        throughput_mapping=throughput_mapping,
        resilience_weight=RESILIENCE_WEIGHT,
        preference_weight=PREFERENCE_WEIGHT,
        experience_weight=EXPERIENCE_WEIGHT,
        fairness_weight=FAIRNESS_WEIGHT,
    )

    env.greedy_rollout_sparse()

    experience, resilience, preference = env.get_KPIs()
    allocations_dict = env.get_worker_allocation(filter_no_workers_assigned=True)
    print(f"Allocations: \n{pprint.pformat(allocations_dict)}")
    print(f"KPIs: experience={experience:.2f}, resilience={resilience:.2f}, preference={preference:.2f}")
```

### Monte Carlo Tree Search Assignment
```python
import pprint

from demonstrator.example_geometry_line_mapping import geometry_line_mapping
from demonstrator.example_human_factors import human_factor_data
from demonstrator.example_order_data import order_data
from demonstrator.example_start_timestamp import start_timestamp
from demonstrator.example_worker_availabilities import worker_availabilities
from demonstrator.layer_two_worker_line_mdp import CrfWorkerAllocationEnv
from demonstrator.example_layer_one_output import layer_one_output

import gymnasium as gym
import pandas as pd

from gymcts.gymcts_agent import SoloMCTSAgent
from gymcts.gymcts_gym_env import SoloMCTSGymEnv


RESILIENCE_WEIGHT = 1
PREFERENCE_WEIGHT = 1
EXPERIENCE_WEIGHT = 1
FAIRNESS_WEIGHT = 1

class CrfMCTSWrapper(SoloMCTSGymEnv, gym.Wrapper):

    def __init__(self, env: CrfWorkerAllocationEnv):
        gym.Wrapper.__init__(self, env)

    def load_state(self, state: pd.DataFrame) -> None:
        self.env.unwrapped.load_state(state)

    def is_terminal(self) -> bool:
        return self.env.unwrapped.is_terminal_state()

    def get_valid_actions(self) -> list[int]:
        return self.env.unwrapped.valid_action_list()

    def rollout(self) -> float:
        # return self.env.unwrapped.greedy_rollout_sparse()
        for _ in range(2):
            best_action = self.env.unwrapped.best_eager_action()
            if best_action is not None:
                self.env.step(best_action)

        return self.env.unwrapped.get_scaled_KPI_score()

    def get_state(self) -> pd.DataFrame:
        return self.env.unwrapped.get_state()


def solve_with_mcts(env: CrfWorkerAllocationEnv, n_sim=2) -> dict:
    env.reset()
    env = CrfMCTSWrapper(env)

    agent = SoloMCTSAgent(
        env=env,
        clear_mcts_tree_after_step=False,
        render_tree_after_step=True,
        exclude_unvisited_nodes_from_render=True,
        number_of_simulations_per_step=n_sim,
    )
    actions = agent.solve(render_tree_after_step=True)

    experience, resilience, preference = env.unwrapped.get_KPIs()
    allocation_with_worker_data = env.unwrapped.get_worker_allocation()

    return {
        "experience": experience,
        "preference": preference,
        "resilience": resilience,
        "transparency": "medium",
        "allocations": allocation_with_worker_data,
    }

if __name__ == '__main__':
    env = CrfWorkerAllocationEnv(
        previous_step_output=layer_one_output,
        worker_availabilities=worker_availabilities,
        geometry_line_mapping=geometry_line_mapping,
        human_factor_data=human_factor_data,
        start_timestamp=start_timestamp,
        allocate_workers_on_the_same_line_if_possible=False,
        order_data=order_data,
        resilience_weight=RESILIENCE_WEIGHT,
        preference_weight=PREFERENCE_WEIGHT,
        experience_weight=EXPERIENCE_WEIGHT,
        fairness_weight=FAIRNESS_WEIGHT,
    )

    res = solve_with_mcts(env, n_sim=3) # increase n_sim to improve solution quality
    print(f"Allocations: \n{pprint.pformat(res['allocations'])}")
    print(f"KPIs: experience={res['experience']:.2f}, resilience={res['resilience']:.2f}, preference={res['preference']:.2f}")
```

## Testing

### Unit Tests with `pytest`
Unit tests are implemented using `pytest`. 
To run the tests, run `pytest` in the root directory of the repository.
Note, that you have to install the 'requirements_dev.txt' (`pip install -r requirements_dev.txt`) to run the tests.
The configuration for the tests is located in the `pyproject.toml` file.


### Unit Tests with `tox`
Tox is a generic virtualenv management and test command line tool you can use for checking that your package installs correctly with different Python versions and interpreters.
To run the tests with `tox`, run `tox` in the root directory of the repository.
The configuration for the tests is located in the `tox.ini` file.


# FAIRWork Project
Development Repository for AI Services for the FAIRWork Project

“This work has been supported by the FAIRWork project (www.fairwork-project.eu) and has been funded within the European Commission’s Horizon Europe Programme under contract number 101049499. This paper expresses the opinions of the authors and not necessarily those of the European Commission. The European Commission is not liable for any use that may be made of the information contained in this presentation.”

Copyright © RWTH of the FAIRWork Consortium
