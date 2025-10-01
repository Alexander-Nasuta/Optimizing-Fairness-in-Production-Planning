from demonstrator.example_geometry_line_mapping import geometry_line_mapping
from demonstrator.example_human_factors import human_factor_data
from demonstrator.example_order_data import order_data
from demonstrator.example_start_timestamp import start_timestamp
from demonstrator.example_worker_availabilities import worker_availabilities
from demonstrator.layer_two_worker_line_mdp import CrfWorkerAllocationEnv
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
        order_data=order_data
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

