import pprint

from demonstrator.example_geometry_line_mapping import geometry_line_mapping
from demonstrator.example_human_factors import human_factor_data
from demonstrator.example_order_data import order_data
from demonstrator.example_start_timestamp import start_timestamp
from demonstrator.example_worker_availabilities import worker_availabilities
from demonstrator.layer_two_worker_line_mdp import CrfWorkerAllocationEnv
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
