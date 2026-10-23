import pprint
import time

import wandb as wb

from utils.logger import log
from utils.reproducibility import seed_everything, data_fingerprint, environment_info

from computational_experiments.layer_two.instances import SCENARIOS, LAYER_ONE_INSTANCE_NAME, LAYER_ONE_OUTPUT, \
    ORDER_DATA, THROUGHPUT_MAPPING, START_TIMESTAMP
from demonstrator.layer_two_worker_line_mdp import CrfWorkerAllocationEnv

WANDB_PROJECT = "fairness-in-production-planning"
WANDB_ENTITY = "querry"

SCENARIO_NAMES = list(SCENARIOS.keys())
OPTIMISATION_STRATEGIES = ["balanced"]
# only preference fairness enters the reward (experience and resilience fairness are logged for evaluation only)
REWARD_FAIRNESS_SIGNALS = ("preference",)

experiment_sweep_config = {
    'method': 'grid',
    'metric': {
        'name': 'time',
        'goal': 'minimize'
    },
    'parameters': {
        "approach": {
            'values': ["greedy"]
        },
        "layer_one_instance": {
            'values': [LAYER_ONE_INSTANCE_NAME]
        },
        "scenario": {
            'values': SCENARIO_NAMES
        },
        "optimisation": {
            'values': OPTIMISATION_STRATEGIES
        },
    }
}

# (resilience, preference, experience, fairness) weights of Equation (19)
OPTIMISATION_WEIGHTS = {
    "resilience": (1, 0, 0, 1),
    "preference": (0, 1, 0, 1),
    "experience": (0, 0, 1, 1),
    "balanced": (1, 1, 1, 1),
}


def perform_run():
    with wb.init(
            sync_tensorboard=False,
            monitor_gym=False,
            save_code=True,
    ) as run:
        log.info(f"run name: {run.name}, run id: {run.id}")

        experiment_params = wb.config
        log.info(f"experiment params: {pprint.pformat(experiment_params)}")

        optimisation_target = experiment_params["optimisation"]
        if optimisation_target not in OPTIMISATION_WEIGHTS:
            raise NotImplementedError(f"unknown optimisation target: {optimisation_target}")
        RESILIENCE_WEIGHT, PREFERENCE_WEIGHT, EXPERIENCE_WEIGHT, FAIRNESS_WEIGHT = OPTIMISATION_WEIGHTS[optimisation_target]

        scenario = SCENARIOS[experiment_params["scenario"]]

        # greedy is deterministic; seeding keeps all approaches (greedy, MCTS, RL) on the same footing
        random_seed = scenario["seed"]
        seed_everything(random_seed)

        run.config.update({
            "random_seed": random_seed,
            "reward_fairness_signals": list(REWARD_FAIRNESS_SIGNALS),
            # fingerprints to verify that runs used identical input data
            "layer_one_output_fingerprint": data_fingerprint(LAYER_ONE_OUTPUT),
            "order_data_fingerprint": data_fingerprint(ORDER_DATA),
            "scenario_fingerprint": data_fingerprint(scenario),
        } | environment_info())

        wb.log({
            "RESILIENCE_WEIGHT": RESILIENCE_WEIGHT,
            "PREFERENCE_WEIGHT": PREFERENCE_WEIGHT,
            "EXPERIENCE_WEIGHT": EXPERIENCE_WEIGHT,
            "FAIRNESS_WEIGHT": FAIRNESS_WEIGHT,
            "scenario_seed": scenario["seed"],
        })

        env = CrfWorkerAllocationEnv(
            previous_step_output=LAYER_ONE_OUTPUT,
            worker_availabilities=scenario["worker_availabilities"],
            geometry_line_mapping=scenario["geometry_line_mapping"],
            human_factor_data=scenario["human_factor_data"],
            start_timestamp=START_TIMESTAMP,
            allocate_workers_on_the_same_line_if_possible=False,
            order_data=ORDER_DATA,
            throughput_mapping=THROUGHPUT_MAPPING,
            resilience_weight=RESILIENCE_WEIGHT,
            preference_weight=PREFERENCE_WEIGHT,
            experience_weight=EXPERIENCE_WEIGHT,
            fairness_weight=FAIRNESS_WEIGHT,
            reward_fairness_signals=REWARD_FAIRNESS_SIGNALS,
        )

        start = time.perf_counter()
        scaled_kpi_score = env.greedy_rollout_sparse()
        end = time.perf_counter()

        elapsed = end - start

        experience, resilience, preference = env.get_KPIs()
        # fairness signal of the reward (preference fairness only, as configured)
        fairness = env.get_fairness_metric(is_terminal=True)
        # fairness scores for evaluation: preference, experience, resilience and total (mean of the three)
        fairness_metrics = env.get_fairness_metrics()

        state = env.get_state()
        required_slots = int(state["required_workers"].sum())
        filled_slots = int(state["allocated_workers"].sum())

        allocations_dict = env.get_worker_allocation(filter_no_workers_assigned=True)

        wb.log({
            "run_time": float(elapsed),
            "time": float(elapsed),
            "experience": experience,
            "resilience": resilience,
            "preference": preference,
            "fairness": fairness,
            **fairness_metrics,
            "scaled_kpi_score": scaled_kpi_score,
            "num_workers": env.get_number_of_workers(),
            "num_intervals": env.get_number_of_intervals(),
            "num_rows": len(state),
            "required_slots": required_slots,
            "filled_slots": filled_slots,
            "unfilled_slots": required_slots - filled_slots,
        })
        # identical fingerprints <=> identical worker allocations
        run.summary["allocation_fingerprint"] = data_fingerprint(allocations_dict)

        log.info(f"Allocations: \n{pprint.pformat(allocations_dict)}")
        log.info(f"KPIs: experience={experience:.2f}, resilience={resilience:.2f}, preference={preference:.2f}, "
                 f"fairness={fairness:.2f}, fairness metrics={pprint.pformat(fairness_metrics)}")


if __name__ == '__main__':
    sweep_id = wb.sweep(experiment_sweep_config, project=WANDB_PROJECT, entity=WANDB_ENTITY)
    wb.agent(sweep_id, function=perform_run, count=len(SCENARIO_NAMES) * len(OPTIMISATION_STRATEGIES),
             project=WANDB_PROJECT, entity=WANDB_ENTITY)
