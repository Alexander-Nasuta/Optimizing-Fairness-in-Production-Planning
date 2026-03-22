import pprint
import time

import wandb as wb
import gymnasium as gym
import pandas as pd

from gymcts.gymcts_agent import SoloMCTSAgent
from gymcts.gymcts_gym_env import SoloMCTSGymEnv

from utils.logger import log

from computational_experiments.layer_two.instances import get_instance
from demonstrator.example_geometry_line_mapping import geometry_line_mapping
from demonstrator.example_human_factors import human_factor_data
from demonstrator.example_order_data import order_data
from demonstrator.example_start_timestamp import start_timestamp
from demonstrator.layer_two_worker_line_mdp import CrfWorkerAllocationEnv

experiment_sweep_config = {
    'method': 'grid',
    'metric': {
        'name': 'time',
        'goal': 'minimize'
    },
    'parameters': {
        "approach": {
            'values': ["mcts"]
        },
        "num_jobs": {
            'values': [8, 7, 6, 5, 4, 3, 2, 1]
        },
        "optimisation": {
            'values': [
                "resilience",
                "preference",
                "experience",
                "balanced",
            ]
        },
        "run_no": {
            'values': [
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10
            ]
        }
    }
}


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



def perform_run():
    with wb.init(
            sync_tensorboard=False,
            monitor_gym=False,
            save_code=True,
    ) as run:
        log.info(f"run name: {run.name}, run id: {run.id}")

        experiment_params = wb.config
        log.info(f"experiment params: {pprint.pformat(experiment_params)}")

        num_jobs = experiment_params["num_jobs"]

        optimisation_target = experiment_params["optimisation"]

        if optimisation_target == "resilience":
            RESILIENCE_WEIGHT = 1
            PREFERENCE_WEIGHT = 0
            EXPERIENCE_WEIGHT = 0
            FAIRNESS_WEIGHT = 1
        elif optimisation_target == "preference":
            RESILIENCE_WEIGHT = 0
            PREFERENCE_WEIGHT = 1
            EXPERIENCE_WEIGHT = 0
            FAIRNESS_WEIGHT = 1
        elif optimisation_target == "experience":
            RESILIENCE_WEIGHT = 0
            PREFERENCE_WEIGHT = 0
            EXPERIENCE_WEIGHT = 1
            FAIRNESS_WEIGHT = 1
        elif optimisation_target == "balanced":
            RESILIENCE_WEIGHT = 1
            PREFERENCE_WEIGHT = 1
            EXPERIENCE_WEIGHT = 1
            FAIRNESS_WEIGHT = 1


        wb.log({
            "RESILIENCE_WEIGHT": RESILIENCE_WEIGHT,
            "PREFERENCE_WEIGHT": PREFERENCE_WEIGHT,
            "EXPERIENCE_WEIGHT": EXPERIENCE_WEIGHT,
            "FAIRNESS_WEIGHT": FAIRNESS_WEIGHT,
        })

        layer_one, availabilities = get_instance(num_jobs=num_jobs)

        env = CrfWorkerAllocationEnv(
            previous_step_output=layer_one,
            worker_availabilities=availabilities,
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

        start = time.perf_counter()

        res = solve_with_mcts(env, n_sim=3)  # increase n_sim to improve solution quality

        end = time.perf_counter()

        elapsed = end - start

        wb.log({
                "run_time": float(elapsed),
                "time": float(elapsed),
            })

        log.info(f"Allocations: \n{pprint.pformat(res['allocations'])}")
        log.info(
            f"KPIs: experience={res['experience']:.2f}, resilience={res['resilience']:.2f}, preference={res['preference']:.2f}")


if __name__ == '__main__':
    #sweep_id = wb.sweep(experiment_sweep_config, project="optimizing-fairness", entity="querry")
    sweep_id = "b42nb31u"
    wb.agent(sweep_id, function=perform_run, count=10*4*8, project="optimizing-fairness", entity="querry")


