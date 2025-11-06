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
