from demonstrator.example_geometry_line_mapping import geometry_line_mapping
from demonstrator.example_human_factors import human_factor_data
from demonstrator.example_order_data import order_data
from demonstrator.example_start_timestamp import start_timestamp
from demonstrator.example_worker_availabilities import worker_availabilities
from demonstrator.layer_two_worker_line_mdp import CrfWorkerAllocationEnv
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



