import pprint
import time
import sb3_contrib


import wandb as wb
import gymnasium as gym
import numpy as np
from numpy.ma.extras import average
from sb3_contrib.common.maskable.policies import MaskableActorCriticPolicy

from sb3_contrib.common.wrappers import ActionMasker
from stable_baselines3.common.env_util import make_vec_env
from stable_baselines3.common.monitor import Monitor
from stable_baselines3.common.vec_env import DummyVecEnv

from wandb.integration.sb3 import WandbCallback

from utils.logger import log


from computational_experiments.layer_two.instances import get_instance
from demonstrator.example_geometry_line_mapping import geometry_line_mapping
from demonstrator.example_human_factors import human_factor_data
from demonstrator.example_order_data import order_data
from demonstrator.example_start_timestamp import start_timestamp as start_time
from demonstrator.layer_two_worker_line_mdp import CrfWorkerAllocationEnv

experiment_sweep_config = {
    'method': 'grid',
    'metric': {
        'name': 'time',
        'goal': 'minimize'
    },
    'parameters': {
        "approach": {
            'values': ["rl"]
        },
        "num_jobs": {
            'values': [8,7,6,5,4,3,2,1]
        },
        "training_budget": {
            'values': [10_000]
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
                1,
            ]
        }
    }
}


def make_env(layer_one_output, worker_availabilities, resilience, preference, experience, fairness_weight):
    log.info(f"{resilience=}, {preference=}, {experience=}, {fairness_weight=}")
    env = CrfWorkerAllocationEnv(
        previous_step_output=layer_one_output,
        worker_availabilities=worker_availabilities,
        geometry_line_mapping=geometry_line_mapping,
        human_factor_data=human_factor_data,
        start_timestamp=start_time,
        allocate_workers_on_the_same_line_if_possible=False,
        order_data=order_data,
        resilience_weight=resilience,
        preference_weight=preference,
        experience_weight=experience,
        fairness_weight=fairness_weight,
    )

    def mask_fn(env: gym.Env) -> np.ndarray:
        return env.unwrapped.valid_action_mask()

    env = ActionMasker(env, mask_fn)

    env = Monitor(env)
    return env


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


        vec_env = make_vec_env(make_env, n_envs=8, vec_env_cls=DummyVecEnv, env_kwargs={
            "layer_one_output": layer_one,
            "worker_availabilities": availabilities,
            "resilience": RESILIENCE_WEIGHT,
            "preference": PREFERENCE_WEIGHT,
            "experience": EXPERIENCE_WEIGHT,
            "fairness_weight": FAIRNESS_WEIGHT,
        })
        log.info("environment initialized")
        model = sb3_contrib.MaskablePPO(MaskableActorCriticPolicy, vec_env, device="cpu")
        log.info("training the model. This may take a while...")

        training_budget = int(wb.config["training_budget"])

        # print(f"budget: {training_budget}")
        cb = WandbCallback()

        start = time.perf_counter()
        model.learn(total_timesteps=training_budget, callback=[cb], progress_bar=True)
        end = time.perf_counter()

        elapsed = end - start

        wb.log({"training_time": float(elapsed)})

        model.save(f"crf_rl_run_{run.name}.zip")

        times = []

        for idx in range(10):
            test_env = CrfWorkerAllocationEnv(
                previous_step_output=layer_one,
                worker_availabilities=availabilities,
                geometry_line_mapping=geometry_line_mapping,
                human_factor_data=human_factor_data,
                start_timestamp=start_time,
                allocate_workers_on_the_same_line_if_possible=False,
                order_data=order_data,
                resilience_weight=RESILIENCE_WEIGHT,
                preference_weight=PREFERENCE_WEIGHT,
                experience_weight=EXPERIENCE_WEIGHT,
                fairness_weight=FAIRNESS_WEIGHT,
            )
            obs, start_timestamp = test_env.reset()
            done = False

            start = time.perf_counter()

            while not done:
                masks = test_env.valid_action_mask()
                action, _ = model.predict(observation=obs, deterministic=True, action_masks=masks)
                obs, rew, done, turn, info = test_env.step(action)
                log.info(f"Action: {action}, Reward: {rew}")

            end = time.perf_counter()

            elapsed = float(end - start)

            times.append(elapsed)

            wb.log({f"time_{idx}": elapsed})

            allocations_dict = test_env.get_worker_allocation(filter_no_workers_assigned=True)
            log.info(f"Allocations: \n {pprint.pformat(allocations_dict)}")

        average_time = sum(times) / len(times)
        wb.log({"time": average_time})



if __name__ == '__main__':
    sweep_id = wb.sweep(experiment_sweep_config, project="optimizing-fairness", entity="querry")
    wb.agent(sweep_id, function=perform_run, count=8*4, project="optimizing-fairness", entity="querry")


