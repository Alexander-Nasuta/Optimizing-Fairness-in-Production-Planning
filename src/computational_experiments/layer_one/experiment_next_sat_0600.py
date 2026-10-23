import copy

import wandb as wb

from computational_experiments.layer_one.experiment_tue_2200 import experiment_sweep_config as tue_sweep_config, \
    perform_run, OPTIMISATION_STRATEGIES, N_RUNS, WANDB_PROJECT, WANDB_ENTITY

INSTANCE_NAMES = [f"INSTANCE_NEXT_SAT_0600_V{i}" for i in range(1, 6)]

experiment_sweep_config = copy.deepcopy(tue_sweep_config)
experiment_sweep_config['parameters']["Instance"] = {'values': INSTANCE_NAMES}


if __name__ == '__main__':
    sweep_id = wb.sweep(experiment_sweep_config, project=WANDB_PROJECT, entity=WANDB_ENTITY)
    wb.agent(sweep_id, function=perform_run, count=N_RUNS * len(OPTIMISATION_STRATEGIES) * len(INSTANCE_NAMES),
             project=WANDB_PROJECT, entity=WANDB_ENTITY)
