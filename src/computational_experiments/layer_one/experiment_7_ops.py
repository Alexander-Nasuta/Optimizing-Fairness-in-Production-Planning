import pprint
import time

import pandas as pd
import wandb
import wandb as wb
from ortools.sat.python import cp_model

from computational_experiments.layer_one.instances import EXAMPLE_INSTANCE_11_OPTIONS_V5, \
    EXAMPLE_INSTANCE_12_OPTIONS_V1, EXAMPLE_INSTANCE_12_OPTIONS_V2, EXAMPLE_INSTANCE_12_OPTIONS_V3, \
    EXAMPLE_INSTANCE_12_OPTIONS_V4, EXAMPLE_INSTANCE_12_OPTIONS_V5, EXAMPLE_INSTANCE_1_OPTIONS_V1, \
    EXAMPLE_INSTANCE_1_OPTIONS_V2, EXAMPLE_INSTANCE_1_OPTIONS_V3, EXAMPLE_INSTANCE_1_OPTIONS_V4, \
    EXAMPLE_INSTANCE_1_OPTIONS_V5, EXAMPLE_INSTANCE_7_OPTIONS_V1, EXAMPLE_INSTANCE_7_OPTIONS_V2, \
    EXAMPLE_INSTANCE_7_OPTIONS_V3, EXAMPLE_INSTANCE_7_OPTIONS_V4, EXAMPLE_INSTANCE_7_OPTIONS_V5
from demonstrator.layer_one_order_to_line_solver import main, line_id_mapping_to_line_str_mapping, \
    task_idx_to_task_key_mapping, start_timestamp
from utils.crf_timestamp_solver_time_conversion import solver_time_to_timestamp
from utils.logger import log

from jsp_vis.console import gantt_chart_console


experiment_sweep_config = {
    'method': 'grid',
    'metric': {
        'name': 'time',
        'goal': 'minimize'
    },
    'parameters': {
        "no_op": {
            'values': [7]
        },
        "approach": {
            'values': ["cp"]
        },
        "Instance": {
            'values': [
                "EXAMPLE_INSTANCE_7_OPTIONS_V1",
                "EXAMPLE_INSTANCE_7_OPTIONS_V2",
                "EXAMPLE_INSTANCE_7_OPTIONS_V3",
                "EXAMPLE_INSTANCE_7_OPTIONS_V4",
                "EXAMPLE_INSTANCE_7_OPTIONS_V5",
            ]
        },
        "optimisation": {
            'values': [
                "tardiness",
                "makespan",
                "balanced",
            ]
        },
        "run_no": {
            'values': [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10
            ]
        }
    }
}

INSTANCES = {
    "EXAMPLE_INSTANCE_7_OPTIONS_V1": EXAMPLE_INSTANCE_7_OPTIONS_V1,
    "EXAMPLE_INSTANCE_7_OPTIONS_V2": EXAMPLE_INSTANCE_7_OPTIONS_V2,
    "EXAMPLE_INSTANCE_7_OPTIONS_V3": EXAMPLE_INSTANCE_7_OPTIONS_V3,
    "EXAMPLE_INSTANCE_7_OPTIONS_V4": EXAMPLE_INSTANCE_7_OPTIONS_V4,
    "EXAMPLE_INSTANCE_7_OPTIONS_V5": EXAMPLE_INSTANCE_7_OPTIONS_V5,
}

class LoggerCB(cp_model.CpSolverSolutionCallback):
    """Print intermediate solutions."""

    def __init__(self, start_time):
        cp_model.CpSolverSolutionCallback.__init__(self)
        self.__solution_count = 0
        self.__start_time = start_time

    def on_solution_callback(self):
        """Called at each new solution."""
        self.__solution_count += 1
        elapsed = time.perf_counter() - self.__start_time
        wandb.log(
            {
                "run_time": float(elapsed),
                "time": float(elapsed),

                "solution_count": self.__solution_count,
            }
        )



def perform_run():
    with wb.init(
            sync_tensorboard=False,
            monitor_gym=False,
            save_code=True,
    ) as run:
        log.info(f"run name: {run.name}, run id: {run.id}")

        experiment_params = wb.config
        log.info(f"experiment params: {pprint.pformat(experiment_params)}")


        optimisation_strategy = wb.config["optimisation"]

        if optimisation_strategy == "tardiness":
            makespan_weight=0
            tardiness_weight = 1
        elif optimisation_strategy == "makespan":
            makespan_weight = 1
            tardiness_weight = 0
        elif optimisation_strategy == "balanced":
            makespan_weight = 1
            tardiness_weight = 1
        else:
            raise NotImplementedError("")

        instance_name = wb.config["Instance"]

        instance = INSTANCES[instance_name]


        wb.log({
            "makespan_weight": float(makespan_weight),
            "tardiness_weight": float(tardiness_weight),
            "num_operations": len(instance),
        })

        start = time.perf_counter()
        cb = LoggerCB(start_time=start)
        solution_dict = main(
            order_list=instance,
            makespan_weight=makespan_weight,
            tardiness_weight=tardiness_weight,
            verbose=0,
            callback=cb
        )

        end = time.perf_counter()

        elapsed = end - start
        log.info(f"Elapsed time: {elapsed:.6f} seconds")

        dict_for_gantt = [
            elem | {
                'Resource': f'Line {elem["Resource"]}'
            } for elem in solution_dict
        ]
        log.info(pprint.pformat(dict_for_gantt))
        gantt_chart_console(pd.DataFrame(dict_for_gantt), n_machines=3, resource_naming='Line')

        wb.log(
            {
                "run_time": float(elapsed),
                "time": float(elapsed),
            }
        )


if __name__ == '__main__':
    #sweep_id = "zl1hk7jf"
    sweep_id = wb.sweep(experiment_sweep_config, project="optimizing-fairness", entity="querry")
    wb.agent(sweep_id, function=perform_run, count=10*3*5, project="optimizing-fairness", entity="querry")



