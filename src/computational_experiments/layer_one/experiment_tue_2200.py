import pprint
import time

import pandas as pd
import wandb as wb
from ortools.sat.python import cp_model

from computational_experiments.layer_one.instances import INSTANCES, END_TIMES
from demonstrator.layer_one_order_to_line_solver import main
from demonstrator.layer_one_order_to_line_solver_due_date_adherence import main as main_due_date_adherence
from utils.logger import log
from utils.reproducibility import data_fingerprint, environment_info

from jsp_vis.console import gantt_chart_console

WANDB_PROJECT = "fairness-in-production-planning"
WANDB_ENTITY = "querry"

INSTANCE_NAMES = [f"INSTANCE_TUE_2200_V{i}" for i in range(1, 6)]
OPTIMISATION_STRATEGIES = ["tardiness", "makespan", "balanced", "due_date_adherence"]
N_RUNS = 10
# CP-SAT random seed = run_no; deterministic (interleaved) parallel search makes each run reproducible
DETERMINISTIC = True
# the solver is limited by deterministic time, so that runs hitting the limit are reproducible as well.
# On the development machine ~2.6 deterministic time units correspond to 1 s, i.e. 1500 units ~ 10 min.
DETERMINISTIC_TIME_LIMIT = 1500
# wall clock limit only as a safety net, chosen such that the deterministic limit is reached first
TIME_LIMIT = 20 * 60  # seconds

experiment_sweep_config = {
    'method': 'grid',
    'metric': {
        'name': 'time',
        'goal': 'minimize'
    },
    'parameters': {
        "approach": {
            'values': ["cp"]
        },
        "Instance": {
            'values': INSTANCE_NAMES
        },
        "optimisation": {
            'values': OPTIMISATION_STRATEGIES
        },
        "run_no": {
            'values': list(range(1, N_RUNS + 1))
        }
    }
}

# (makespan_weight, tardiness_weight, earliness_weight). Strategies without earliness use the model of Equation (1)
# (`layer_one_order_to_line_solver`), strategies with earliness the due-date adherence variant
# (`layer_one_order_to_line_solver_due_date_adherence`).
OPTIMISATION_WEIGHTS = {
    "tardiness": (0, 1, 0),
    "makespan": (1, 0, 0),
    "balanced": (1, 1, 0),
    "due_date_adherence": (0, 1, 1),
}


def relative_gap(objective: float, lower_bound: float) -> float:
    """(objective - lower_bound) / objective, in [0, 1] for minimisation; 0 if the objective is 0."""
    if objective == 0:
        return 0.0
    return float((objective - lower_bound) / abs(objective))


class LoggerCB(cp_model.CpSolverSolutionCallback):
    """Log every intermediate solution and the best values found so far."""

    def __init__(self, start_time, optimal_objective):
        cp_model.CpSolverSolutionCallback.__init__(self)
        self.__solution_count = 0
        self.__start_time = start_time
        self.__optimal_objective = optimal_objective
        self.__makespan_var = None
        self.__total_tardiness_var = None
        self.__total_earliness_var = None
        self.__best_makespan = None
        self.__best_total_tardiness = None
        self.__best_total_earliness = None
        self.__best_objective = None

    def set_objective_variables(self, makespan, total_tardiness):
        """Called by `main` before solving."""
        self.__makespan_var = makespan
        self.__total_tardiness_var = total_tardiness

    def set_earliness_variable(self, total_earliness):
        """Called by the due-date adherence variant of `main` before solving."""
        self.__total_earliness_var = total_earliness

    def on_solution_callback(self):
        """Called at each new solution."""
        self.__solution_count += 1
        elapsed = time.perf_counter() - self.__start_time

        objective = self.objective_value
        bound = self.best_objective_bound
        self.__best_objective = objective if self.__best_objective is None else min(self.__best_objective, objective)
        log_dict = {
            # gap the solver can certify: current objective vs. its lower bound
            "intermediate_solver_gap": float(objective - bound),
            "intermediate_solver_gap_relative": relative_gap(objective, bound),
            "run_time": float(elapsed),
            "time": float(elapsed),
            "solver_time": float(self.wall_time),
            "solution_count": self.__solution_count,
            "intermediate_objective": float(objective),
            "best_objective_so_far": float(self.__best_objective),
            "intermediate_best_objective_bound": float(bound),
        }

        if self.__optimal_objective is not None:
            # gap to the known optimum of the constructed instance
            log_dict |= {
                "intermediate_optimality_gap": float(objective - self.__optimal_objective),
                "intermediate_optimality_gap_relative": relative_gap(objective, self.__optimal_objective),
            }

        if self.__total_earliness_var is not None:
            total_earliness = self.value(self.__total_earliness_var)
            self.__best_total_earliness = total_earliness if self.__best_total_earliness is None \
                else min(self.__best_total_earliness, total_earliness)
            log_dict |= {
                "intermediate_total_earliness": total_earliness,
                "best_total_earliness_so_far": self.__best_total_earliness,
            }

        if self.__makespan_var is not None:
            makespan = self.value(self.__makespan_var)
            total_tardiness = self.value(self.__total_tardiness_var)
            self.__best_makespan = makespan if self.__best_makespan is None else min(self.__best_makespan, makespan)
            self.__best_total_tardiness = total_tardiness if self.__best_total_tardiness is None \
                else min(self.__best_total_tardiness, total_tardiness)
            log_dict |= {
                "intermediate_makespan": makespan,
                "intermediate_total_tardiness": total_tardiness,
                "best_makespan_so_far": self.__best_makespan,
                "best_total_tardiness_so_far": self.__best_total_tardiness,
            }

        wb.log(log_dict)


def evaluate_solution(instance: list, solution: list) -> dict:
    """
    Compute operational indicators of a solution returned by `main`: makespan, total/max tardiness, number of tardy
    jobs, total earliness and number of early jobs. Earliness is only part of the objective for the due-date adherence
    strategy.
    """
    makespan = 0
    tardiness = []
    earliness = []
    for elem in solution:
        due_date = next(due for _, line_id, _, due in instance[elem["Task"]] if line_id == elem["Resource"])
        makespan = max(makespan, elem["Finish"])
        tardiness.append(max(0, elem["Finish"] - due_date))
        earliness.append(max(0, due_date - elem["Finish"]))
    return {
        "makespan": makespan,
        "total_tardiness": sum(tardiness),
        "max_tardiness": max(tardiness, default=0),
        "num_tardy_jobs": sum(1 for t in tardiness if t > 0),
        "total_earliness": sum(earliness),
        "num_early_jobs": sum(1 for e in earliness if e > 0),
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

        optimisation_strategy = wb.config["optimisation"]
        if optimisation_strategy not in OPTIMISATION_WEIGHTS:
            raise NotImplementedError(f"unknown optimisation strategy: {optimisation_strategy}")
        makespan_weight, tardiness_weight, earliness_weight = OPTIMISATION_WEIGHTS[optimisation_strategy]

        instance_name = wb.config["Instance"]
        instance = INSTANCES[instance_name]

        # instances are constructed such that makespan == END_TIMES[instance_name] with zero tardiness is optimal.
        # this does not hold if earliness is penalised, so there is no known optimum for the due-date adherence strategy
        optimal_makespan = END_TIMES[instance_name]
        optimal_objective = makespan_weight * optimal_makespan if earliness_weight == 0 else None

        random_seed = int(wb.config["run_no"])

        # fingerprint to verify that runs used identical input data
        run.config.update({"instance_fingerprint": data_fingerprint(instance)} | environment_info())

        wb.log({
            "makespan_weight": float(makespan_weight),
            "tardiness_weight": float(tardiness_weight),
            "earliness_weight": float(earliness_weight),
            "num_operations": len(instance),
            "time_limit": TIME_LIMIT,
            "deterministic_time_limit": DETERMINISTIC_TIME_LIMIT,
            "random_seed": random_seed,
            "deterministic": int(DETERMINISTIC),
            "optimal_makespan": optimal_makespan,
        } | ({"optimal_objective": optimal_objective} if optimal_objective is not None else {}))

        solver_kwargs = dict(
            order_list=instance,
            makespan_weight=makespan_weight,
            tardiness_weight=tardiness_weight,
            verbose=0,
            time_limit=TIME_LIMIT,
            random_seed=random_seed,
            deterministic=DETERMINISTIC,
            deterministic_time_limit=DETERMINISTIC_TIME_LIMIT,
        )

        start = time.perf_counter()
        cb = LoggerCB(start_time=start, optimal_objective=optimal_objective)
        solver_info = {}
        if earliness_weight > 0:
            solution_dict = main_due_date_adherence(
                **solver_kwargs, earliness_weight=earliness_weight, callback=cb, solver_info=solver_info,
            )
        else:
            solution_dict = main(**solver_kwargs, callback=cb, solver_info=solver_info)

        end = time.perf_counter()

        elapsed = end - start
        log.info(f"Elapsed time: {elapsed:.6f} seconds")

        indicators = evaluate_solution(instance, solution_dict)
        objective = makespan_weight * indicators["makespan"] + tardiness_weight * indicators["total_tardiness"] \
            + earliness_weight * indicators["total_earliness"]

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
                **indicators,
                "objective": objective,
            } | ({
                "objective_gap": objective - optimal_objective,
                "objective_gap_relative": relative_gap(objective, optimal_objective),
            } if optimal_objective is not None else {}) | {
                "solver_gap_relative": relative_gap(solver_info["objective"], solver_info["best_objective_bound"]),
                "solver_status": solver_info["status"],
                "solver_is_optimal": int(solver_info["is_optimal"]),
                "solver_is_feasible": int(solver_info["is_feasible"]),
                "solver_objective": solver_info["objective"],
                "solver_best_objective_bound": solver_info["best_objective_bound"],
                "solver_wall_time": solver_info["wall_time"],
                "solver_deterministic_time": solver_info["deterministic_time"],
                "solver_num_branches": solver_info["num_branches"],
                "solver_num_conflicts": solver_info["num_conflicts"],
            }
        )
        run.summary["solver_status"] = solver_info["status"]


if __name__ == '__main__':
    sweep_id = wb.sweep(experiment_sweep_config, project=WANDB_PROJECT, entity=WANDB_ENTITY)
    wb.agent(sweep_id, function=perform_run, count=N_RUNS * len(OPTIMISATION_STRATEGIES) * len(INSTANCE_NAMES),
             project=WANDB_PROJECT, entity=WANDB_ENTITY)
