"""
Generator for the Worker-Line allocation scenarios in `instances.py`.

The layer one input is fixed: INSTANCE_TUE_2200_V1 solved with the makespan objective (proven optimal, makespan
2400 = Tuesday 22:00) using CP-SAT's deterministic search with a fixed seed (LAYER_ONE_SEED), converted from solver
time to timestamps. Layer two works on timestamps, starting at
START_TIMESTAMP (Monday 2025-09-29 06:00 UTC).

For each scenario (seed) the following layer two inputs are generated to match the layer one output:

- geometry_line_mapping: main line = the fastest line of the geometry in the layer one instance, alternative lines =
  the remaining lines; the required number of workers per geometry is drawn from [3, 6].
- worker_availabilities: three shift crews (06:00-14:00, 14:00-22:00, 22:00-06:00). Each crew works its shift on every
  day within the planning horizon. A crew has as many workers as its most demanding interval requires plus 2-6 spare
  workers. Each worker is absent on a given shift with probability P_ABSENT.
- human_factor_data: experience and resilience ~ U[0, 1] for every worker and geometry; a worker is not medically
  cleared for a geometry with probability P_MEDICAL_NOT_CLEARED. The preference is correlated with experience and
  resilience, depending on the worker type (worker_types):
    - "conventional" (1 - SHARE_CHALLENGE_SEEKING of the workers) prefer tasks they are experienced in and that are
      less demanding: preference = (experience + resilience) / 2 + noise. With probability P_PREFERENCE_EXCEPTION a
      conventional worker dislikes a task regardless (preference ~ U[0, DISLIKE_MAX_PREFERENCE]), i.e. there are tasks
      a worker is experienced in, but does not like.
    - "challenge_seeking" (SHARE_CHALLENGE_SEEKING of the workers) prefer demanding tasks (low resilience):
      preference = (1 - resilience) + noise.
  noise ~ N(0, PREFERENCE_NOISE_STD); preferences are clipped to [0, 1].

Absences and medical restrictions are repaired afterwards such that every interval can be fully staffed (checked with a
bipartite matching of required slots to available, medically cleared workers).

The environment checks that the produced amounts add up exactly to the order amounts, so the throughput is set to 60
units per hour (one unit per minute) and the order amounts are derived from the scheduled durations minus setup time.

Run this module to regenerate `instances.py`.
"""
import datetime
import pathlib
import pprint
import random

from computational_experiments.layer_one.instances import INSTANCES, END_TIMES
from demonstrator.layer_one_order_to_line_solver import main
from utils.crf_timestamp_solver_time_conversion import solver_time_to_timestamp

START_TIMESTAMP = 1759125600  # Monday 2025-09-29 06:00:00 UTC
LAYER_ONE_INSTANCE_NAME = "INSTANCE_TUE_2200_V1"
LAYER_ONE_SEED = 0  # CP-SAT random seed for solving the layer one instance
SCENARIO_SEEDS = [1, 2, 3, 4, 5]

SHIFT = 480
MOLD = 4  # the environment uses 15 minutes of setup time per mold
SETUP_TIME = MOLD * 15
THROUGHPUT = 60  # units per hour

MIN_WORKERS_PER_GEOMETRY = 3
MAX_WORKERS_PER_GEOMETRY = 6
MIN_SPARE_WORKERS = 2
MAX_SPARE_WORKERS = 6
P_ABSENT = 0.1
P_MEDICAL_NOT_CLEARED = 0.1

SHARE_CHALLENGE_SEEKING = 0.1  # share of workers that prefer demanding tasks
P_PREFERENCE_EXCEPTION = 0.1  # probability that a conventional worker dislikes a task regardless
DISLIKE_MAX_PREFERENCE = 0.3
PREFERENCE_NOISE_STD = 0.1


def draw_preference(rng: random.Random, worker_type: str, experience: float, resilience: float) -> float:
    if worker_type == "challenge_seeking":
        preference = (1 - resilience) + rng.gauss(0, PREFERENCE_NOISE_STD)
    elif rng.random() < P_PREFERENCE_EXCEPTION:
        preference = rng.uniform(0, DISLIKE_MAX_PREFERENCE)
    else:
        preference = (experience + resilience) / 2 + rng.gauss(0, PREFERENCE_NOISE_STD)
    return round(min(1.0, max(0.0, preference)), 2)


def geometry_name(g_idx: int) -> str:
    return f"Geometry {g_idx + 1}"


def order_name(g_idx: int) -> str:
    return f"Order {g_idx + 1}"


def line_name(line_id: int) -> str:
    return f"Line {line_id + 1}"


def solve_layer_one(instance_name: str) -> list[dict]:
    """Solve the layer one instance with the makespan objective. Returns the solution in solver time."""
    solver_info = {}
    # seeded, deterministic search: regenerating yields the same layer one solution
    solution = main(order_list=INSTANCES[instance_name], makespan_weight=1, tardiness_weight=0, verbose=0,
                    solver_info=solver_info, random_seed=LAYER_ONE_SEED, deterministic=True)
    assert solver_info["status"] == "OPTIMAL", f"layer one not solved to optimality: {solver_info}"
    assert max(elem["Finish"] for elem in solution) == END_TIMES[instance_name]
    return sorted(solution, key=lambda elem: (elem["Resource"], elem["Start"]))


def layer_one_output_from_solution(solution: list[dict]) -> list[dict]:
    """Convert a layer one solution (solver time) into the timestamp based layer two input."""
    return [
        {
            'Finish': solver_time_to_timestamp(solver_time=elem["Finish"], start_timestamp=START_TIMESTAMP),
            'Order': order_name(elem["Task"]),
            'Resource': line_name(elem["Resource"]),
            'Start': solver_time_to_timestamp(solver_time=elem["Start"], start_timestamp=START_TIMESTAMP),
            'Task': f"{order_name(elem['Task'])} × {geometry_name(elem['Task'])}",
            'geometry': geometry_name(elem["Task"]),
        }
        for elem in solution
    ]


def order_data_from_solution(instance: list, solution: list[dict]) -> list[dict]:
    order_data = []
    for elem in sorted(solution, key=lambda e: e["Task"]):
        g_idx = elem["Task"]
        due_date = instance[g_idx][0][3]
        production_minutes = elem["Finish"] - elem["Start"] - SETUP_TIME
        assert production_minutes > 0
        order_data.append({
            'amount': production_minutes * THROUGHPUT // 60,
            'deadline': solver_time_to_timestamp(solver_time=due_date, start_timestamp=START_TIMESTAMP),
            'geometry': geometry_name(g_idx),
            'mold': MOLD,
            'order': order_name(g_idx),
            'priority': 'false',
        })
    return order_data


def throughput_mapping_from_instance(instance: list) -> list[dict]:
    return [
        {'geometry': geometry_name(g_idx), 'line': line_name(line_id), 'throughput': THROUGHPUT}
        for g_idx, alternatives in enumerate(instance)
        for _, line_id, _, _ in sorted(alternatives, key=lambda a: a[1])
    ]


def max_matching(slots: list[str], workers: list[str], is_cleared) -> int:
    """Maximum bipartite matching of slots (geometries) to workers (Kuhn's algorithm)."""
    worker_to_slot = {}

    def try_assign(slot_idx: int, visited: set) -> bool:
        for worker in workers:
            if worker in visited or not is_cleared(worker, slots[slot_idx]):
                continue
            visited.add(worker)
            if worker not in worker_to_slot or try_assign(worker_to_slot[worker], visited):
                worker_to_slot[worker] = slot_idx
                return True
        return False

    return sum(try_assign(slot_idx, set()) for slot_idx in range(len(slots)))


def generate_scenario(instance: list, solution: list[dict], seed: int) -> dict:
    rng = random.Random(seed)
    n_geometries = len(instance)
    geometries = [geometry_name(g_idx) for g_idx in range(n_geometries)]
    end = max(elem["Finish"] for elem in solution)

    # geometry line mapping
    geometry_line_mapping = []
    required_workers = {}
    for g_idx, alternatives in enumerate(instance):
        lines_by_duration = [line_id for _, line_id, _, _ in sorted(alternatives, key=lambda a: (a[0], a[1]))]
        n_workers = rng.randint(MIN_WORKERS_PER_GEOMETRY, MAX_WORKERS_PER_GEOMETRY)
        required_workers[geometry_name(g_idx)] = n_workers
        geometry_line_mapping.append({
            "geometry": geometry_name(g_idx),
            "main_line": line_name(lines_by_duration[0]),
            "alternative_lines": [line_name(line_id) for line_id in lines_by_duration[1:]],
            "number_of_workers": n_workers,
        })

    # shifts within the planning horizon: (start, end, crew), crew 0 = early, 1 = late, 2 = night
    shifts = [(start, start + SHIFT, (start // SHIFT) % 3) for start in range(0, end, SHIFT)]

    # elementary segments with the slots (one entry per required worker) that have to be filled
    bounds = sorted({0, end} | {s for s, _, _ in shifts} | {e["Start"] for e in solution} | {e["Finish"] for e in solution})
    segments = []
    for seg_start, seg_end in zip(bounds[:-1], bounds[1:]):
        slots = [geometry_name(e["Task"]) for e in solution if e["Start"] < seg_end and seg_start < e["Finish"]
                 for _ in range(required_workers[geometry_name(e["Task"])])]
        shift_idx = seg_start // SHIFT
        segments.append((shift_idx, slots))

    # crews
    crews = {}
    worker_no = 1
    for crew in range(3):
        crew_demand = max((len(slots) for shift_idx, slots in segments if shifts[shift_idx][2] == crew), default=0)
        crew_size = crew_demand + rng.randint(MIN_SPARE_WORKERS, MAX_SPARE_WORKERS)
        crews[crew] = [f"w_{worker_no + i}" for i in range(crew_size)]
        worker_no += crew_size
    all_workers = [w for crew in range(3) for w in crews[crew]]

    absent = {(w, shift_idx) for shift_idx, (_, _, crew) in enumerate(shifts) for w in crews[crew]
              if rng.random() < P_ABSENT}

    challenge_seeking_workers = set(rng.sample(all_workers, round(SHARE_CHALLENGE_SEEKING * len(all_workers))))
    worker_types = {w: "challenge_seeking" if w in challenge_seeking_workers else "conventional" for w in all_workers}

    human_factors = {}
    for w in all_workers:
        for g in geometries:
            experience = round(rng.random(), 2)
            resilience = round(rng.random(), 2)
            human_factors[(w, g)] = {
                'experience': experience,
                'medical_condition': 'false' if rng.random() < P_MEDICAL_NOT_CLEARED else 'true',
                'preference': draw_preference(rng, worker_types[w], experience, resilience),
                'resilience': resilience,
            }

    def is_cleared(worker: str, geometry: str) -> bool:
        return human_factors[(worker, geometry)]['medical_condition'] == 'true'

    # repair absences and medical restrictions until every segment can be fully staffed
    for shift_idx, slots in segments:
        crew_workers = crews[shifts[shift_idx][2]]
        while True:
            available = [w for w in crew_workers if (w, shift_idx) not in absent]
            if max_matching(slots, available, is_cleared) == len(slots):
                break
            absent_workers = [w for w in crew_workers if (w, shift_idx) in absent]
            if len(available) < len(slots) or not any(not is_cleared(w, g) for w in available for g in slots):
                absent.discard((rng.choice(absent_workers), shift_idx))
            else:
                worker, geometry = rng.choice([(w, g) for w in available for g in set(slots) if not is_cleared(w, g)])
                human_factors[(worker, geometry)]['medical_condition'] = 'true'

    worker_availabilities = []
    for shift_idx, (shift_start, shift_end, crew) in enumerate(shifts):
        from_timestamp = solver_time_to_timestamp(solver_time=shift_start, start_timestamp=START_TIMESTAMP)
        date = datetime.datetime.fromtimestamp(from_timestamp, datetime.timezone.utc).strftime("%Y-%m-%d")
        for w in crews[crew]:
            if (w, shift_idx) in absent:
                continue
            worker_availabilities.append({
                'date': date,
                'end_timestamp': solver_time_to_timestamp(solver_time=shift_end, start_timestamp=START_TIMESTAMP),
                'from_timestamp': from_timestamp,
                'worker': w,
            })

    human_factor_data = [
        {'experience': hf['experience'], 'geometry': g, 'medical_condition': hf['medical_condition'],
         'preference': hf['preference'], 'resilience': hf['resilience'], 'worker': w}
        for (w, g), hf in human_factors.items()
    ]

    return {
        "seed": seed,
        "geometry_line_mapping": geometry_line_mapping,
        "worker_availabilities": worker_availabilities,
        "human_factor_data": human_factor_data,
        "worker_types": worker_types,
    }


def main_generate() -> None:
    instance = INSTANCES[LAYER_ONE_INSTANCE_NAME]
    solution = solve_layer_one(LAYER_ONE_INSTANCE_NAME)

    layer_one_output = layer_one_output_from_solution(solution)
    order_data = order_data_from_solution(instance, solution)
    throughput_mapping = throughput_mapping_from_instance(instance)
    scenarios = {f"SCENARIO_{seed}": generate_scenario(instance, solution, seed) for seed in SCENARIO_SEEDS}

    def fmt(obj) -> str:
        return pprint.pformat(obj, width=160, sort_dicts=False)

    blocks = [
        '"""\n'
        'Worker-Line allocation (layer two) scenarios for the layer one instance INSTANCE_TUE_2200_V1.\n\n'
        'LAYER_ONE_OUTPUT is the makespan-optimal layer one solution (makespan 2400 = Tuesday 22:00) in timestamps\n'
        '(UTC, starting at START_TIMESTAMP = Monday 2025-09-29 06:00). It is the same for all scenarios.\n'
        'SCENARIOS contains the layer two inputs (geometry line mapping, worker availabilities, human factors)\n'
        'that differ per scenario (seed).\n\n'
        'Generated by computational_experiments/layer_two/instance_generator.py -- do not edit by hand.\n'
        '"""',
        f'START_TIMESTAMP = {START_TIMESTAMP}  # Monday 2025-09-29 06:00:00 UTC',
        f'LAYER_ONE_INSTANCE_NAME = "{LAYER_ONE_INSTANCE_NAME}"',
        f'# layer one solution in solver time (Task = geometry index, Resource = line index)\n'
        f'LAYER_ONE_SOLUTION_SOLVER_TIME = {fmt(solution)}',
        f'LAYER_ONE_OUTPUT = {fmt(layer_one_output)}',
        f'ORDER_DATA = {fmt(order_data)}',
        f'THROUGHPUT_MAPPING = {fmt(throughput_mapping)}',
    ]
    for name, scenario in scenarios.items():
        n_workers = len({w["worker"] for w in scenario["worker_availabilities"]})
        blocks.append(f'# seed {scenario["seed"]}, {n_workers} workers\n{name} = {fmt(scenario)}')
    blocks.append("SCENARIOS = {\n" + "".join(f'    "{name}": {name},\n' for name in scenarios) + "}")

    out = pathlib.Path(__file__).parent / "instances.py"
    out.write_text("\n\n\n".join(blocks) + "\n")
    print(f"wrote {len(scenarios)} scenarios to {out}")


if __name__ == "__main__":
    main_generate()
