import wandb
import numpy as np


def compute_boxplot_stats(data):
    data = np.array(sorted(data))

    q1 = np.percentile(data, 25)
    median = np.percentile(data, 50)
    q3 = np.percentile(data, 75)

    iqr = q3 - q1

    # Tukey whiskers (1.5 * IQR rule)
    lower_whisker = data[data >= (q1 - 1.5 * iqr)].min()
    upper_whisker = data[data <= (q3 + 1.5 * iqr)].max()

    return lower_whisker, q1, median, q3, upper_whisker


def get_sweep_metric_values(entity, project, sweep_id, metric_name="run_time") -> dict[int,list[float]]:
    api = wandb.Api()

    # Load sweep
    sweep = api.sweep(f"{entity}/{project}/{sweep_id}")

    res_dict = {
        1: [],
        2: [],
        3: [],
        4: [],
        5: [],
        6: [],
        7: [],
        8: [],
    }

    # Loop over runs in the sweep
    for num, run in enumerate(sweep.runs):
        print(f"handling run '{run}' (no. {num})")

        full_run = api.run("/".join(run.path))
        config = dict(full_run.config)
        summary = dict(full_run.summary)
        print(summary)
        val = float(summary.get("time"))
        print(f"value: {val}")



        num_jobs = int(config.get("num_jobs"))
        print(config)
        print(f'num_jobs: {num_jobs}')

        res_dict[num_jobs].append(val)

    return res_dict


if __name__ == "__main__":
    ENTITY = "querry"  # e.g. your username or team
    PROJECT = "optimizing-fairness"
    SWEEP_ID = "7hfvv44m"

    res_dict = get_sweep_metric_values(ENTITY, PROJECT, SWEEP_ID)

    #print([(0, y) for y in data])

    for num_jobs, data in res_dict.items():
        if not data:
            raise ValueError("No data found")

        lw, q1, med, q3, uw = compute_boxplot_stats(data)

        # Example extra point (e.g. a highlighted run)
        outlier_x = num_jobs
        outlier_y = np.mean(data)

        tikz_code = f"""
        \\addplot+[boxplot prepared={{
            draw position={num_jobs},
            lower whisker={lw:.6f},
            lower quartile={q1:.6f},
            median={med:.6f},
            upper quartile={q3:.6f},
            upper whisker={uw:.6f}
        }}, draw=text, fill=lavender, fill opacity=0.6, solid] coordinates {{}};

        \\addplot+[only marks, red, mark=diamond*] coordinates {{({outlier_x},{outlier_y:.6f})}};
        """

        print(tikz_code)


