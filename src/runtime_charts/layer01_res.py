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


def get_sweep_metric_values(entity, project, sweep_id, metric_name="time"):
    api = wandb.Api()

    # Load sweep
    sweep = api.sweep(f"{entity}/{project}/{sweep_id}")

    values = []

    # Loop over runs in the sweep
    for num, run in enumerate(sweep.runs):
        print(f"handling run '{run}' (no. {num})")
        # Use summary (final logged value)
        value = run.summary.get(metric_name)

        # If not in summary, optionally fall back to history
        if value is None:
            history = run.scan_history(keys=[metric_name])
            history_values = [row[metric_name] for row in history if metric_name in row]
            if history_values:
                value = history_values[-1]

        if value is not None:
            values.append(float(value))

    return values


if __name__ == "__main__":
    ENTITY = "querry"  # e.g. your username or team
    PROJECT = "optimizing-fairness"
    SWEEP_ID = "5smftk9v"

    data = get_sweep_metric_values(ENTITY, PROJECT, SWEEP_ID)

    print([(0, y) for y in data])

    if not data:
        raise ValueError("No data found for run_time")

    lw, q1, med, q3, uw = compute_boxplot_stats(data)

    # Example extra point (e.g. a highlighted run)
    outlier_x = 0
    outlier_y = np.mean(data)

    tikz_code = f"""
    \\addplot+[boxplot prepared={{
        draw position=1,
        lower whisker={lw:.6f},
        lower quartile={q1:.6f},
        median={med:.6f},
        upper quartile={q3:.6f},
        upper whisker={uw:.6f}
    }}, draw=text, fill=lavender, fill opacity=0.6, solid] coordinates {{}};

    \\addplot+[only marks, red, mark=diamond*] coordinates {{({outlier_x},{outlier_y:.6f})}};
    """

    print(tikz_code)
