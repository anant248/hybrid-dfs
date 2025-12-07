import matplotlib.pyplot as plt
import numpy as np

# -------------------- DATASET A --------------------
rainstorm_A_app1_trials = [5000, 5200, 5100]   # dummy, replace
rainstorm_A_app2_trials = [4300, 4100, 4200]

spark_A_app1_trials     = [6400, 6600, 6500]
spark_A_app2_trials     = [5200, 5400, 5300]

# -------------------- DATASET B --------------------
rainstorm_B_app1_trials = [4800, 5000, 4900]
rainstorm_B_app2_trials = [3800, 4000, 3900]

spark_B_app1_trials     = [6900, 7100, 7000]
spark_B_app2_trials     = [5500, 5700, 5600]

# ============================================================
# COMPUTE MEAN + STD FOR EACH APP AND EACH SYSTEM
# ============================================================

def mean_std(trials):
    return np.mean(trials), np.std(trials)

# ----- Dataset A -----
rainstorm_A_means = []
rainstorm_A_stds  = []
spark_A_means     = []
spark_A_stds      = []

for trials in [rainstorm_A_app1_trials, rainstorm_A_app2_trials]:
    m, s = mean_std(trials)
    rainstorm_A_means.append(m)
    rainstorm_A_stds.append(s)

for trials in [spark_A_app1_trials, spark_A_app2_trials]:
    m, s = mean_std(trials)
    spark_A_means.append(m)
    spark_A_stds.append(s)

# ----- Dataset B -----
rainstorm_B_means = []
rainstorm_B_stds  = []
spark_B_means     = []
spark_B_stds      = []

for trials in [rainstorm_B_app1_trials, rainstorm_B_app2_trials]:
    m, s = mean_std(trials)
    rainstorm_B_means.append(m)
    rainstorm_B_stds.append(s)

for trials in [spark_B_app1_trials, spark_B_app2_trials]:
    m, s = mean_std(trials)
    spark_B_means.append(m)
    spark_B_stds.append(s)

# ============================================================
# PLOTTING FUNCTION
# ============================================================

def plot_dataset(dataset_name, rainstorm_means, rainstorm_stds,
                 spark_means, spark_stds, filename):

    applications = ["Application 1 (Filter + Replace)", "Application 2 (Filter + Aggregate)"]
    x = np.arange(len(applications))
    width = 0.35

    plt.figure(figsize=(8, 5))

    plt.bar(x - width/2, rainstorm_means, width,
            yerr=rainstorm_stds, capsize=5, label="RainStorm")

    plt.bar(x + width/2, spark_means, width,
            yerr=spark_stds, capsize=5, label="Spark Streaming")

    plt.xticks(x, applications)
    plt.ylabel("Throughput (tuples/sec)")
    plt.title(f"{dataset_name}: RainStorm vs Spark Streaming")
    plt.legend()
    plt.tight_layout()
    plt.savefig(filename, dpi=300)
    plt.show()


# ============================================================
# GENERATE THE 2 REQUIRED PLOTS
# ============================================================

plot_dataset("Dataset A",
             rainstorm_A_means, rainstorm_A_stds,
             spark_A_means, spark_A_stds,
             "dataset_A_comparison.png")

plot_dataset("Dataset B",
             rainstorm_B_means, rainstorm_B_stds,
             spark_B_means, spark_B_stds,
             "dataset_B_comparison.png")