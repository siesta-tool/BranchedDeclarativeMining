import matplotlib.pyplot as plt

# Data
events = [15214, 72151, 262200]
and_times = [1349, 33847, 70313]
or_times = [19107, 49374, 55604]
xor_times = [1563, 23661, 85793]

# Line Plot
plt.figure(figsize=(7, 5))
plt.plot(events, and_times, label="AND", marker='o', color='blue', markersize=8, linewidth=2)
plt.plot(events, or_times, label="OR", marker='o', color='red', markersize=8, linewidth=2)
plt.plot(events, xor_times, label="XOR", marker='o', color='orange', markersize=8, linewidth=2)

# Labels and title
plt.xlabel("# Events", fontsize=14)
plt.ylabel("Time (ms)", fontsize=14)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plt.legend(fontsize=12)

# Save the figure
plt.savefig("exp2a.png", dpi=300, bbox_inches='tight')
plt.show()

# Box Plot
data = [and_times, or_times, xor_times]

fig, ax = plt.subplots(figsize=(7, 5))
box = ax.boxplot(data, patch_artist=True, labels=["AND", "OR", "XOR"], medianprops={'color': 'black', 'linewidth': 2})

# Customizing colors
colors = ['blue', 'red', 'gold']
for patch, color in zip(box['boxes'], colors):
    patch.set(facecolor=color, alpha=0.6)

# Labels and title
ax.set_xlabel("Branching Policy", fontsize=14)
ax.set_ylabel("Time (ms)", fontsize=14)
ax.tick_params(axis='both', labelsize=12)

# Save the figure
plt.savefig("exp2b.png", dpi=300, bbox_inches='tight')
plt.show()
