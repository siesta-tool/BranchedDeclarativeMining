#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 12:29:23 2024

@author: mavroudo
"""

import pandas as pd
from matplotlib import pyplot as plt
plt.rcParams['hatch.linewidth'] = 1
plt.rcParams['xtick.labelsize'] = 20
plt.rcParams['ytick.labelsize'] = 20
plt.rcParams['font.size'] = '20'

# Creating the dataset based on provided values
data = {
    "#Events": [1200000, 12000000, 24000000, 48000000, 72000000, 96000000],
    "Cumulative Index": [99879, 1625883, 3278831, 6416578, 9521048, 12897870],
    "Average per batch": [99879, 162588, 163942, 160414, 158684, 161223]
}

# Convert to DataFrame
df = pd.DataFrame(data)

# Create the plot
fig, ax = plt.subplots(figsize=(12, 6))

ax.bar(df['#Events'], df['Cumulative Index']/1000, color='#2E8A99', alpha=0.8, label='Cumulative index time', width=10e6, edgecolor='black')

for x, y in zip(df['#Events'], df['Cumulative Index']/1000):
    ax.text(x, y*1.05, f'{y:,.0f}', ha='center', va='bottom', fontsize=20)
    
for x, y in zip(df['#Events'], df['Average per batch']/1000):
    ax.text(x, y * 1.05, f'{y:,.0f}', ha='center', va='bottom', color='#191970', fontsize=20)
          
# Line chart for average per batch values
ax.plot(df['#Events'], df['Average per batch']/1000, color='#191970', marker='o', linestyle='-', label='Average index time per batch')

# Set log scale for y-axis
ax.set_yscale('log')

# Set labels and title
ax.set_xlabel('#Events (in millions)')
ax.set_ylabel('Time (s)')

# Show legends
ax.legend(loc='upper left')

# Adding x-ticks for clarity
ax.set_xticks(df['#Events'])
ax.set_xticklabels(df['#Events']/1e6)
plt.ylim([0,100000])
plt.tight_layout()
# Show the graph
plt.show()