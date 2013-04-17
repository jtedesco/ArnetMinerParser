import os
from collections import defaultdict

counts_dir = defaultdict(int)
totals_dir = defaultdict(int)

keys_order = []

folder = "intermediate_output-real-040913"
for filename in os.listdir(folder):
    if not filename.endswith('-stats.txt'):
        continue
    input_file = open(os.path.join(folder, filename))
    for line in input_file:
        if not line.startswith('\t'):
            continue

        tokens = line.strip().split()
        count = int(tokens[-4])
        total = int(tokens[-2])

        label = line[:line.find(':')].strip()
        keys_order.append(label)

        counts_dir[label] += count
        totals_dir[label] += total

count_and_percent = lambda a, b: (a, b, float(a) / b * 100)

for key in keys_order[:18]:
    print '%s: %d / %d (%2.2f%%)' % tuple((key,) + count_and_percent(counts_dir[key], totals_dir[key]))
