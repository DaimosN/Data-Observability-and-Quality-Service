import matplotlib.pyplot as plt

sizes = [100, 500, 1000, 5000, 10000]
throughput = [825.0, 1482.8, 2053.2, 2591.8, 2782.1]

fig, ax = plt.subplots(figsize=(8, 5))
ax.plot(sizes, throughput, 'o-', color='#2e86c1', linewidth=2, markersize=8)
ax.set_xlabel('Размер файла, записей')
ax.set_ylabel('Throughput, записей/с')
ax.set_title('Зависимость пропускной способности от размера файла')
ax.grid(True, alpha=0.3)
for x, y in zip(sizes, throughput):
    ax.annotate(f'{y:.0f}', (x, y), textcoords="offset points", xytext=(0, 10), ha='center')
plt.tight_layout()
plt.savefig('e2_throughput.png', dpi=150)