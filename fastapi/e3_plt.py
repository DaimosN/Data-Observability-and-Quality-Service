import matplotlib.pyplot as plt
import numpy as np

sizes = [500, 1000, 5000]
insert = [0.152, 0.310, 1.466]
executemany = [0.156, 0.284, 1.608]
copy = [0.051, 0.038, 0.062]

x = np.arange(len(sizes))
width = 0.25

fig, ax = plt.subplots(figsize=(8, 5))
bars1 = ax.bar(x - width, insert, width, label='INSERT', color='#e74c3c')
bars2 = ax.bar(x, executemany, width, label='executemany', color='#f39c12')
bars3 = ax.bar(x + width, copy, width, label='COPY', color='#2ecc71')

ax.set_xlabel('Размер файла, записей')
ax.set_ylabel('Время, с')
ax.set_title('Сравнение времени выполнения стратегий записи')
ax.set_xticks(x)
ax.set_xticklabels(sizes)
ax.legend()
ax.grid(True, alpha=0.3, axis='y')

# Подписи значений
for bar in bars1:
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02, f'{bar.get_height():.3f}',
            ha='center', va='bottom', fontsize=8)
for bar in bars2:
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02, f'{bar.get_height():.3f}',
            ha='center', va='bottom', fontsize=8)
for bar in bars3:
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02, f'{bar.get_height():.3f}',
            ha='center', va='bottom', fontsize=8)

plt.tight_layout()
plt.savefig('e3_strategies.png', dpi=150)