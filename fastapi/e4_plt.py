import matplotlib.pyplot as plt
import numpy as np

users = [1, 3, 6]
p50 = [490, 560, 1800]
p95 = [560, 620, 2300]
p99 = [720, 620, 2400]

x = np.arange(len(users))
width = 0.25

fig, ax = plt.subplots(figsize=(7, 5))
ax.bar(x - width, p50, width, label='p50 (медиана)', color='#2ecc71')
ax.bar(x, p95, width, label='p95', color='#f39c12')
ax.bar(x + width, p99, width, label='p99', color='#e74c3c')

# Добавляем подписи значений
for i, (v50, v95, v99) in enumerate(zip(p50, p95, p99)):
    ax.text(i - width, v50 + 30, str(v50), ha='center', fontsize=8)
    ax.text(i, v95 + 30, str(v95), ha='center', fontsize=8)
    ax.text(i + width, v99 + 30, str(v99), ha='center', fontsize=8)

ax.set_xlabel('Одновременных пользователей')
ax.set_ylabel('Время отклика, мс')
ax.set_title('Процентили времени отклика при различной нагрузке (Locust)')
ax.set_xticks(x)
ax.set_xticklabels(users)
ax.legend()
ax.grid(True, alpha=0.3, axis='y')
ax.axhline(y=2000, color='red', linestyle='--', alpha=0.5, label='Целевой порог НФТ-2 (2000 мс)')
ax.legend()
plt.tight_layout()
plt.savefig('e4_latency.png', dpi=150)