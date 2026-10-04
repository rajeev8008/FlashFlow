"""Seeded five-minute retail demand bins shared by dataset and live demo."""

import math
import random

KINDS = ("NORMAL", "FLASH_SALE", "DEMAND_SPIKE", "RESTOCK")


def demand_bin(
    rng, minute, popularity, price_ratio=1, kind="NORMAL", strength=4, progress=0
):
    seasonal = 1 + 0.35 * math.sin(minute / 1440 * 2 * math.pi)
    multiplier = 1
    if kind == "FLASH_SALE":
        multiplier = 1 + (strength - 1) * (0.6 + 0.4 * min(1, progress * 3))
    elif kind == "DEMAND_SPIKE":
        multiplier = 1 + (strength - 1) * math.exp(-(((progress - 0.35) / 0.2) ** 2))
    mean = max(0.05, popularity * seasonal * multiplier / max(0.5, price_ratio) ** 0.7)
    # Stdlib Poisson; bounded means keep simulation proportional to demo scope.
    limit, n, p = math.exp(-min(mean, 50)), 0, 1.0
    while p > limit:
        n += 1
        p *= rng.random()
    return max(0, n - 1)


def scenario_plan(seed, kind, bins, strength=4, popularity=1.2, start_minute=600):
    if kind not in KINDS or not 1 <= bins <= 120 or not 1 <= strength <= 10:
        raise ValueError("Invalid scenario")
    rng = random.Random(seed)
    return [
        demand_bin(
            rng,
            start_minute + i * 5,
            popularity,
            # Flash demonstrations include an observed normal warm-up, then a ramp.
            kind="NORMAL" if kind == "FLASH_SALE" and i < bins // 3 else kind,
            strength=strength,
            progress=max(0, i - bins // 3) / max(1, bins - bins // 3 - 1)
            if kind == "FLASH_SALE"
            else i / max(1, bins - 1),
        )
        for i in range(bins)
    ]
