"""
Shared, deterministic data rules for the join workloads (S4, S5). Imported by
common/producer.py (data generation) and bench/stateful_check.py (ground truth), so the
generator and the checker can never disagree.

S4 reference table: one row per user_id in 1..rows, except user_id % miss_mod == 0 (those
users have no reference row, so an inner join must drop their events). tier/region are a pure
function of user_id: the checker verifies every enriched value without capturing the table.
"""

TIERS = ["bronze", "silver", "gold", "platinum"]
REGIONS = ["na", "latam", "emea", "apac", "anz", "mena", "ssa", "cis"]


def ref_attrs(user_id):
    """(tier, region) of a user_id; Knuth multiplicative hash, stable across languages."""
    h = (int(user_id) * 2654435761) & 0xFFFFFFFF
    return TIERS[h % 4], REGIONS[(h >> 8) % 8]


def has_ref(user_id, rows, miss_mod):
    u = int(user_id)
    return 1 <= u <= rows and (miss_mod <= 0 or u % miss_mod != 0)
