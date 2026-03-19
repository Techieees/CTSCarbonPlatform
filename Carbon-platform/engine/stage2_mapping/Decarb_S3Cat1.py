from __future__ import annotations

from typing import Mapping

import numpy as np
import pandas as pd


def build_s3_cat1_bau_and_decarb(
    df: pd.DataFrame,
    *,
    ef_reduction_by_id: Mapping[str, float],
    scenario_col: str = "Scenario",
    ef_scenario_col: str = "EF_Scenario",
    emissions_col: str = "Emissions_tCO2e",
    emissions_spend_col: str = "Emissions_tCO2e_spend_based",
    bau_name: str = "BAU",
    decarb_name: str = "DECARB",
    ef_id_col: str = "ef_id",
    ef_value_col: str = "ef_value",
    spend_col: str = "Spend_Euro",
    baseline_emissions_col: str = "co2e (t)",
) -> pd.DataFrame:
    """
    Build BAU and DECARB scenarios for Scope 3 Category 1 (Purchased Goods & Services).

    Rules are defined by ef_reduction_by_id:
      - keys: ef_id string
      - values: reduction fraction (e.g. 0.30 means reduce EF by 30%)

    Output:
      - returns a single dataframe with BAU and DECARB stacked (row-wise)
      - original columns are kept unchanged
      - adds: Scenario, EF_Scenario, Emissions_tCO2e, Emissions_tCO2e_spend_based
    """
    if df is None:
        raise ValueError("df cannot be None")

    # We must be able to identify the group key (ef_id or dummy_ef_id) and we must have baseline emissions.
    # ef_value/spend are optional; they are not used for the primary DECARB calculation because
    # dummy groups can aggregate multiple sources with different ef_value values.
    missing = [c for c in (ef_id_col, baseline_emissions_col) if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Normalize inputs without mutating caller's dataframe
    base = df.copy(deep=False)
    ef_id = base[ef_id_col].astype("string")

    has_ef_value = ef_value_col in base.columns
    has_spend = spend_col in base.columns
    ef_value = pd.to_numeric(base[ef_value_col], errors="coerce") if has_ef_value else pd.Series([np.nan] * len(base))
    spend = pd.to_numeric(base[spend_col], errors="coerce") if has_spend else pd.Series([np.nan] * len(base))
    baseline_emissions = pd.to_numeric(base[baseline_emissions_col], errors="coerce")

    # Build DECARB EF via vectorized map
    red = ef_id.map(lambda x: ef_reduction_by_id.get(str(x), 0.0))
    red = pd.to_numeric(red, errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0)
    mult = (1.0 - red.to_numpy(dtype=float)).astype(float)  # multiplier on baseline co2e(t)

    # Keep ef_decarb for compatibility/inspection only (NOT used for primary decarb emissions).
    ef_decarb = ef_value.to_numpy(dtype=float) * mult

    # BAU scenario
    bau = base.copy()
    bau[scenario_col] = bau_name
    # Historically this column stored ef_value; now we store the multiplier applied to baseline emissions.
    bau[ef_scenario_col] = np.ones(len(bau), dtype=float)
    # Keep BAU equal to the dataset's current calculated emissions.
    bau[emissions_col] = baseline_emissions.astype(float)
    bau[emissions_spend_col] = (spend.to_numpy(dtype=float) * ef_value.to_numpy(dtype=float)).astype(float)

    # DECARB scenario
    dec = base.copy()
    dec[scenario_col] = decarb_name
    dec[ef_scenario_col] = mult.astype(float)
    # Primary decarb logic: scale the already-calculated baseline emissions (co2e(t)) by the multiplier.
    dec[emissions_col] = (baseline_emissions.to_numpy(dtype=float) * mult).astype(float)
    # Optional inspection metric (may be inconsistent across sources; do not use as primary output).
    dec[emissions_spend_col] = (spend.to_numpy(dtype=float) * np.asarray(ef_decarb, dtype=float)).astype(float)

    # Stack
    out = pd.concat([bau, dec], ignore_index=True)
    return out

