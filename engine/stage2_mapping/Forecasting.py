from __future__ import annotations

import argparse
import math
import os
import sys
import warnings
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import STAGE2_OUTPUT_DIR

# Reduce console spam from known, non-fatal sklearn warnings (safe even if sklearn not installed).
warnings.filterwarnings(
    "ignore",
    message="`sklearn.utils.parallel.delayed` should be used*",
    category=UserWarning,
)


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = STAGE2_OUTPUT_DIR

# Columns we expect to exist in window workbook sheets (best-effort).
DATE_CANDIDATES = ["Date", "date", "Reporting period (month, year)", "Reporting Period", "Reporting_Month", "Month"]
COMPANY_COL = "Company"
METRICS = ["co2e (t)", "Spend_Euro"]

# Sheets that are not timeseries data tabs (summaries/logs), plus Water (requested to exclude by default).
DEFAULT_SKIP_SHEETS = {
    "Water",
    "DC Log",
    "Anomalies",
    "Data Volume Summary",
    "Data Type Summary",
}

# Default forecasting target: we are in 2026 and want forecasts through end of 2030
DEFAULT_TARGET_YEAR = 2030


@dataclass(frozen=True)
class ForecastConfig:
    target_year: int = DEFAULT_TARGET_YEAR
    freq: str = "M"  # monthly
    lags: int = 12
    # Minimum number of data points required in the raw monthly series
    # (can be lowered for quick demos / shorter windows).
    min_points: int = 18
    # Number of holdout points (upper bound; actual may be smaller).
    validation_points: int = 6
    max_series: int = 0  # 0 = no cap
    # Model selection:
    # - "full": all sklearn models (including FFNN/MLP), optional XGBoost, plus ARIMA if available
    # - "nn": only feed-forward neural network (MLP)
    # - "linear": LinearRegression + Ridge
    model_set: str = "full"

    # Sheet / company outputs:
    include_sheet: bool = True
    include_company: bool = True
    include_total: bool = True  # total across all companies + all sheets
    # Optional: apply a smooth annual growth rate ONLY to total forecasts (post-processing),
    # while preserving the monthly seasonal pattern. Example: 0.05 = +5% per year.
    total_growth_annual: float = 0.0
    skip_sheets_csv: str = ",".join(sorted(DEFAULT_SKIP_SHEETS))
    include_trend: bool = True  # include "t" (time index) feature
    y_transform: str = "none"  # "none" | "log1p"
    cap_multiplier: float = 0.0  # 0 = no cap; else cap = cap_multiplier * max(observed)
    anchor_to_seasonal: bool = False
    anchor_tau_months: float = 12.0
    anchor_cap_multiplier: float = 0.0  # 0 disables; else cap each month to (seasonal_naive * multiplier)


def _adapt_cfg_for_series(cfg: ForecastConfig, n_points: int) -> ForecastConfig:
    """
    If a series is shorter than the default config expects (e.g. a 12-month window),
    adapt lags/validation/min_points downward just enough so we can still produce
    a reasonable forecast instead of skipping everything.
    """
    if n_points <= 0:
        return cfg

    # If config is already feasible, keep it as-is.
    if n_points >= max(cfg.min_points, cfg.lags + cfg.validation_points + 2):
        return cfg

    # Heuristics for short series:
    # - keep a small holdout
    # - keep lags <= ~25% of history
    # - require at least lags + validation + a couple points
    val = int(min(cfg.validation_points, max(1, n_points // 4)))
    lags = int(min(cfg.lags, max(1, n_points // 4)))
    min_points = int(min(cfg.min_points, n_points))
    min_points = int(max(min_points, lags + val + 2))

    return ForecastConfig(
        target_year=cfg.target_year,
        freq=cfg.freq,
        lags=lags,
        min_points=min_points,
        validation_points=val,
        max_series=cfg.max_series,
        model_set=cfg.model_set,
        include_sheet=cfg.include_sheet,
        include_company=cfg.include_company,
        include_total=getattr(cfg, "include_total", True),
        total_growth_annual=float(getattr(cfg, "total_growth_annual", 0.0) or 0.0),
        skip_sheets_csv=cfg.skip_sheets_csv,
        include_trend=cfg.include_trend,
        y_transform=cfg.y_transform,
        cap_multiplier=cfg.cap_multiplier,
        anchor_to_seasonal=cfg.anchor_to_seasonal,
        anchor_tau_months=cfg.anchor_tau_months,
        anchor_cap_multiplier=cfg.anchor_cap_multiplier,
    )


def _get_y_transform(cfg: ForecastConfig):
    name = str(getattr(cfg, "y_transform", "none")).strip().lower()
    if name == "log1p":
        # NOTE: inverse can overflow if model outputs extreme values.
        return (np.log1p, np.expm1, "log1p")
    return (lambda a: a, lambda a: a, "none")


def _safe_inverse_from_work(y_work_arr: np.ndarray, cfg: ForecastConfig) -> np.ndarray:
    """
    Convert model-space targets back to original units, with overflow protection.
    """
    y_work_arr = np.asarray(y_work_arr, dtype=float)
    name = str(getattr(cfg, "y_transform", "none")).strip().lower()
    if name == "log1p":
        # exp(709) is near float overflow; keep far below.
        y_work_arr = np.clip(y_work_arr, a_min=-50.0, a_max=50.0)
        return np.expm1(y_work_arr)
    return y_work_arr


def _smape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    yt = np.asarray(y_true, dtype=float)
    yp = np.asarray(y_pred, dtype=float)
    denom = np.maximum(np.abs(yt) + np.abs(yp), 1e-9)
    return float(np.mean(2.0 * np.abs(yp - yt) / denom)) * 100.0


def _mdape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    yt = np.asarray(y_true, dtype=float)
    yp = np.asarray(y_pred, dtype=float)
    denom = np.maximum(np.abs(yt), 1e-9)
    ape = np.abs(yp - yt) / denom * 100.0
    return float(np.median(ape))


def _wape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    yt = np.asarray(y_true, dtype=float)
    yp = np.asarray(y_pred, dtype=float)
    denom = float(np.sum(np.abs(yt)))
    if denom <= 1e-9:
        return float("nan")
    return float(np.sum(np.abs(yp - yt)) / denom) * 100.0


def _r2_score(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    yt = np.asarray(y_true, dtype=float)
    yp = np.asarray(y_pred, dtype=float)
    if len(yt) < 2:
        return float("nan")
    ss_res = float(np.sum((yt - yp) ** 2))
    ss_tot = float(np.sum((yt - float(np.mean(yt))) ** 2))
    if ss_tot <= 1e-12:
        return float("nan")
    return 1.0 - ss_res / ss_tot


def _mase_scale(y_train: np.ndarray, season: int = 12) -> float:
    yt = np.asarray(y_train, dtype=float)
    if len(yt) < 2:
        return float("nan")
    if len(yt) > season:
        diffs = np.abs(yt[season:] - yt[:-season])
    else:
        diffs = np.abs(yt[1:] - yt[:-1])
    s = float(np.mean(diffs)) if len(diffs) else float("nan")
    return s if s > 1e-12 else float("nan")


def _compute_metrics(y_true: np.ndarray, y_pred: np.ndarray, *, mase_scale: float | None = None) -> Dict[str, float]:
    yt = np.asarray(y_true, dtype=float)
    yp = np.asarray(y_pred, dtype=float)
    # Ensure finite
    m = np.isfinite(yt) & np.isfinite(yp)
    yt = yt[m]
    yp = yp[m]
    if len(yt) == 0:
        return {
            "mae": float("nan"),
            "rmse": float("nan"),
            "mape": float("nan"),
            "smape": float("nan"),
            "mdape": float("nan"),
            "wape": float("nan"),
            "mase": float("nan"),
            "bias": float("nan"),
            "r2": float("nan"),
        }

    err = yp - yt
    mae = float(np.mean(np.abs(err)))
    rmse = float(np.sqrt(np.mean(err**2)))
    mape = float(np.mean(np.abs(err) / np.maximum(np.abs(yt), 1e-9))) * 100.0
    smape = _smape(yt, yp)
    mdape = _mdape(yt, yp)
    wape = _wape(yt, yp)
    bias = float(np.mean(err))
    r2 = _r2_score(yt, yp)
    mase = float("nan")
    if mase_scale is not None and np.isfinite(mase_scale) and mase_scale > 1e-12:
        mase = float(mae / float(mase_scale))

    return {
        "mae": mae,
        "rmse": rmse,
        "mape": mape,
        "smape": smape,
        "mdape": mdape,
        "wape": wape,
        "mase": mase,
        "bias": bias,
        "r2": r2,
    }


def _forecast_naive_last(y: pd.Series, steps: int) -> pd.Series:
    yv = _safe_to_numeric(y).fillna(0.0).astype(float)
    if len(yv) == 0:
        return pd.Series([], dtype=float)
    last = float(yv.iloc[-1])
    idx = pd.date_range(yv.index[-1] + pd.offsets.MonthBegin(1), periods=steps, freq="MS")
    return pd.Series([max(0.0, last)] * steps, index=idx, name="yhat")


def _forecast_mean(y: pd.Series, steps: int) -> pd.Series:
    yv = _safe_to_numeric(y).fillna(0.0).astype(float)
    if len(yv) == 0:
        return pd.Series([], dtype=float)
    mu = float(np.nanmean(np.asarray(yv, dtype=float))) if len(yv) else 0.0
    idx = pd.date_range(yv.index[-1] + pd.offsets.MonthBegin(1), periods=steps, freq="MS")
    return pd.Series([max(0.0, mu)] * steps, index=idx, name="yhat")


def _forecast_drift(y: pd.Series, steps: int) -> pd.Series:
    yv = _safe_to_numeric(y).fillna(0.0).astype(float)
    if len(yv) == 0:
        return pd.Series([], dtype=float)
    if len(yv) == 1:
        return _forecast_naive_last(yv, steps)
    drift = (float(yv.iloc[-1]) - float(yv.iloc[0])) / max(1.0, float(len(yv) - 1))
    idx = pd.date_range(yv.index[-1] + pd.offsets.MonthBegin(1), periods=steps, freq="MS")
    vals = [max(0.0, float(yv.iloc[-1]) + drift * (i + 1)) for i in range(steps)]
    return pd.Series(vals, index=idx, name="yhat")


def _forecast_seasonal_naive(y: pd.Series, steps: int, season: int = 12) -> pd.Series:
    yv = _safe_to_numeric(y).fillna(0.0).astype(float)
    if len(yv) == 0:
        return pd.Series([], dtype=float)
    if len(yv) < season:
        # Not enough history for seasonality; fallback to last.
        return _forecast_naive_last(yv, steps)
    last_season = yv.iloc[-season:].to_numpy(dtype=float)
    idx = pd.date_range(yv.index[-1] + pd.offsets.MonthBegin(1), periods=steps, freq="MS")
    vals = [max(0.0, float(last_season[i % season])) for i in range(steps)]
    return pd.Series(vals, index=idx, name="yhat")


def _find_latest_window_workbook(base_dir: Path) -> Optional[Path]:
    out_dir = STAGE2_OUTPUT_DIR
    try:
        candidates = [
            p
            for p in out_dir.rglob("mapped_results_window_*.xlsx")
            if p.is_file() and (not p.name.startswith("~$"))
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return candidates[0]
    except Exception:
        return None


def _detect_date_series(df: pd.DataFrame) -> Optional[pd.Series]:
    for name in DATE_CANDIDATES:
        if name in df.columns:
            try:
                return pd.to_datetime(df[name], errors="coerce")
            except Exception:
                try:
                    return pd.to_datetime(df[name].astype(str), errors="coerce")
                except Exception:
                    return None
    return None


def _safe_to_numeric(series: pd.Series) -> pd.Series:
    try:
        if series is None:
            return pd.Series(dtype="float64")
        if pd.api.types.is_numeric_dtype(series):
            return pd.to_numeric(series, errors="coerce")
        txt = series.astype(str).str.replace("\u00A0", "", regex=False).str.replace(" ", "", regex=False)

        def _parse_one(v: str) -> Optional[float]:
            try:
                vv = str(v).strip()
                if vv == "" or vv.lower() == "nan":
                    return None
                if "," in vv and "." in vv:
                    last_c = vv.rfind(",")
                    last_d = vv.rfind(".")
                    if last_c > last_d:
                        vv = vv.replace(".", "").replace(",", ".")
                    else:
                        vv = vv.replace(",", "")
                else:
                    vv = vv.replace(",", ".")
                return float(vv)
            except Exception:
                return None

        parsed = txt.map(_parse_one)
        return pd.to_numeric(parsed, errors="coerce")
    except Exception:
        return pd.to_numeric(series, errors="coerce")


def _to_monthly_series(df: pd.DataFrame, date_ser: pd.Series, value_col: str) -> Optional[pd.Series]:
    if df is None or df.empty:
        return None
    if value_col not in df.columns:
        return None
    dt = pd.to_datetime(date_ser, errors="coerce")
    val = _safe_to_numeric(df[value_col]).fillna(0.0)
    tmp = pd.DataFrame({"dt": dt, "val": val})
    tmp = tmp.dropna(subset=["dt"])
    if tmp.empty:
        return None
    # Month start index
    tmp["m"] = tmp["dt"].dt.to_period("M").dt.to_timestamp(how="start")
    out = tmp.groupby("m", dropna=False)["val"].sum().sort_index()
    # Ensure a continuous monthly index (fill missing months with 0)
    idx = pd.date_range(out.index.min(), out.index.max(), freq="MS")
    out = out.reindex(idx).fillna(0.0)
    out.index.name = "Month"
    return out


def _month_features(dts: pd.DatetimeIndex, *, include_trend: bool = True) -> pd.DataFrame:
    # Cyclical month encoding (+ optional time index trend)
    m = dts.month.astype(float)
    sin_m = np.sin(2.0 * math.pi * (m / 12.0))
    cos_m = np.cos(2.0 * math.pi * (m / 12.0))
    data = {"sin_month": sin_m, "cos_month": cos_m}
    if include_trend:
        data["t"] = np.arange(len(dts), dtype=float)
    return pd.DataFrame(data, index=dts)


def _make_supervised(y: pd.Series, lags: int, *, include_trend: bool = True) -> Tuple[pd.DataFrame, pd.Series]:
    # y indexed by Month (DatetimeIndex)
    df = pd.DataFrame({"y": _safe_to_numeric(y).fillna(0.0)}, index=y.index)
    for k in range(1, lags + 1):
        df[f"lag_{k}"] = df["y"].shift(k)
    feat = _month_features(df.index, include_trend=include_trend)
    df = pd.concat([df, feat], axis=1)
    df = df.dropna()
    X = df.drop(columns=["y"])
    yy = df["y"]
    return X, yy


def _forecast_iterative(
    model,
    y_work: pd.Series,
    steps: int,
    lags: int,
    start_t: int,
    *,
    cap_work: Optional[float] = None,
) -> pd.Series:
    # Forecast next `steps` months iteratively from last observed y.
    # start_t is the t index (trend) for the first forecast point, relative to training feature generation.
    last_idx = y_work.index[-1]
    future_idx = pd.date_range(last_idx + pd.offsets.MonthBegin(1), periods=steps, freq="MS")

    # `y_work` is in the model's target space (possibly transformed).
    hist = _safe_to_numeric(y_work).fillna(0.0).astype(float).tolist()
    preds_work: List[float] = []
    for i, dt in enumerate(future_idx):
        feats: Dict[str, float] = {}
        for k in range(1, lags + 1):
            feats[f"lag_{k}"] = float(hist[-k]) if len(hist) >= k else 0.0
        m = float(dt.month)
        feats["sin_month"] = float(np.sin(2.0 * math.pi * (m / 12.0)))
        feats["cos_month"] = float(np.cos(2.0 * math.pi * (m / 12.0)))
        # NOTE: "t" feature (trend) may be dropped upstream; model will ignore extra column
        feats["t"] = float(start_t + i)
        X1 = pd.DataFrame([feats])
        expected = getattr(model, "feature_names_in_", None)
        if expected is not None:
            X1 = X1.reindex(columns=list(expected), fill_value=0.0)
        yhat_work = float(model.predict(X1)[0])
        if cap_work is not None and np.isfinite(cap_work):
            yhat_work = float(min(yhat_work, float(cap_work)))
        preds_work.append(yhat_work)
        hist.append(yhat_work)

    return pd.Series(preds_work, index=future_idx, name="yhat_work")


def _try_fit_sklearn_models(y: pd.Series, cfg: ForecastConfig) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      - forecasts: rows per (model, Month) with yhat
      - metrics: rows per model with RMSE/MAPE on validation
    """
    # Lazy imports so the module can still run even if sklearn isn't installed
    try:
        from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
        from sklearn.linear_model import LinearRegression, Ridge
        from sklearn.metrics import mean_squared_error
        from sklearn.neural_network import MLPRegressor
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
        from sklearn.decomposition import PCA
        from sklearn.exceptions import ConvergenceWarning
    except Exception as exc:
        raise RuntimeError("scikit-learn is required for ML models.") from exc

    # Reduce console spam from known, non-fatal sklearn warnings.
    warnings.filterwarnings("ignore", category=ConvergenceWarning)
    warnings.filterwarnings(
        "ignore",
        message="`sklearn.utils.parallel.delayed` should be used*",
    )

    y_fwd, _y_inv_raw, _tr_name = _get_y_transform(cfg)
    y0 = _safe_to_numeric(y).fillna(0.0).astype(float)
    # log1p requires non-negative values; emissions/spend should not be negative in general,
    # but some inputs can contain negatives (e.g., reversals). Clamp to 0 for transform safety.
    if str(getattr(cfg, "y_transform", "none")).strip().lower() == "log1p":
        y0_for_tr = y0.clip(lower=0.0)
    else:
        y0_for_tr = y0
    y_work = pd.Series(y_fwd(y0_for_tr.to_numpy(dtype=float)), index=y0.index, name="y_work")

    X, yy = _make_supervised(
        y_work,
        cfg.lags,
        include_trend=bool(getattr(cfg, "include_trend", True)),
    )
    # At this point `yy` is the supervised dataset (after lagging & dropna).
    # `cfg.min_points` is meant for the raw monthly series length, not `yy`.
    if len(yy) < max(cfg.validation_points + 1, 5):
        return pd.DataFrame(), pd.DataFrame()

    n_val = min(cfg.validation_points, max(1, len(yy) // 5))
    X_train, X_val = X.iloc[:-n_val], X.iloc[-n_val:]
    y_train, y_val = yy.iloc[:-n_val], yy.iloc[-n_val:]

    def _mape(a: np.ndarray, b: np.ndarray) -> float:
        denom = np.maximum(np.abs(a), 1e-9)
        return float(np.mean(np.abs(a - b) / denom)) * 100.0

    model_set = str(getattr(cfg, "model_set", "full")).strip().lower()
    if model_set == "linear":
        models = {
            "LinearRegression": LinearRegression(),
            "Ridge": Ridge(alpha=1.0),
        }
    elif model_set == "nn":
        models = {
            "MLP_FFNN": Pipeline(
                steps=[
                    ("scaler", StandardScaler(with_mean=True, with_std=True)),
                    (
                        "mlp",
                        MLPRegressor(
                            hidden_layer_sizes=(64, 32),
                            activation="relu",
                            solver="adam",
                            alpha=1e-2,
                            learning_rate_init=1e-3,
                            max_iter=1200,
                            early_stopping=False,
                            random_state=42,
                        ),
                    ),
                ]
            ),
        }
    else:
        # full
        models = {
            "LinearRegression": LinearRegression(),
            "Ridge": Ridge(alpha=1.0),
            "RandomForest": RandomForestRegressor(
                n_estimators=150, random_state=42, n_jobs=-1, min_samples_leaf=2
            ),
            "GradientBoosting": GradientBoostingRegressor(random_state=42),
            "MLP_FFNN": Pipeline(
                steps=[
                    ("scaler", StandardScaler(with_mean=True, with_std=True)),
                    (
                        "mlp",
                        MLPRegressor(
                            hidden_layer_sizes=(64, 32),
                            activation="relu",
                            solver="adam",
                            alpha=1e-2,
                            learning_rate_init=1e-3,
                            max_iter=1200,
                            early_stopping=False,
                            random_state=42,
                        ),
                    ),
                ]
            ),
            "PCA_Ridge": Pipeline(
                steps=[
                    ("scaler", StandardScaler(with_mean=True, with_std=True)),
                    ("pca", PCA(n_components=min(8, X_train.shape[1]))),
                    ("ridge", Ridge(alpha=1.0)),
                ]
            ),
        }

    # Optional XGBoost (only in full mode)
    if model_set == "full":
        try:
            from xgboost import XGBRegressor  # type: ignore

            models["XGBoost"] = XGBRegressor(
                n_estimators=600,
                learning_rate=0.05,
                max_depth=5,
                subsample=0.9,
                colsample_bytree=0.9,
                reg_lambda=1.0,
                random_state=42,
                n_jobs=max(1, os.cpu_count() or 1),
            )
        except Exception:
            pass

    metrics_rows: List[Dict[str, object]] = []
    forecasts_rows: List[Dict[str, object]] = []

    steps = _steps_to_target_year(y.index, cfg.target_year)
    if steps <= 0:
        return pd.DataFrame(), pd.DataFrame()

    # For iterative forecasts we need the "t" index that continues after the training rows.
    # In _make_supervised we used t=0..len(index)-1 BEFORE dropna; after dropna, last t is still aligned.
    # We'll define start_t as last observed index position + 1 (based on full monthly index).
    start_t = len(pd.date_range(y.index.min(), y.index.max(), freq="MS"))

    cap_mult = float(getattr(cfg, "cap_multiplier", 0.0) or 0.0)
    cap_value = None
    cap_work = None
    if cap_mult > 0:
        mx = float(np.nanmax(np.asarray(y0, dtype=float))) if len(y0) else 0.0
        cap_value = max(0.0, mx * cap_mult)
        # Transform cap to model space if needed
        try:
            cap_work = float(y_fwd(np.asarray([cap_value], dtype=float))[0])
        except Exception:
            cap_work = None

    for name, model in models.items():
        try:
            model.fit(X_train, y_train)
            yhat_val_work = np.asarray(model.predict(X_val), dtype=float)
            yhat_val = _safe_inverse_from_work(yhat_val_work, cfg)
            y_val_orig = _safe_inverse_from_work(np.asarray(y_val, dtype=float), cfg)
            y_val_orig = np.asarray(y_val_orig, dtype=float)
            yhat_val = np.asarray(yhat_val, dtype=float)

            # Validation metrics (many, for comparison)
            mase_s = _mase_scale(np.asarray(y0.iloc[:-n_val], dtype=float), season=12)
            met = _compute_metrics(y_val_orig, yhat_val, mase_scale=mase_s)

            fc_work = _forecast_iterative(
                model,
                y_work,
                steps=steps,
                lags=cfg.lags,
                start_t=start_t,
                cap_work=cap_work,
            )
            # Optional seasonal anchoring in MODEL SPACE to avoid overflow and long-horizon drift.
            if bool(getattr(cfg, "anchor_to_seasonal", False)):
                tau = float(getattr(cfg, "anchor_tau_months", 12.0) or 12.0)
                base_orig = _forecast_seasonal_naive(y0, steps=steps, season=12).to_numpy(dtype=float)
                base_work = y_fwd(np.asarray(base_orig, dtype=float))
                anchored_work: List[float] = []
                for i, v_work in enumerate(np.asarray(fc_work.values, dtype=float)):
                    w = float(np.exp(-float(i + 1) / max(1e-6, tau)))
                    anchored_work.append(float(w * float(v_work) + (1.0 - w) * float(base_work[i])))
                fc_work_to_inv = np.asarray(anchored_work, dtype=float)
                model_suffix = f" (anchored_to_seasonal_naive,tau={tau:g})"
            else:
                fc_work_to_inv = np.asarray(fc_work.values, dtype=float)
                model_suffix = ""

            fc_vals = _safe_inverse_from_work(fc_work_to_inv, cfg)
            fc_vals = np.maximum(0.0, np.asarray(fc_vals, dtype=float))
            # Optional per-month cap relative to seasonal naive (prevents rare extreme explosions while keeping seasonality)
            if bool(getattr(cfg, "anchor_to_seasonal", False)):
                m = float(getattr(cfg, "anchor_cap_multiplier", 0.0) or 0.0)
                if m > 0:
                    base_orig = _forecast_seasonal_naive(y0, steps=steps, season=12).to_numpy(dtype=float)
                    fc_vals = np.minimum(fc_vals, np.asarray(base_orig, dtype=float) * float(m))
            if cap_value is not None and np.isfinite(cap_value):
                fc_vals = np.minimum(fc_vals, float(cap_value))
            fc_vals_to_write = np.asarray(fc_vals, dtype=float)

            # Forecast summary stats (helps detect blow-ups / plateaus)
            fmax = float(np.nanmax(fc_vals_to_write)) if len(fc_vals_to_write) else float("nan")
            flast = float(fc_vals_to_write[-1]) if len(fc_vals_to_write) else float("nan")
            fmean = float(np.nanmean(fc_vals_to_write)) if len(fc_vals_to_write) else float("nan")

            clip_frac_global = float("nan")
            if cap_value is not None and np.isfinite(cap_value) and len(fc_vals_to_write):
                clip_frac_global = float(np.mean(fc_vals_to_write >= float(cap_value) - 1e-9))
            clip_frac_anchor = float("nan")
            if bool(getattr(cfg, "anchor_to_seasonal", False)):
                m = float(getattr(cfg, "anchor_cap_multiplier", 0.0) or 0.0)
                if m > 0 and len(fc_vals_to_write):
                    base_orig = _forecast_seasonal_naive(y0, steps=steps, season=12).to_numpy(dtype=float)
                    cap_arr = np.asarray(base_orig, dtype=float) * float(m)
                    clip_frac_anchor = float(np.mean(fc_vals_to_write >= cap_arr - 1e-9))

            # Full descriptive model name (no abbreviations)
            model_full = _format_model_name(name, model, cfg) + model_suffix
            for dt, v in zip(fc_work.index, fc_vals_to_write):
                forecasts_rows.append({"model": model_full, "Month": dt, "yhat": float(v)})
            metrics_rows.append(
                {
                    "model": model_full,
                    **met,
                    "n_train": int(len(y_train)),
                    "n_val": int(len(y_val)),
                    "forecast_mean": fmean,
                    "forecast_max": fmax,
                    "forecast_last": flast,
                    "clip_frac_global": clip_frac_global,
                    "clip_frac_anchor": clip_frac_anchor,
                }
            )
        except Exception as exc:
            metrics_rows.append(
                {
                    "model": _format_model_name(name, model, cfg),
                    "mae": np.nan,
                    "rmse": np.nan,
                    "mape": np.nan,
                    "smape": np.nan,
                    "mdape": np.nan,
                    "wape": np.nan,
                    "mase": np.nan,
                    "bias": np.nan,
                    "r2": np.nan,
                    "n_train": int(len(y_train)),
                    "n_val": int(len(y_val)),
                    "forecast_mean": np.nan,
                    "forecast_max": np.nan,
                    "forecast_last": np.nan,
                    "clip_frac_global": np.nan,
                    "clip_frac_anchor": np.nan,
                    "error": str(exc),
                }
            )

    forecasts = pd.DataFrame(forecasts_rows)
    metrics = pd.DataFrame(metrics_rows)
    return forecasts, metrics


def _try_fit_arima(y: pd.Series, cfg: ForecastConfig) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Seasonal ARIMA (SARIMAX) best-effort. If statsmodels is missing, returns empty.
    """
    try:
        from statsmodels.tsa.statespace.sarimax import SARIMAX  # type: ignore
    except Exception:
        return pd.DataFrame(), pd.DataFrame()

    if len(y) < max(cfg.min_points, 24):
        return pd.DataFrame(), pd.DataFrame()

    steps = _steps_to_target_year(y.index, cfg.target_year)
    if steps <= 0:
        return pd.DataFrame(), pd.DataFrame()

    # Simple, robust default orders. We keep it deterministic and avoid heavy auto-search.
    order = (1, 1, 1)
    seasonal_order = (1, 1, 1, 12)
    try:
        model = SARIMAX(
            _safe_to_numeric(y).astype(float),
            order=order,
            seasonal_order=seasonal_order,
            enforce_stationarity=False,
            enforce_invertibility=False,
        )
        res = model.fit(disp=False)
        pred = res.get_forecast(steps=steps).predicted_mean
        pred = pd.Series(np.maximum(0.0, np.asarray(pred, dtype=float)), index=pd.date_range(y.index[-1] + pd.offsets.MonthBegin(1), periods=steps, freq="MS"))
        forecasts = pd.DataFrame({"model": "SARIMAX(1,1,1)(1,1,1,12)", "Month": pred.index, "yhat": pred.values})
        metrics = pd.DataFrame([{"model": "SARIMAX(1,1,1)(1,1,1,12)", "rmse": np.nan, "mape": np.nan, "n_train": int(len(y)), "n_val": 0}])
        return forecasts, metrics
    except Exception:
        return pd.DataFrame(), pd.DataFrame()


def _try_logistic_direction(y: pd.Series, cfg: ForecastConfig) -> pd.DataFrame:
    """
    Logistic Regression: Predict probability that next month increases (delta > 0).
    Returns future probabilities through target year (does not output numeric yhat).
    """
    try:
        from sklearn.linear_model import LogisticRegression
    except Exception:
        return pd.DataFrame()

    yv = _safe_to_numeric(y).fillna(0.0).astype(float)
    if len(yv) < max(cfg.min_points, cfg.lags + 6):
        return pd.DataFrame()

    # Build classification dataset on deltas.
    dy = yv.diff().shift(-1)  # delta to next month aligned with current month row
    label = (dy > 0).astype(int)

    X, _yy = _make_supervised(yv, cfg.lags, include_trend=bool(getattr(cfg, "include_trend", True)))  # features end at last available minus lags
    # Align labels to X's index
    lbl = label.reindex(X.index).dropna().astype(int)
    X = X.loc[lbl.index]
    if len(lbl) < 30:
        return pd.DataFrame()

    clf = LogisticRegression(max_iter=500, solver="lbfgs")
    try:
        clf.fit(X, lbl)
    except Exception:
        return pd.DataFrame()

    steps = _steps_to_target_year(y.index, cfg.target_year)
    if steps <= 0:
        return pd.DataFrame()

    # For future, we reuse iterative lag features but keep lags based on last observed values.
    last_idx = y.index[-1]
    future_idx = pd.date_range(last_idx + pd.offsets.MonthBegin(1), periods=steps, freq="MS")
    hist = yv.tolist()

    rows: List[Dict[str, object]] = []
    start_t = len(pd.date_range(y.index.min(), y.index.max(), freq="MS"))
    for i, dt in enumerate(future_idx):
        feats: Dict[str, float] = {}
        for k in range(1, cfg.lags + 1):
            feats[f"lag_{k}"] = float(hist[-k]) if len(hist) >= k else 0.0
        m = float(dt.month)
        feats["sin_month"] = float(np.sin(2.0 * math.pi * (m / 12.0)))
        feats["cos_month"] = float(np.cos(2.0 * math.pi * (m / 12.0)))
        feats["t"] = float(start_t + i)
        X1 = pd.DataFrame([feats])
        try:
            p_up = float(clf.predict_proba(X1)[0][1])
        except Exception:
            p_up = float("nan")
        rows.append({"model": "LogisticRegression(direction)", "Month": dt, "p_increase": p_up})
        # We do NOT roll forward using predicted direction; keep hist unchanged to avoid compounding.
    return pd.DataFrame(rows)


def _steps_to_target_year(index: pd.DatetimeIndex, target_year: int) -> int:
    if index is None or len(index) == 0:
        return 0
    last = pd.to_datetime(index[-1])
    target_end = pd.Timestamp(year=int(target_year), month=12, day=1)
    # If already beyond target, nothing to do
    if last >= target_end:
        return 0
    last_p = last.to_period("M")
    targ_p = target_end.to_period("M")
    # Pandas 3.0 changed Period subtraction to return a DateOffset (e.g. "<60 * MonthEnds>")
    # instead of an integer count. Use ordinals for a stable month difference.
    try:
        return int(getattr(targ_p, "ordinal") - getattr(last_p, "ordinal"))
    except Exception:
        # Fallback: approximate via year/month arithmetic
        return int((targ_p.year - last_p.year) * 12 + (targ_p.month - last_p.month))


def _choose_best_model(metrics: pd.DataFrame) -> Optional[str]:
    if metrics is None or metrics.empty:
        return None
    m = metrics.copy()
    m["mape"] = pd.to_numeric(m.get("mape"), errors="coerce")
    m["rmse"] = pd.to_numeric(m.get("rmse"), errors="coerce")
    # Prefer lowest MAPE; fallback to RMSE
    m1 = m.dropna(subset=["mape"])
    if not m1.empty:
        return str(m1.sort_values(["mape", "rmse"], ascending=[True, True]).iloc[0]["model"])
    m2 = m.dropna(subset=["rmse"])
    if not m2.empty:
        return str(m2.sort_values(["rmse"], ascending=[True]).iloc[0]["model"])
    return None


def _format_model_name(key: str, model, cfg: ForecastConfig) -> str:
    """
    Human-readable full model name for the `model` column.
    """
    k = str(key)
    try:
        if k == "LinearRegression":
            return "OrdinaryLeastSquares_LinearRegression()"
        if k == "Ridge":
            alpha = getattr(model, "alpha", None)
            return f"RidgeRegression(alpha={alpha})"
        if k == "RandomForest":
            return (
                "RandomForestRegressor("
                f"n_estimators={getattr(model,'n_estimators',None)},"
                f"min_samples_leaf={getattr(model,'min_samples_leaf',None)},"
                f"random_state={getattr(model,'random_state',None)})"
            )
        if k == "GradientBoosting":
            return f"GradientBoostingRegressor(random_state={getattr(model,'random_state',None)})"
        if k == "MLP_FFNN":
            # Can be raw MLPRegressor or Pipeline(StandardScaler -> MLPRegressor)
            try:
                if hasattr(model, "named_steps") and "mlp" in getattr(model, "named_steps", {}):
                    mlp = model.named_steps["mlp"]
                    return (
                        "Pipeline(StandardScaler -> NeuralNetwork_MLPRegressor_FeedForward("
                        f"hidden_layer_sizes={getattr(mlp,'hidden_layer_sizes',None)},"
                        f"activation={getattr(mlp,'activation',None)},"
                        f"solver={getattr(mlp,'solver',None)},"
                        f"alpha={getattr(mlp,'alpha',None)},"
                        f"learning_rate_init={getattr(mlp,'learning_rate_init',None)},"
                        f"max_iter={getattr(mlp,'max_iter',None)},"
                        f"early_stopping={getattr(mlp,'early_stopping',None)},"
                        f"random_state={getattr(mlp,'random_state',None)}))"
                    )
            except Exception:
                pass
            return "NeuralNetwork_MLPRegressor_FeedForward(MLPRegressor)"
        if k == "PCA_Ridge":
            return "Pipeline(StandardScaler -> PCA -> RidgeRegression)"
        if k == "XGBoost":
            return (
                "XGBRegressor("
                f"n_estimators={getattr(model,'n_estimators',None)},"
                f"learning_rate={getattr(model,'learning_rate',None)},"
                f"max_depth={getattr(model,'max_depth',None)},"
                f"subsample={getattr(model,'subsample',None)},"
                f"colsample_bytree={getattr(model,'colsample_bytree',None)},"
                f"reg_lambda={getattr(model,'reg_lambda',None)},"
                f"random_state={getattr(model,'random_state',None)})"
            )
    except Exception:
        pass
    return k


def _build_sheet_series(xls: pd.ExcelFile, cfg: ForecastConfig) -> List[Tuple[str, str, pd.Series]]:
    # returns list of (level_id, metric, series)
    skip = {s.strip() for s in str(getattr(cfg, "skip_sheets_csv", "")).split(",") if s.strip()}
    out: List[Tuple[str, str, pd.Series]] = []
    for sheet_name in xls.sheet_names:
        if sheet_name in skip:
            continue
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name)
        except Exception:
            continue
        dt = _detect_date_series(df)
        if dt is None:
            continue
        for metric in METRICS:
            ser = _to_monthly_series(df, dt, metric)
            if ser is None or ser.empty:
                continue
            level_id = f"sheet={sheet_name}"
            out.append((level_id, metric, ser))
            if cfg.max_series and cfg.max_series > 0 and len(out) >= cfg.max_series:
                return out
    return out


def _build_company_series(xls: pd.ExcelFile, cfg: ForecastConfig) -> List[Tuple[str, str, pd.Series]]:
    # Aggregate across all sheets by company-month; returns list of (company=..., metric, series)
    skip = {s.strip() for s in str(getattr(cfg, "skip_sheets_csv", "")).split(",") if s.strip()}
    parts: Dict[Tuple[str, str], List[pd.Series]] = {}
    for sheet_name in xls.sheet_names:
        if sheet_name in skip:
            continue
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        if COMPANY_COL not in df.columns:
            continue
        dt = _detect_date_series(df)
        if dt is None:
            continue
        dti = pd.to_datetime(dt, errors="coerce")
        if dti.isna().all():
            continue
        tmp = df.copy()
        tmp["_dt"] = dti
        tmp = tmp.dropna(subset=["_dt"])
        if tmp.empty:
            continue
        tmp["_m"] = tmp["_dt"].dt.to_period("M").dt.to_timestamp(how="start")
        for metric in METRICS:
            if metric not in tmp.columns:
                continue
            tmp2 = tmp[[COMPANY_COL, "_m", metric]].copy()
            tmp2[metric] = _safe_to_numeric(tmp2[metric]).fillna(0.0)
            g = tmp2.groupby([COMPANY_COL, "_m"], dropna=False)[metric].sum().reset_index()
            for comp, sub in g.groupby(COMPANY_COL, dropna=False):
                comp_name = str(comp) if comp is not None else "NaN"
                ser = sub.set_index("_m")[metric].sort_index()
                idx = pd.date_range(ser.index.min(), ser.index.max(), freq="MS")
                ser = ser.reindex(idx).fillna(0.0)
                ser.index.name = "Month"
                key = (f"company={comp_name}", metric)
                parts.setdefault(key, []).append(ser)

    out: List[Tuple[str, str, pd.Series]] = []
    for (level_id, metric), series_list in parts.items():
        # Sum across sheets (align on monthly index)
        if not series_list:
            continue
        base = series_list[0].copy()
        for s in series_list[1:]:
            base = base.add(s, fill_value=0.0)
        base = base.sort_index()
        out.append((level_id, metric, base))
    return out


def _build_total_series(xls: pd.ExcelFile, cfg: ForecastConfig) -> List[Tuple[str, str, pd.Series]]:
    """
    Total across all companies (monthly), for each metric.

    IMPORTANT:
    The window workbook often contains both:
    - detail tabs (Scope 1/2, S3 categories) which are additive, AND
    - already-aggregated "Window" summary tabs (Company Totals Window, Stacked Window, etc.).

    Summing *all* tabs will double-count. Therefore we prefer the canonical stacked-months
    tabs when available (these match the Power BI monthly distribution):
    - emissions: "Company Stacked Months Window" -> "Row Total (t)"
    - spend: "Co Stacked Months Win Spend" -> "Row Total (t)" (naming kept from generator)

    Fallback: if stacked-months tabs are missing, sum only additive GHGP category tabs
    (sheet names starting with "Scope " or "S3 Cat ").
    Returns list of (level_id, metric, series).
    """
    skip = {s.strip() for s in str(getattr(cfg, "skip_sheets_csv", "")).split(",") if s.strip()}
    level_id = "total=ALL_COMPANIES"

    def _series_from_stacked_months(sheet_name: str, value_col: str) -> Optional[pd.Series]:
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name)
        except Exception:
            return None
        if df is None or df.empty:
            return None
        if "Month" not in df.columns or value_col not in df.columns:
            return None
        dt = pd.to_datetime(df["Month"], errors="coerce")
        tmp = df.copy()
        tmp["_dt"] = dt
        tmp = tmp.dropna(subset=["_dt"])
        if tmp.empty:
            return None
        tmp["_m"] = tmp["_dt"].dt.to_period("M").dt.to_timestamp(how="start")
        tmp[value_col] = _safe_to_numeric(tmp[value_col]).fillna(0.0)
        ser = tmp.groupby("_m", dropna=False)[value_col].sum().sort_index()
        if ser.empty:
            return None
        idx = pd.date_range(ser.index.min(), ser.index.max(), freq="MS")
        ser = ser.reindex(idx).fillna(0.0)
        ser.index.name = "Month"
        return ser

    built: Dict[str, pd.Series] = {}

    # Preferred canonical totals (matches Power BI)
    if "co2e (t)" in METRICS:
        ser0 = _series_from_stacked_months("Company Stacked Months Window", "Row Total (t)")
        if ser0 is not None and (not ser0.empty):
            built["co2e (t)"] = ser0

    if "Spend_Euro" in METRICS:
        ser1 = _series_from_stacked_months("Co Stacked Months Win Spend", "Row Total (t)")
        if ser1 is not None and (not ser1.empty):
            built["Spend_Euro"] = ser1

    # Fallback for any missing metrics: sum only additive GHGP category tabs
    missing = [m for m in METRICS if m not in built]
    if missing:
        parts: Dict[str, List[pd.Series]] = {m: [] for m in missing}
        include_prefixes = ("Scope ", "S3 Cat ")

        for sheet_name in xls.sheet_names:
            if sheet_name in skip:
                continue
            if not str(sheet_name).startswith(include_prefixes):
                continue
            try:
                df = pd.read_excel(xls, sheet_name=sheet_name)
            except Exception:
                continue
            if df is None or df.empty:
                continue
            dt = _detect_date_series(df)
            if dt is None:
                continue
            dti = pd.to_datetime(dt, errors="coerce")
            if dti.isna().all():
                continue
            tmp = df.copy()
            tmp["_dt"] = dti
            tmp = tmp.dropna(subset=["_dt"])
            if tmp.empty:
                continue
            tmp["_m"] = tmp["_dt"].dt.to_period("M").dt.to_timestamp(how="start")

            for metric in missing:
                if metric not in tmp.columns:
                    continue
                tmp2 = tmp[["_m", metric]].copy()
                tmp2[metric] = _safe_to_numeric(tmp2[metric]).fillna(0.0)
                ser = tmp2.groupby("_m", dropna=False)[metric].sum().sort_index()
                if ser.empty:
                    continue
                idx = pd.date_range(ser.index.min(), ser.index.max(), freq="MS")
                ser = ser.reindex(idx).fillna(0.0)
                ser.index.name = "Month"
                parts[metric].append(ser)

        for metric, series_list in parts.items():
            if not series_list:
                continue
            base = series_list[0].copy()
            for s in series_list[1:]:
                base = base.add(s, fill_value=0.0)
            built[metric] = base.sort_index()

    out: List[Tuple[str, str, pd.Series]] = []
    for metric in METRICS:
        ser = built.get(metric)
        if ser is None or ser.empty:
            continue
        out.append((level_id, metric, ser))
    return out


def run_forecasting(window_path: Optional[str] = None, cfg: Optional[ForecastConfig] = None) -> Optional[Path]:
    cfg = cfg or ForecastConfig()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    wp: Optional[Path]
    if window_path:
        wp = Path(window_path)
        if wp.suffix.lower() != ".xlsx":
            wp = wp.with_suffix(".xlsx")
        if not wp.exists():
            wp = None
    else:
        wp = None

    if wp is None:
        wp = _find_latest_window_workbook(BASE_DIR)
    if wp is None or (not wp.exists()):
        print("[WARN] Forecasting: No window workbook found under output/. Skipping.")
        return None

    try:
        xls = pd.ExcelFile(wp)
    except Exception as exc:
        print(f"[WARN] Forecasting: Could not open workbook: {wp.name} -> {exc}")
        return None

    sheet_series = _build_sheet_series(xls, cfg) if cfg.include_sheet else []
    company_series = _build_company_series(xls, cfg) if cfg.include_company else []
    total_series = _build_total_series(xls, cfg) if bool(getattr(cfg, "include_total", True)) else []

    if cfg.max_series and cfg.max_series > 0:
        sheet_series = sheet_series[: cfg.max_series]
        company_series = company_series[: cfg.max_series]
        total_series = total_series[: cfg.max_series]

    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_DIR / f"forecast_results_{ts}.xlsx"

    all_forecasts: List[pd.DataFrame] = []
    all_metrics: List[pd.DataFrame] = []
    all_directions: List[pd.DataFrame] = []

    def _run_one(level_id: str, metric: str, ser: pd.Series) -> None:
        ser = _safe_to_numeric(ser).fillna(0.0)
        ser.name = "y"
        ser = ser.sort_index()
        ser = ser.loc[~ser.index.duplicated(keep="last")]

        cfg1 = _adapt_cfg_for_series(cfg, int(len(ser)))
        # For total (ALL companies) we prefer a conservative, seasonal forecast.
        # With short histories (often 12 months), the explicit trend feature "t"
        # can dominate and produce unrealistic drift (e.g., January forecast as high as December).
        if str(level_id).startswith("total=") and bool(getattr(cfg1, "include_trend", True)):
            cfg1 = replace(cfg1, include_trend=False)
        # Also enable seasonal anchoring + per-month cap for TOTAL to prevent extreme explosions.
        # This keeps monthly shape consistent with the last observed year (matches Power BI view).
        if str(level_id).startswith("total=") and not bool(getattr(cfg1, "anchor_to_seasonal", False)):
            cfg1 = replace(cfg1, anchor_to_seasonal=True, anchor_tau_months=8.0)
        if str(level_id).startswith("total=") and float(getattr(cfg1, "anchor_cap_multiplier", 0.0) or 0.0) <= 0.0:
            # Default to NO growth vs last-year seasonal naive (1.0x). If you want controlled growth,
            # pass --anchor-cap-multiplier (e.g., 1.1 for +10%).
            cfg1 = replace(cfg1, anchor_cap_multiplier=1.0)

        if len(ser) < max(cfg1.min_points, cfg1.lags + cfg1.validation_points + 2):
            return
        if _steps_to_target_year(ser.index, cfg1.target_year) <= 0:
            return

        # Baseline statistical methods (always include)
        steps = _steps_to_target_year(ser.index, cfg1.target_year)
        if steps > 0:
            baselines = [
                ("Baseline_SeasonalNaive(season=12)", _forecast_seasonal_naive(ser, steps=steps, season=12)),
                ("Baseline_NaiveLastValue", _forecast_naive_last(ser, steps=steps)),
                ("Baseline_Mean", _forecast_mean(ser, steps=steps)),
                ("Baseline_Drift", _forecast_drift(ser, steps=steps)),
            ]
            for model_name, fc0 in baselines:
                if fc0 is None or fc0.empty:
                    continue
                tmp = pd.DataFrame({"model": model_name, "Month": fc0.index, "yhat": fc0.values})
                tmp.insert(0, "level_id", level_id)
                tmp.insert(1, "metric", metric)
                all_forecasts.append(tmp)

            # Backtest baselines on last n_val months (direct forecast from train window)
            n_val = min(cfg1.validation_points, max(1, len(ser) // 5))
            if n_val >= 1 and len(ser) > n_val:
                y_train0 = ser.iloc[:-n_val]
                y_val0 = ser.iloc[-n_val:]
                mase_s = _mase_scale(np.asarray(y_train0, dtype=float), season=12)
                for model_name, _fc0 in baselines:
                    # rebuild forecast for validation horizon from train only
                    if "SeasonalNaive" in model_name:
                        pv = _forecast_seasonal_naive(y_train0, steps=n_val, season=12)
                    elif "NaiveLastValue" in model_name:
                        pv = _forecast_naive_last(y_train0, steps=n_val)
                    elif "Mean" in model_name:
                        pv = _forecast_mean(y_train0, steps=n_val)
                    elif "Drift" in model_name:
                        pv = _forecast_drift(y_train0, steps=n_val)
                    else:
                        continue
                    pv = pv.reindex(y_val0.index)
                    met = _compute_metrics(
                        np.asarray(y_val0, dtype=float),
                        np.asarray(pv.values, dtype=float),
                        mase_scale=mase_s,
                    )
                    all_metrics.append(
                        pd.DataFrame(
                            [
                                {
                                    "level_id": level_id,
                                    "metric": metric,
                                    "model": model_name,
                                    **met,
                                    "n_train": int(len(y_train0)),
                                    "n_val": int(len(y_val0)),
                                    "forecast_mean": float(np.nanmean(np.asarray(_fc0.values, dtype=float))),
                                    "forecast_max": float(np.nanmax(np.asarray(_fc0.values, dtype=float))),
                                    "forecast_last": float(_fc0.values[-1]) if len(_fc0) else np.nan,
                                    "clip_frac_global": np.nan,
                                    "clip_frac_anchor": np.nan,
                                }
                            ]
                        )
                    )

        fc_ml, met_ml = pd.DataFrame(), pd.DataFrame()
        try:
            fc_ml, met_ml = _try_fit_sklearn_models(ser, cfg1)
        except Exception as exc:
            # Keep going with ARIMA if possible
            met_ml = pd.DataFrame([{"model": "sklearn_models", "rmse": np.nan, "mape": np.nan, "error": str(exc)}])

        fc_ar, met_ar = _try_fit_arima(ser, cfg1)

        # Combine forecasts/metrics
        fc = pd.concat([fc_ml, fc_ar], ignore_index=True) if (not fc_ml.empty or not fc_ar.empty) else pd.DataFrame()
        met = pd.concat([met_ml, met_ar], ignore_index=True) if (not met_ml.empty or not met_ar.empty) else pd.DataFrame()

        if not fc.empty:
            fc.insert(0, "level_id", level_id)
            fc.insert(1, "metric", metric)
            all_forecasts.append(fc)
        if not met.empty:
            met.insert(0, "level_id", level_id)
            met.insert(1, "metric", metric)
            # Pick a best model (for convenience)
            met["is_best"] = False
            best = _choose_best_model(met)
            if best is not None:
                met.loc[met["model"].astype(str) == str(best), "is_best"] = True
            all_metrics.append(met)

        # Logistic regression direction probabilities (optional)
        try:
            dirs = _try_logistic_direction(ser, cfg1)
            if dirs is not None and (not dirs.empty):
                dirs.insert(0, "level_id", level_id)
                dirs.insert(1, "metric", metric)
                all_directions.append(dirs)
        except Exception:
            pass

    for level_id, metric, ser in sheet_series:
        _run_one(level_id, metric, ser)
    for level_id, metric, ser in company_series:
        _run_one(level_id, metric, ser)
    for level_id, metric, ser in total_series:
        _run_one(level_id, metric, ser)

    # Write output
    meta = pd.DataFrame(
        [
            {"key": "input_window_workbook", "value": str(wp)},
            {"key": "target_year", "value": str(cfg.target_year)},
            {"key": "freq", "value": str(cfg.freq)},
            {"key": "lags", "value": str(cfg.lags)},
            {"key": "validation_points", "value": str(cfg.validation_points)},
            {"key": "model_set", "value": str(getattr(cfg, "model_set", "full"))},
            {"key": "include_sheet", "value": str(bool(getattr(cfg, "include_sheet", True)))},
            {"key": "include_company", "value": str(bool(getattr(cfg, "include_company", True)))},
            {"key": "include_total", "value": str(bool(getattr(cfg, "include_total", True)))},
            {"key": "total_growth_annual", "value": str(float(getattr(cfg, "total_growth_annual", 0.0) or 0.0))},
            {"key": "include_trend", "value": str(bool(getattr(cfg, "include_trend", True)))},
            {"key": "y_transform", "value": str(getattr(cfg, "y_transform", "none"))},
            {"key": "cap_multiplier", "value": str(getattr(cfg, "cap_multiplier", 0.0))},
            {"key": "anchor_to_seasonal", "value": str(bool(getattr(cfg, "anchor_to_seasonal", False)))},
            {"key": "anchor_tau_months", "value": str(getattr(cfg, "anchor_tau_months", 12.0))},
            {"key": "anchor_cap_multiplier", "value": str(getattr(cfg, "anchor_cap_multiplier", 0.0))},
            {"key": "skip_sheets", "value": str(getattr(cfg, "skip_sheets_csv", ""))},
        ]
    )

    fc_all = pd.concat(all_forecasts, ignore_index=True) if all_forecasts else pd.DataFrame(
        columns=["level_id", "metric", "model", "Month", "yhat"]
    )
    met_all = pd.concat(all_metrics, ignore_index=True) if all_metrics else pd.DataFrame(
        columns=[
            "level_id",
            "metric",
            "model",
            "mae",
            "rmse",
            "mape",
            "smape",
            "mdape",
            "wape",
            "mase",
            "bias",
            "r2",
            "n_train",
            "n_val",
            "forecast_mean",
            "forecast_max",
            "forecast_last",
            "clip_frac_global",
            "clip_frac_anchor",
            "is_best",
            "error",
        ]
    )
    dir_all = pd.concat(all_directions, ignore_index=True) if all_directions else pd.DataFrame(
        columns=["level_id", "metric", "model", "Month", "p_increase"]
    )

    # Normalize datatypes for Excel
    if "Month" in fc_all.columns:
        fc_all["Month"] = pd.to_datetime(fc_all["Month"], errors="coerce")
    if "Month" in dir_all.columns:
        dir_all["Month"] = pd.to_datetime(dir_all["Month"], errors="coerce")

    # Optional post-processing: apply smooth annual growth ONLY to TOTAL forecasts.
    # This avoids the "same year repeated" look when you want controlled long-run change,
    # while keeping the within-year seasonal profile intact.
    g_annual = float(getattr(cfg, "total_growth_annual", 0.0) or 0.0)
    if g_annual != 0.0 and (not fc_all.empty) and ("yhat" in fc_all.columns) and ("Month" in fc_all.columns):
        if g_annual <= -0.99:
            g_annual = -0.99
        mask_total = fc_all["level_id"].astype(str).str.startswith("total=") & fc_all["Month"].notna()
        if bool(mask_total.any()):
            tmp = fc_all.loc[mask_total, ["level_id", "metric", "model", "Month", "yhat"]].copy()

            def _apply_growth(gr: pd.DataFrame) -> pd.DataFrame:
                gr = gr.sort_values("Month")
                n = int(len(gr))
                if n <= 0:
                    return gr
                factors = (1.0 + float(g_annual)) ** (np.arange(n, dtype=float) / 12.0)
                gr["yhat"] = np.asarray(gr["yhat"], dtype=float) * factors
                return gr

            tmp2 = tmp.groupby(["level_id", "metric", "model"], dropna=False, group_keys=False).apply(_apply_growth)
            fc_all.loc[mask_total, "yhat"] = tmp2["yhat"].to_numpy(dtype=float)

    # Split for readability
    fc_sheet = fc_all[fc_all["level_id"].astype(str).str.startswith("sheet=")].copy()
    fc_comp = fc_all[fc_all["level_id"].astype(str).str.startswith("company=")].copy()
    fc_total = fc_all[fc_all["level_id"].astype(str).str.startswith("total=")].copy()

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        meta.to_excel(writer, sheet_name="Meta", index=False)
        fc_sheet.to_excel(writer, sheet_name="Forecast_Sheet", index=False)
        fc_comp.to_excel(writer, sheet_name="Forecast_Company", index=False)
        fc_total.to_excel(writer, sheet_name="Forecast_Total", index=False)
        met_all.to_excel(writer, sheet_name="Backtest", index=False)
        if dir_all is not None and (not dir_all.empty):
            dir_all.to_excel(writer, sheet_name="Direction_LogReg", index=False)

    print(f"[info] Forecasting: Wrote forecasts -> {out_path.name}")
    return out_path


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Forecasting for latest window workbook (sheet-based + company-based).")
    ap.add_argument("--input", help="Explicit window workbook path (xlsx)")
    ap.add_argument("--target-year", type=int, default=DEFAULT_TARGET_YEAR, help="Forecast horizon end year (default: 2030)")
    ap.add_argument("--lags", type=int, default=12, help="Number of lag months for ML models (default: 12)")
    ap.add_argument(
        "--min-points",
        type=int,
        default=ForecastConfig().min_points,
        help="Minimum number of monthly points required to forecast (default: 18)",
    )
    ap.add_argument(
        "--validation-points",
        type=int,
        default=ForecastConfig().validation_points,
        help="Max validation points for backtesting (default: 6)",
    )
    ap.add_argument("--max-series", type=int, default=0, help="Limit number of series for quick tests (0=all)")
    ap.add_argument("--no-sheet", action="store_true", help="Skip sheet-level forecasting")
    ap.add_argument("--no-company", action="store_true", help="Skip company-level forecasting (often much faster)")
    ap.add_argument("--no-total", action="store_true", help="Skip total (all companies + all sheets) forecasting")
    ap.add_argument(
        "--model-set",
        default=ForecastConfig().model_set,
        choices=["full", "nn", "linear"],
        help="Model set to use: full (default), nn (MLP only), linear (Linear+Ridge)",
    )
    ap.add_argument(
        "--skip-sheets",
        default=ForecastConfig().skip_sheets_csv,
        help="Comma-separated sheet names to skip (default excludes Water and summary/log sheets)",
    )
    ap.add_argument(
        "--no-trend",
        action="store_true",
        help="Disable the time-index trend feature (t). Often improves realism with only 12 months of history.",
    )
    ap.add_argument(
        "--y-transform",
        default=ForecastConfig().y_transform,
        choices=["none", "log1p"],
        help="Transform target before modeling (default: none). log1p often reduces explosive growth.",
    )
    ap.add_argument(
        "--cap-multiplier",
        type=float,
        default=ForecastConfig().cap_multiplier,
        help="Optional cap: predictions are clipped to cap_multiplier * max(observed). 0 disables.",
    )
    ap.add_argument(
        "--anchor-to-seasonal",
        action="store_true",
        help="Blend forecasts towards seasonal naive as horizon increases (reduces drift with 12 months history).",
    )
    ap.add_argument(
        "--anchor-tau-months",
        type=float,
        default=ForecastConfig().anchor_tau_months,
        help="Anchoring strength (months). Smaller = faster reversion to seasonal naive.",
    )
    ap.add_argument(
        "--anchor-cap-multiplier",
        type=float,
        default=ForecastConfig().anchor_cap_multiplier,
        help="When anchoring, optionally cap each month to seasonal_naive * multiplier (0 disables).",
    )
    ap.add_argument(
        "--total-growth-annual",
        type=float,
        default=ForecastConfig().total_growth_annual,
        help="Apply a smooth annual growth rate ONLY to total=ALL_COMPANIES forecasts (e.g., 0.05 = +5 percent per year). Default: 0.",
    )
    return ap.parse_args()


def main(window_path: Optional[str] = None) -> None:
    if window_path is None:
        # Allow CLI usage as well
        args = _parse_args()
        cfg = ForecastConfig(
            target_year=int(args.target_year),
            lags=int(args.lags),
            min_points=int(args.min_points),
            validation_points=int(args.validation_points),
            max_series=int(args.max_series),
            model_set=str(getattr(args, "model_set", "full")),
            include_sheet=not bool(getattr(args, "no_sheet", False)),
            include_company=not bool(getattr(args, "no_company", False)),
            include_total=not bool(getattr(args, "no_total", False)),
            total_growth_annual=float(getattr(args, "total_growth_annual", 0.0) or 0.0),
            skip_sheets_csv=str(getattr(args, "skip_sheets", ForecastConfig().skip_sheets_csv)),
            include_trend=not bool(getattr(args, "no_trend", False)),
            y_transform=str(getattr(args, "y_transform", "none")),
            cap_multiplier=float(getattr(args, "cap_multiplier", 0.0) or 0.0),
            anchor_to_seasonal=bool(getattr(args, "anchor_to_seasonal", False)),
            anchor_tau_months=float(getattr(args, "anchor_tau_months", 12.0) or 12.0),
            anchor_cap_multiplier=float(getattr(args, "anchor_cap_multiplier", 0.0) or 0.0),
        )
        run_forecasting(args.input, cfg)
    else:
        run_forecasting(window_path, ForecastConfig())


if __name__ == "__main__":
    main(None)

