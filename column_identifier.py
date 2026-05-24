"""
CRPUtils.column_identifier
==========================

Empirical column identification by statistical comparison against known
reference columns. Used to discover what a mystery column actually
contains when documentation is missing, wrong, or partial.

Typical use cases:
- A feed file has columns named Field101, Field102, ... that we want to identify
- A loader's position-to-name mapping turns out to be wrong (PLM_ClarityData)
- A new feed arrives and we need to map it to known business concepts

The utility scores each candidate field on three independent dimensions:

1. EXACT MATCH (with float tolerance) — strongest single signal. When two
   columns represent the same underlying field, their values will be
   identical across the population modulo floating-point noise.

2. CORRELATION (Pearson + Spearman) — second-strongest signal. Catches
   cases where the unknown column is a scaled or shifted version of a
   known field. Spearman is added because it's robust to non-linear
   monotonic transformations.

3. LINEAR FIT (slope + intercept + residual std) — quantifies "scaled
   version of" relationships. If slope ≈ 1 and intercept ≈ 0 with low
   residual std, the unknown column is essentially identical to the
   candidate. If slope is some other constant, the unknown column is a
   simple scaling.

A verdict heuristic combines these signals into one of: 'identity',
'near-identity', 'scaled', 'correlated', 'unrelated'.

Why exact match matters more than correlation alone: many supply-chain
fields are highly correlated by construction (LSP and OUL co-move with
forecast; safety stock co-moves with lead time; etc.). A high Pearson
correlation does not establish identity, only co-movement. Exact-match
fraction across a population is the only signal that distinguishes
identical fields from merely correlated ones.

Author: Pat Yearick + Claude (PLM Pilot, 2026-05-11)
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)


# ---- Fingerprinting ---------------------------------------------------------

def fingerprint(series: pd.Series) -> dict:
    """Compute a statistical fingerprint of a numeric series.

    Returns a dict with summary statistics that can be eyeballed to spot
    obvious type mismatches before running full identification.
    """
    s = pd.to_numeric(series, errors='coerce').dropna()
    if len(s) == 0:
        return {
            'n': 0, 'mean': np.nan, 'std': np.nan, 'min': np.nan,
            'max': np.nan, 'pct_zero': np.nan, 'pct_int': np.nan,
            'pct_negative': np.nan,
        }
    return {
        'n': int(len(s)),
        'mean': float(s.mean()),
        'std': float(s.std()),
        'min': float(s.min()),
        'max': float(s.max()),
        'pct_zero': float((s == 0).mean()),
        'pct_int': float((s == s.round()).mean()),
        'pct_negative': float((s < 0).mean()),
    }


def fingerprint_table(df: pd.DataFrame, numeric_only: bool = True) -> pd.DataFrame:
    """Fingerprint every column of a DataFrame and return as a DataFrame."""
    rows = []
    for col in df.columns:
        s = df[col]
        if numeric_only and not pd.api.types.is_numeric_dtype(s):
            continue
        fp = fingerprint(s)
        fp['column'] = col
        rows.append(fp)
    cols = ['column', 'n', 'mean', 'std', 'min', 'max',
            'pct_zero', 'pct_int', 'pct_negative']
    return pd.DataFrame(rows)[cols]


# ---- Identification ---------------------------------------------------------

def identify_column(
    unknown: pd.Series,
    candidates: Dict[str, pd.Series],
    min_overlap: int = 30,
    abs_tolerance: float = 1e-3,
    rel_tolerance: float = 1e-3,
) -> pd.DataFrame:
    """Score each candidate as a possible identity for the unknown column.

    Parameters
    ----------
    unknown : pd.Series
        The unidentified column, indexed by a join key (e.g. ItemNumber,
        or (ItemNumber, LocationName, CaptureDate) for snapshot data).
    candidates : dict[str, pd.Series]
        Named reference columns, each indexed by the same key type as
        `unknown`. Include both raw fields AND derived quantities you
        suspect the unknown might be (e.g. Forecast * LeadTime / 365).
    min_overlap : int
        Skip candidates with fewer than this many overlapping non-null rows.
    abs_tolerance, rel_tolerance : float
        Passed to np.isclose for "exact match" detection.

    Returns
    -------
    pd.DataFrame
        Ranked candidates, sorted by exact_match_pct desc then corr_pearson desc.
    """
    u_numeric = pd.to_numeric(unknown, errors='coerce')

    results = []
    for name, candidate in candidates.items():
        c_numeric = pd.to_numeric(candidate, errors='coerce')
        paired = pd.concat([u_numeric, c_numeric], axis=1, join='inner').dropna()
        paired.columns = ['u', 'c']

        if len(paired) < min_overlap:
            continue

        u, c = paired['u'], paired['c']

        # Exact match (float-tolerant)
        if u.std() == 0 and c.std() == 0:
            # Both constant; check if their single value matches
            exact = float(
                np.isclose(u.iloc[0], c.iloc[0],
                           atol=abs_tolerance, rtol=rel_tolerance)
            )
        else:
            exact = float(
                np.isclose(u.values, c.values,
                           atol=abs_tolerance, rtol=rel_tolerance).mean()
            )

        # Correlations (require variance in both)
        if u.std() > 0 and c.std() > 0:
            corr_p = float(u.corr(c))
            try:
                corr_s = float(stats.spearmanr(u, c).correlation)
            except Exception:
                corr_s = np.nan
        else:
            corr_p = np.nan
            corr_s = np.nan

        # Linear fit: u = slope * c + intercept
        if u.std() > 0 and c.std() > 0 and len(paired) >= 3:
            slope, intercept, _, _, _ = stats.linregress(c, u)
            residuals = u - (slope * c + intercept)
            resid_std = float(residuals.std())
            # Normalize residual std by mean of u for cross-scale comparison
            resid_norm = float(resid_std / abs(u.mean())) if u.mean() != 0 else np.nan
        else:
            slope = np.nan
            intercept = np.nan
            resid_std = np.nan
            resid_norm = np.nan

        # Verdict heuristic
        verdict = _classify_match(exact, corr_p, slope, resid_norm)

        results.append({
            'candidate': name,
            'n_overlap': int(len(paired)),
            'exact_match_pct': exact,
            'corr_pearson': corr_p,
            'corr_spearman': corr_s,
            'slope': slope,
            'intercept': intercept,
            'residual_std': resid_std,
            'residual_std_norm': resid_norm,
            'mean_u': float(u.mean()),
            'mean_c': float(c.mean()),
            'std_u': float(u.std()),
            'std_c': float(c.std()),
            'pct_zero_u': float((u == 0).mean()),
            'pct_zero_c': float((c == 0).mean()),
            'verdict': verdict,
        })

    df = pd.DataFrame(results)
    if df.empty:
        return df
    return df.sort_values(
        ['exact_match_pct', 'corr_pearson'],
        ascending=[False, False],
    ).reset_index(drop=True)


def _classify_match(exact, corr_p, slope, resid_norm):
    """Heuristic verdict from the scoring signals."""
    if not np.isnan(exact) and exact >= 0.95:
        return 'identity'
    if (not np.isnan(corr_p) and abs(corr_p) >= 0.99
            and not np.isnan(slope) and abs(slope - 1.0) < 0.01):
        return 'near-identity'
    if (not np.isnan(corr_p) and abs(corr_p) >= 0.95
            and not np.isnan(resid_norm) and resid_norm < 0.05):
        return 'scaled'
    if not np.isnan(corr_p) and abs(corr_p) >= 0.7:
        return 'correlated'
    return 'unrelated'


def identify_columns_batch(
    unknown_df: pd.DataFrame,
    candidates: Dict[str, pd.Series],
    unknown_columns: Optional[List[str]] = None,
    top_n: int = 5,
    log_top: bool = True,
    **kwargs,
) -> Dict[str, pd.DataFrame]:
    """Apply identify_column across multiple unknown columns at once.

    Returns dict mapping each unknown column name to its top-N candidate
    matches (DataFrame). Logs the top match per column for quick scanning.
    """
    if unknown_columns is None:
        unknown_columns = [
            c for c in unknown_df.columns
            if pd.api.types.is_numeric_dtype(unknown_df[c])
        ]

    results = {}
    for col in unknown_columns:
        ranked = identify_column(unknown_df[col], candidates, **kwargs)
        if ranked.empty:
            if log_top:
                logger.info(f'{col:30}  no viable candidates (too few overlap?)')
            results[col] = ranked
            continue

        top = ranked.head(top_n)
        if log_top:
            row = top.iloc[0]
            logger.info(
                f'{col:30}  best: {row["candidate"]:35} '
                f'exact={row["exact_match_pct"]:6.1%}  '
                f'corr={row["corr_pearson"]:+.3f}  '
                f'slope={row["slope"]:+.3f}  '
                f'verdict={row["verdict"]}'
            )
        results[col] = top
    return results


# ---- Derived candidates -----------------------------------------------------

def build_derived_candidates(
    base: Dict[str, pd.Series],
    lead_time_key: str = 'LeadTimeDays',
) -> Dict[str, pd.Series]:
    """Generate common supply-chain derived quantities from a dict of base fields.

    These are the kinds of derived quantities that mystery columns often
    turn out to be (e.g. lead-time forecast, replenishment cycle demand).
    Pass the returned dict in alongside the raw candidates so identify_column
    can test both raw fields AND derived expressions.

    Available derivations depend on which base fields are present.
    """
    derived = {}

    def has(*cols):
        return all(col in base for col in cols)

    if has('Forecast4Weekly', lead_time_key):
        derived['Fcst4W_x_LT_over_28'] = (
            base['Forecast4Weekly'] * base[lead_time_key] / 28.0
        )
        derived['Fcst4W_x_LT_over_7'] = (
            base['Forecast4Weekly'] * base[lead_time_key] / 7.0
        )

    if has('ForecastYearly', lead_time_key):
        derived['FcstYr_x_LT_over_365'] = (
            base['ForecastYearly'] * base[lead_time_key] / 365.0
        )

    if has('ForecastWeekly', lead_time_key):
        derived['FcstWk_x_LT_over_7'] = (
            base['ForecastWeekly'] * base[lead_time_key] / 7.0
        )

    if has('OnHand', 'OnOrder'):
        derived['OnHand_plus_OnOrder'] = base['OnHand'] + base['OnOrder']

    if has('OnHand', 'OnOrder', 'BackOrder'):
        derived['Balance'] = base['OnHand'] + base['OnOrder'] - base['BackOrder']

    if has('OrderUpLevel', 'OnHand', 'OnOrder'):
        derived['Need'] = base['OrderUpLevel'] - (base['OnHand'] + base['OnOrder'])

    if has('OrderUpLevel', 'LowStockPoint'):
        derived['OUL_minus_LSP'] = base['OrderUpLevel'] - base['LowStockPoint']

    if has('OnHand', 'LowStockPoint'):
        derived['OnHand_minus_LSP'] = base['OnHand'] - base['LowStockPoint']

    if has('MOQ', 'OrderMultiple'):
        derived['MOQ_x_OrderMultiple'] = base['MOQ'] * base['OrderMultiple']

    if has('SafetyStockCalDays', lead_time_key):
        derived['SSDays_plus_LT'] = base['SafetyStockCalDays'] + base[lead_time_key]

    if has('Forecast4Weekly'):
        for n in (13, 12, 6, 4, 3, 2):
            derived[f'Fcst4W_x_{n}'] = base['Forecast4Weekly'] * n

    if has('ForecastYearly'):
        derived['FcstYr_over_12'] = base['ForecastYearly'] / 12.0
        derived['FcstYr_over_13'] = base['ForecastYearly'] / 13.0
        derived['FcstYr_over_52'] = base['ForecastYearly'] / 52.0
        derived['FcstYr_over_365'] = base['ForecastYearly'] / 365.0

    return derived


# ---- Convenience -----------------------------------------------------------

def make_candidate_dict(df: pd.DataFrame, columns: Optional[List[str]] = None,
                        prefix: str = '') -> Dict[str, pd.Series]:
    """Convert a DataFrame's columns into a {name: Series} dict suitable
    for passing to identify_column. Optionally apply a prefix to disambiguate
    columns from different sources."""
    if columns is None:
        columns = df.columns.tolist()
    return {f'{prefix}{c}': df[c] for c in columns}


if __name__ == '__main__':
    # Self-test: identify a column against a candidate dict where the right
    # answer is known.
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    np.random.seed(42)
    n = 500
    idx = pd.Index([f'SKU{i:04d}' for i in range(n)], name='ItemNumber')

    onhand = pd.Series(np.random.gamma(2, 100, n), index=idx, name='OnHand')
    forecast = pd.Series(np.random.gamma(3, 20, n), index=idx, name='Forecast')
    lead_time = pd.Series(np.full(n, 90.0), index=idx, name='LeadTime')

    # Unknown column = Forecast * LeadTime / 365 (Lead Time Demand)
    unknown = (forecast * lead_time / 365.0).rename('mystery')
    # Add a tiny bit of noise so exact match isn't trivial
    unknown = unknown + np.random.normal(0, 0.001, n)

    candidates = {
        'OnHand': onhand,
        'Forecast': forecast,
        'LeadTime': lead_time,
        'Fcst_x_LT_over_365': forecast * lead_time / 365.0,
        'Fcst_x_LT_over_28':  forecast * lead_time / 28.0,
        'OnHand_x_Forecast':  onhand * forecast,
    }

    result = identify_column(unknown, candidates)
    print(result[['candidate', 'exact_match_pct', 'corr_pearson',
                  'slope', 'verdict']].to_string(index=False))
    # Expected: Fcst_x_LT_over_365 wins with exact_match_pct near 1.0