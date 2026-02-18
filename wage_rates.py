"""
BuildBrain Wage Rate Tables
============================
Official CT prevailing wage and Davis-Bacon rates for construction trades.

SOURCE 1: CT DOL Residential Prevailing Wage (ID# 25-6853, effective July 3, 2025)
SOURCE 2: Davis-Bacon Building CT20250023, New Haven County, June 6, 2025
"""

CT_WAGE_RATES = {
    "CT_DOL_RESIDENTIAL": {
        "label": "CT State Prevailing Wage — Residential Construction",
        "source": "CT DOL ID# 25-6853, effective July 3, 2025",
        "annual_adjustment": "July 1 each year — verify at www.ct.gov/dol",
        "trades": {
            "carpenter":               {"base": 26.55, "benefits": 12.82, "total": 39.37},
            "electrician":             {"base": 47.40, "benefits": 35.32, "total": 84.14, "note": "add 3% of gross"},
            "plumber":                 {"base": 50.98, "benefits": 35.85, "total": 86.83},
            "hvac_sheet_metal":        {"base": 44.70, "benefits": 44.38, "total": 89.08},
            "roofer_composition":      {"base": 44.50, "benefits": 24.74, "total": 69.24},
            "cement_mason":            {"base": 42.03, "benefits": 17.50, "total": 59.53},
            "tile_setter":             {"base": 40.00, "benefits": 32.72, "total": 72.72},
            "tile_finisher":           {"base": 33.00, "benefits": 27.40, "total": 60.40},
            "painter":                 {"base": 27.85, "benefits": 10.10, "total": 37.95},
            "drywall_taper":           {"base": 28.33, "benefits": 10.10, "total": 38.43},
            "glazier":                 {"base": 30.16, "benefits": 10.10, "total": 40.26},
            "laborer":                 {"base": 25.24, "benefits": 15.86, "total": 41.10},
            "insulator_spray_foam":    {"base": 48.81, "benefits": 34.05, "total": 82.86},
            "sprinkler_fitter":        {"base": 53.76, "benefits": 33.44, "total": 87.20},
            "equip_op_excavator":      {"base": 51.92, "benefits": 29.80, "total": 81.72},
            "equip_op_skid_steer":     {"base": 48.67, "benefits": 29.80, "total": 78.47},
            "equip_op_dozer":          {"base": 50.22, "benefits": 29.80, "total": 80.02},
        }
    },
    "DAVIS_BACON_BUILDING_NEW_HAVEN": {
        "label": "Federal Davis-Bacon — Building Construction, New Haven County CT",
        "source": "General Decision CT20250023, effective June 6, 2025",
        "note": "Does NOT apply to single family homes or apartments 4 stories or under",
        "trades": {
            "carpenter":               {"base": 42.03, "benefits": 29.19, "total": 71.22},
            "electrician":             {"base": 46.48, "benefits": 36.66, "total": 84.53},
            "plumber":                 {"base": 50.58, "benefits": 35.85, "total": 86.43},
            "hvac_sheet_metal":        {"base": 43.89, "benefits": 42.90, "total": 86.79},
            "roofer_composition":      {"base": 44.15, "benefits": 22.23, "total": 66.38},
            "cement_mason":            {"base": 42.61, "benefits": 34.89, "total": 77.50},
            "painter":                 {"base": 38.07, "benefits": 25.70, "total": 63.77},
            "drywall_taper":           {"base": 38.82, "benefits": 25.70, "total": 64.52},
            "glazier":                 {"base": 41.18, "benefits": 24.55, "total": 65.73},
            "laborer":                 {"base": 27.85, "benefits": 18.04, "total": 45.89},
            "insulator_spray_foam":    {"base": 47.60, "benefits": 33.30, "total": 80.90},
            "ironworker":              {"base": 45.25, "benefits": 41.27, "total": 86.52},
        }
    }
}


def get_wage(trade_key: str, regime: str = "CT_DOL_RESIDENTIAL") -> float:
    """
    Returns the fully loaded hourly rate (base + benefits) for a trade.
    Falls back to CT_DOL_RESIDENTIAL if trade not found in requested regime.
    regime options: "CT_DOL_RESIDENTIAL" or "DAVIS_BACON_BUILDING_NEW_HAVEN"
    """
    regime_data = CT_WAGE_RATES.get(regime, CT_WAGE_RATES["CT_DOL_RESIDENTIAL"])
    trade = regime_data["trades"].get(trade_key)
    if trade:
        return trade["total"]
    # fallback to residential
    fallback = CT_WAGE_RATES["CT_DOL_RESIDENTIAL"]["trades"].get(trade_key)
    if fallback:
        return fallback["total"]
    return 41.10  # default to general laborer rate if trade not found
