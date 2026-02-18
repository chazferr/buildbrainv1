"""
BuildBrain Productivity Rates
==============================
Maps work items to hours-per-unit. Pairs with wage rates to calculate labor cost.
"""

PRODUCTIVITY_RATES = {
    # concrete
    "slab_on_grade_4in":            {"unit": "SF",  "hours": 0.065, "trade": "cement_mason"},
    "foundation_wall_10in":         {"unit": "LF",  "hours": 3.20,  "trade": "cement_mason",   "note": "per LF at 4ft height"},
    "footing_continuous":           {"unit": "LF",  "hours": 0.50,  "trade": "cement_mason"},
    "foundation_waterproofing":     {"unit": "SF",  "hours": 0.018, "trade": "laborer"},
    # framing
    "wall_framing_2x6_ext":         {"unit": "SF",  "hours": 0.044, "trade": "carpenter"},
    "wall_framing_2x4_int":         {"unit": "SF",  "hours": 0.036, "trade": "carpenter"},
    "roof_framing_12_12":           {"unit": "SF",  "hours": 0.080, "trade": "carpenter",      "note": "plan area SF"},
    "roof_sheathing":               {"unit": "SF",  "hours": 0.018, "trade": "carpenter"},
    "wall_sheathing":               {"unit": "SF",  "hours": 0.016, "trade": "carpenter"},
    # roofing
    "shingle_install":              {"unit": "SQ",  "hours": 3.00,  "trade": "roofer_composition"},
    "ice_water_shield_install":     {"unit": "SQ",  "hours": 1.00,  "trade": "roofer_composition"},
    "felt_install":                 {"unit": "SQ",  "hours": 0.50,  "trade": "roofer_composition"},
    "drip_edge_install":            {"unit": "LF",  "hours": 0.025, "trade": "roofer_composition"},
    "gutter_install":               {"unit": "LF",  "hours": 0.075, "trade": "carpenter"},
    # exterior
    "siding_install":               {"unit": "SF",  "hours": 0.038, "trade": "carpenter"},
    "housewrap_install":            {"unit": "SF",  "hours": 0.010, "trade": "carpenter"},
    "pvc_trim_install":             {"unit": "LF",  "hours": 0.055, "trade": "carpenter"},
    "window_install":               {"unit": "EA",  "hours": 2.00,  "trade": "carpenter"},
    "door_install_exterior":        {"unit": "EA",  "hours": 2.50,  "trade": "carpenter"},
    # insulation
    "spray_foam_walls":             {"unit": "SF",  "hours": 0.020, "trade": "insulator_spray_foam"},
    "spray_foam_roof":              {"unit": "SF",  "hours": 0.026, "trade": "insulator_spray_foam"},
    "rigid_insulation_foundation":  {"unit": "SF",  "hours": 0.016, "trade": "laborer"},
    # drywall
    "gwb_hang":                     {"unit": "SF",  "hours": 0.014, "trade": "carpenter"},
    "gwb_tape_finish":              {"unit": "SF",  "hours": 0.022, "trade": "drywall_taper"},
    # flooring
    "lvt_install":                  {"unit": "SF",  "hours": 0.032, "trade": "carpenter"},
    "carpet_install":               {"unit": "SF",  "hours": 0.028, "trade": "carpenter"},
    "tile_install_floor":           {"unit": "SF",  "hours": 0.070, "trade": "tile_setter"},
    "tile_install_wall_shower":     {"unit": "SF",  "hours": 0.095, "trade": "tile_setter"},
    # paint
    "paint_interior_2coat":         {"unit": "SF",  "hours": 0.016, "trade": "painter"},
    "paint_exterior_trim":          {"unit": "LF",  "hours": 0.024, "trade": "painter"},
    # plumbing
    "plumbing_rough_per_fixture":   {"unit": "EA",  "hours": 8.00,  "trade": "plumber"},
    "plumbing_fixture_set":         {"unit": "EA",  "hours": 3.00,  "trade": "plumber"},
    # electrical
    "electrical_rough_per_circuit": {"unit": "EA",  "hours": 4.00,  "trade": "electrician"},
    "panel_install_200amp":         {"unit": "EA",  "hours": 16.00, "trade": "electrician"},
    "device_install":               {"unit": "EA",  "hours": 0.45,  "trade": "electrician"},
    "light_fixture_recessed":       {"unit": "EA",  "hours": 1.00,  "trade": "electrician"},
    # hvac
    "mini_split_zone_install":      {"unit": "EA",  "hours": 12.00, "trade": "hvac_sheet_metal"},
    # sitework
    "excavation_machine":           {"unit": "CY",  "hours": 0.14,  "trade": "equip_op_excavator"},
    "backfill_compact":             {"unit": "CY",  "hours": 0.20,  "trade": "equip_op_skid_steer"},
    "sidewalk_ramp_ct_dot":         {"unit": "EA",  "hours": 18.00, "trade": "cement_mason"},
}


def calculate_labor(work_item: str, quantity: float, wage_regime: str = "CT_DOL_RESIDENTIAL") -> dict:
    """
    Returns full labor calculation trace for a work item.

    Returns:
    {
        "work_item": str,
        "quantity": float,
        "unit": str,
        "hours_per_unit": float,
        "total_hours": float,
        "trade": str,
        "wage_rate": float,
        "labor_cost": float,
        "wage_regime": str,
        "trace": str  # human-readable calculation string
    }
    """
    from wage_rates import get_wage

    if work_item not in PRODUCTIVITY_RATES:
        return None

    prod = PRODUCTIVITY_RATES[work_item]
    trade = prod["trade"]
    hours_per_unit = prod["hours"]
    unit = prod["unit"]
    wage = get_wage(trade, wage_regime)
    total_hours = quantity * hours_per_unit
    labor_cost = total_hours * wage

    trace = f"{quantity} {unit} \u00d7 {hours_per_unit} hr/{unit} \u00d7 ${wage:.2f}/hr = ${labor_cost:,.0f}"

    return {
        "work_item": work_item,
        "quantity": quantity,
        "unit": unit,
        "hours_per_unit": hours_per_unit,
        "total_hours": round(total_hours, 2),
        "trade": trade,
        "wage_rate": wage,
        "labor_cost": round(labor_cost, 2),
        "wage_regime": wage_regime,
        "trace": trace
    }
