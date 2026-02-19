"""
BuildBrain Material Price Database
====================================
Baseline material prices for residential construction in the Northeast US (CT region).
Each entry: unit, unit_price, manufacturer_note, last_updated
"""

MATERIAL_DB = {

    # ── CONCRETE & FOUNDATION ──────────────────────────────
    "concrete_ready_mix_3000psi":   {"unit": "CY",  "price": 185.00, "note": "Ready mix 3000 PSI delivered"},
    "concrete_ready_mix_4000psi":   {"unit": "CY",  "price": 198.00, "note": "Ready mix 4000 PSI delivered"},
    "rebar_60_grade":               {"unit": "LB",  "price": 0.95,   "note": "#4 and #5 rebar"},
    "concrete_forms_snap_tie":      {"unit": "SF",  "price": 3.80,   "note": "Plywood form panels, hardware"},
    "welded_wire_mesh_6x6_10x10":   {"unit": "SF",  "price": 0.35,   "note": "6x6 W1.4xW1.4 WWM"},
    "vapor_barrier_10mil":          {"unit": "SF",  "price": 0.18,   "note": "10 mil poly under slab"},
    "rigid_insulation_r10_2in":     {"unit": "SF",  "price": 1.45,   "note": "2\" XPS foam board R-10"},
    "foundation_damp_henry788":     {"unit": "SF",  "price": 0.85,   "note": "Henry 788 fibered asphalt emulsion"},
    "foundation_wp_gcp_preprufe160":{"unit": "SF",  "price": 1.40,   "note": "GCP Applied Technologies Preprufe 160"},
    "drainage_mat_hydroduct220":    {"unit": "SF",  "price": 0.95,   "note": "GCP Applied Technologies Hydroduct 220"},
    "anchor_bolts_5_8":             {"unit": "EA",  "price": 4.50,   "note": "5/8\" galv anchor bolt"},
    "crushed_stone_3_4":            {"unit": "CY",  "price": 48.00,  "note": "3/4\" crushed stone base"},

    # ── FRAMING LUMBER & STRUCTURAL ───────────────────────
    "lumber_2x4_kd":                {"unit": "LF",  "price": 0.72,   "note": "2x4 KD SPF framing"},
    "lumber_2x6_kd":                {"unit": "LF",  "price": 1.05,   "note": "2x6 KD SPF framing"},
    "lumber_2x8_kd":                {"unit": "LF",  "price": 1.45,   "note": "2x8 KD SPF framing"},
    "lumber_2x10_kd":               {"unit": "LF",  "price": 1.85,   "note": "2x10 KD SPF framing"},
    "lumber_2x12_kd":               {"unit": "LF",  "price": 2.25,   "note": "2x12 KD SPF framing"},
    "plywood_5_8_cdx_roof":         {"unit": "SF",  "price": 1.85,   "note": "5/8\" CDX plywood roof sheathing"},
    "plywood_1_2_cdx_wall":         {"unit": "SF",  "price": 1.35,   "note": "1/2\" CDX plywood wall sheathing"},
    "lvl_1_75x11_875":              {"unit": "LF",  "price": 32.00,  "note": "1-3/4\"x11-7/8\" LVL beam"},
    "lvl_1_75x9_5":                 {"unit": "LF",  "price": 26.00,  "note": "1-3/4\"x9-1/2\" LVL beam"},
    "simpson_h2_5a_clip":           {"unit": "EA",  "price": 1.85,   "note": "Simpson H2.5A hurricane clip"},
    "simpson_lsta24_strap":         {"unit": "EA",  "price": 4.20,   "note": "Simpson LSTA24 strap"},
    "pt_lumber_2x6":                {"unit": "LF",  "price": 1.35,   "note": "2x6 pressure treated sill plate"},
    "framing_hardware_misc":        {"unit": "SF_FLOOR", "price": 0.85, "note": "Nails, bolts, misc hardware per SF floor"},

    # ── ROOFING ────────────────────────────────────────────
    "shingles_certainteed_xt25":    {"unit": "SQ",  "price": 148.00, "note": "CertainTeed XT-25 Nickel Gray"},
    "shingles_certainteed_landmark":{"unit": "SQ",  "price": 185.00, "note": "CertainTeed Landmark architectural"},
    "ice_water_shield_grace":       {"unit": "SQ",  "price": 185.00, "note": "Grace Ice & Water Shield 36\" min"},
    "felt_underlayment_30lb":       {"unit": "SQ",  "price": 28.00,  "note": "30 lb asphalt felt"},
    "drip_edge_aluminum":           {"unit": "LF",  "price": 1.25,   "note": "Aluminum drip edge 2\"x2\""},
    "ridge_cap_shingles":           {"unit": "LF",  "price": 3.20,   "note": "Ridge cap shingles"},
    "roof_nails_coil":              {"unit": "SQ",  "price": 4.50,   "note": "Coil roofing nails per square"},
    "gutter_ogee_5in":              {"unit": "LF",  "price": 12.50,  "note": "5\" K-style ogee gutter aluminum"},
    "gutter_leaf_screen":           {"unit": "LF",  "price": 8.50,   "note": "Leaf screen / gutter guard"},
    "downspout_2x3":                {"unit": "LF",  "price": 6.50,   "note": "2x3 rectangular downspout"},

    # ── EXTERIOR ENVELOPE ─────────────────────────────────
    "siding_certainteed_mainstreet_d4":   {"unit": "SF", "price": 3.20, "note": "CertainTeed Mainstreet Double 4\" Woodgrain"},
    "siding_certainteed_cedar_scallop":   {"unit": "SF", "price": 4.85, "note": "CertainTeed Cedar Impressions Scallop Colonial White"},
    "housewrap_hydrogap":           {"unit": "SF",  "price": 0.42,   "note": "Benjamin Obdyke HydroGap drainage wrap"},
    "housewrap_tyvek":              {"unit": "SF",  "price": 0.28,   "note": "DuPont Tyvek HomeWrap"},
    "pvc_trim_3_4x5_5":             {"unit": "LF",  "price": 4.20,   "note": "3/4\"x5-1/2\" PVC trim board"},
    "pvc_trim_3_4x3_5":             {"unit": "LF",  "price": 3.10,   "note": "3/4\"x3-1/2\" PVC trim board"},
    "membrane_flashing_self_adh":   {"unit": "SF",  "price": 1.85,   "note": "Self-adhering membrane flashing"},
    "aluminum_step_flashing":       {"unit": "EA",  "price": 1.20,   "note": "3\"x4\" aluminum step flashing"},

    # ── WINDOWS & DOORS ───────────────────────────────────
    "window_marvin_ultimate_dh_small":  {"unit": "EA", "price": 850.00, "note": "Marvin Signature Ultimate DH, 2'-0\"x3'-0\" approx"},
    "window_marvin_ultimate_dh_med":    {"unit": "EA", "price": 700.00, "note": "Marvin Signature Ultimate DH, 2'-0\"x4'-0\" approx"},
    "window_marvin_ultimate_dh_large":  {"unit": "EA", "price": 1450.00,"note": "Marvin Signature Ultimate DH, 3'-0\"x5'-0\" approx"},
    "door_exterior_insulated_alum":     {"unit": "EA", "price": 1850.00,"note": "Insulated aluminum door + frame, per drawings"},
    "door_interior_hollow_core":        {"unit": "EA", "price": 185.00, "note": "Interior hollow core door pre-hung"},
    "door_hardware_entry_schlage":      {"unit": "EA", "price": 285.00, "note": "Schlage Merano lever entry lockset"},
    "door_hardware_privacy":            {"unit": "EA", "price": 95.00,  "note": "Privacy lever handle set"},

    # ── INSULATION ────────────────────────────────────────
    "spray_foam_closed_cell_r30":   {"unit": "SF",  "price": 4.20,   "note": "Closed cell spray foam 5.5\" R-30 (walls)"},
    "spray_foam_closed_cell_r60":   {"unit": "SF",  "price": 8.40,   "note": "Closed cell spray foam ~9\" R-60 (roof/attic)"},
    "spray_foam_closed_cell_r49":   {"unit": "SF",  "price": 6.80,   "note": "Closed cell spray foam R-49 (cathedral ceiling)"},
    "batt_insulation_r21":          {"unit": "SF",  "price": 0.95,   "note": "R-21 kraft-faced fiberglass batt, 2x6 wall"},
    "batt_insulation_sound_3in":    {"unit": "SF",  "price": 0.65,   "note": "3\" sound attenuation batt (interior walls)"},

    # ── DRYWALL ───────────────────────────────────────────
    "gwb_5_8_type_x":               {"unit": "SF",  "price": 0.85,   "note": "5/8\" Type X gypsum wallboard"},
    "gwb_1_2_standard":             {"unit": "SF",  "price": 0.65,   "note": "1/2\" standard gypsum wallboard"},
    "gwb_5_8_type_mr":              {"unit": "SF",  "price": 1.05,   "note": "5/8\" moisture resistant (bath/kitchen)"},
    "joint_compound_tape":          {"unit": "SF",  "price": 0.32,   "note": "Joint compound, tape, corner bead — per SF GWB"},
    "drywall_screws_misc":          {"unit": "SF",  "price": 0.08,   "note": "Screws, misc fasteners per SF GWB"},

    # ── FLOORING ──────────────────────────────────────────
    "lvt_mid_range":                {"unit": "SF",  "price": 4.50,   "note": "LVT luxury vinyl tile/plank, click-lock, mid-range"},
    "lvt_premium":                  {"unit": "SF",  "price": 6.50,   "note": "LVT luxury vinyl plank, premium"},
    "carpet_commercial_grade":      {"unit": "SF",  "price": 3.20,   "note": "Commercial grade carpet with pad"},
    "ceramic_tile_floor":           {"unit": "SF",  "price": 4.50,   "note": "Ceramic floor tile 12\"x12\" mid-range"},
    "porcelain_tile_floor":         {"unit": "SF",  "price": 6.80,   "note": "Porcelain floor tile premium"},
    "tile_setting_material":        {"unit": "SF",  "price": 1.85,   "note": "Thinset, grout, backer board per SF tile"},

    # ── PAINT ─────────────────────────────────────────────
    "paint_sw_interior_2coat":      {"unit": "SF",  "price": 0.28,   "note": "SW Alabaster SW7008 primer + 2 coats, material only"},
    "paint_primer":                 {"unit": "SF",  "price": 0.12,   "note": "Interior primer per SF"},
    "paint_exterior_trim":          {"unit": "LF",  "price": 0.45,   "note": "Exterior trim paint per LF"},

    # ── PLUMBING FIXTURES ─────────────────────────────────
    "toilet_penguin_254":           {"unit": "EA",  "price": 650.00, "note": "Penguin 254 2-piece elongated, per spec"},
    "tub_sterling_ensemble_ada":    {"unit": "EA",  "price": 480.00, "note": "Sterling Ensemble 71101122 ADA, per spec"},
    "shower_set_miseno_mia":        {"unit": "EA",  "price": 385.00, "note": "Miseno Mia MS-550515E-S brushed nickel, per spec"},
    "vanity_36in_ada":              {"unit": "EA",  "price": 850.00, "note": "36\" vanity ADA white, per spec"},
    "kitchen_faucet_touchless":     {"unit": "EA",  "price": 320.00, "note": "Touchless kitchen faucet"},
    "bath_faucet_touchless":        {"unit": "EA",  "price": 280.00, "note": "Touchless bath faucet"},
    "dishwasher_rough_supply":      {"unit": "EA",  "price": 420.00, "note": "Dishwasher supply/drain rough-in"},
    "plumbing_rough_pipe_per_fix":  {"unit": "EA",  "price": 420.00, "note": "Supply + drain rough-in pipe per fixture"},
    "grab_bar_bobrick_b5806":       {"unit": "EA",  "price": 185.00, "note": "Bobrick B-5806 wall mount grab bar, per spec"},

    # ── ELECTRICAL ────────────────────────────────────────
    "panel_200amp":                 {"unit": "EA",  "price": 2200.00,"note": "200A main panel + breakers"},
    "wire_per_circuit_rough":       {"unit": "EA",  "price": 180.00, "note": "Wire, conduit, boxes per branch circuit rough-in"},
    "outlet_device":                {"unit": "EA",  "price": 18.00,  "note": "Duplex outlet device + cover plate"},
    "switch_device":                {"unit": "EA",  "price": 22.00,  "note": "Switch device + cover plate"},
    "gfi_outlet":                   {"unit": "EA",  "price": 38.00,  "note": "GFCI outlet + cover plate"},
    "light_recessed_lithonia_wf6":  {"unit": "EA",  "price": 45.00,  "note": "Lithonia WF6 LED 30K recessed, per spec"},
    "light_exterior_sconce":        {"unit": "EA",  "price": 185.00, "note": "LNC exterior LED sconce, per spec"},
    "light_pendant_craftmade":      {"unit": "EA",  "price": 220.00, "note": "Craftmade Gaze pendant, per spec"},
    "light_bath_bar":               {"unit": "EA",  "price": 95.00,  "note": "Bath vanity bar light"},

    # ── HVAC ──────────────────────────────────────────────
    "mitsubishi_mini_split_zone":   {"unit": "EA",  "price": 3200.00,"note": "Mitsubishi mini-split zone (head + line set material)"},
    "hvac_condensate_drain":        {"unit": "EA",  "price": 85.00,  "note": "Condensate drain line per zone"},

    # ── KITCHEN & BATH FINISH ─────────────────────────────
    "cabinets_express_rta_per_ln":  {"unit": "LF",  "price": 220.00, "note": "Express Kitchens Barcelona White RTA per LF of cabinet run"},
    "countertop_laminate":          {"unit": "SF",  "price": 28.00,  "note": "Laminate countertop material"},
    "vanity_mirror_ada":            {"unit": "EA",  "price": 185.00, "note": "ADA mirror mounted 40\" AFF max, per spec"},
    "towel_bar_harney_boca_grande": {"unit": "EA",  "price": 85.00,  "note": "Harney Boca Grande 18\" towel bar, per spec"},
    "towel_ring_harney":            {"unit": "EA",  "price": 65.00,  "note": "Harney Boca Grande towel ring, per spec"},
    "tp_holder_harney":             {"unit": "EA",  "price": 75.00,  "note": "Harney Boca Grande TP holder, per spec"},

    # ── SITEWORK ──────────────────────────────────────────
    "bituminous_curb":              {"unit": "LF",  "price": 22.00,  "note": "Bituminous concrete lip curbing"},
    "sidewalk_concrete_4in":        {"unit": "SF",  "price": 8.50,   "note": "4\" concrete sidewalk"},
    "loam_topsoil":                 {"unit": "CY",  "price": 65.00,  "note": "Screened loam / topsoil"},
    "seeding_hydroseed":            {"unit": "SF",  "price": 0.18,   "note": "Hydroseed lawn establishment"},
}


def get_material_price(material_key: str) -> dict:
    """
    Returns {"unit": str, "price": float, "note": str} for a material key.
    Returns None if not found — caller should flag as unknown material.
    """
    return MATERIAL_DB.get(material_key)


def lookup_by_spec(manufacturer: str, description: str) -> dict | None:
    """
    Fuzzy match a specified material to the database.
    Checks manufacturer name and description keywords against db notes.
    Returns the best match entry or None.
    """
    manufacturer_lower = manufacturer.lower()
    description_lower = description.lower()

    best_match = None
    best_score = 0

    for key, entry in MATERIAL_DB.items():
        note_lower = entry["note"].lower()
        score = 0
        # Manufacturer match scores higher
        if manufacturer_lower and manufacturer_lower in note_lower:
            score += 3
        # Description keyword matches
        for word in description_lower.split():
            if len(word) > 3 and word in note_lower:
                score += 1
        if score > best_score:
            best_score = score
            best_match = {**entry, "key": key, "match_score": score}

    return best_match if best_score >= 2 else None
