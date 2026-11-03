"""Shared staircase rules: the ONE place the tracker (staircase_tracker.py) and the bot (bot_v4.py) both read.
Edit a rule here and both pick it up, so they can't drift apart. Needs no extra packages.
Put this file next to both of them (same folder / same repo as each service).

Colour scale by composite: Cyan < 10, Green 10-45, Grey 45-55, Orange 55-90, Red >= 90.
A setup is (direction, id, 4H colour, 30m colour, 5m colour BEFORE, 5m colour NOW).
Setups that only exist in the tracker (the untested U-numbers) and the per-file alert wording stay in their own files.
"""
import os


def _env(name, default):
    return float(os.environ.get(name, str(default)))


# (direction, letter, slow colour, mid colour, fast colour BEFORE, fast colour NOW)
RULES = [
    ("short", "A++", "Red",    "Orange", "Red",    "Orange"),
    ("short", "B", "Red",    "Orange", "Orange", "Grey"),
    ("short", "A+", "Orange", "Red",    "Red",    "Orange"),
    ("short", "B+", "Orange", "Red",   "Orange", "Grey"),
    ("short", "D", "Green",  "Green",  "Orange", "Grey"),
    ("long",  "A++", "Cyan",   "Green",  "Cyan",   "Green"),
    ("long",  "B", "Cyan",   "Green",  "Green",  "Grey"),
    ("long",  "A+", "Green",  "Cyan",   "Cyan",   "Green"),
    ("long",  "B+", "Green",  "Cyan",   "Green",  "Grey"),
    ("long",  "D", "Orange", "Orange", "Green",  "Grey"),
    ("short", "G", "Cyan",   "Orange", "Red",    "Orange"),
    ("short", "H", "Cyan",   "Orange", "Grey",   "Green"),
    ("short", "E", "Green",  "Green",  "Green",  "Grey"),
    ("short", "F", "Orange", "Orange", "Orange", "Orange"),
    ("short", "K", "Green",  "Red",    "Red",    "Orange"),
    ("short", "L", "Green",  "Orange", "Orange", "Grey"),
    ("short", "M", "Green",  "Green",  "Grey",   "Green"),
    ("long",  "M", "Orange", "Orange", "Grey",   "Orange"),
    ("short", "N", "Cyan",   "Green",  "Grey",   "Orange"),
    ("long",  "N", "Red",    "Orange", "Grey",   "Green"),
    ("long",  "K", "Orange", "Cyan",   "Cyan",   "Green"),
    ("long",  "L", "Orange", "Green",  "Green",  "Grey"),
    ("long",  "F", "Green",  "Green",  "Green",  "Green"),
    ("long",  "E", "Orange", "Orange", "Orange", "Grey"),
    ("long",  "G", "Red",    "Green",  "Cyan",   "Green"),
    ("long",  "H", "Red",    "Green",  "Grey",   "Orange"),
    ("long",  "J", "Green",  "Green",  "Red",    "Orange"),
    ("short", "J", "Orange", "Orange", "Green",  "Cyan"),
    ("long",  "O", "Green",  "Green",  "Green",  "Orange"),   # a state (like F), the 5m "before" colour does not matter
    ("long",  "FV2", "Green", "Green", "Green",  "Green"),    # a state, the second version of Long F (kept alongside F to compare)
]

NOTES = {
    ("short", "A++"): "Early entry, best R",   ("long", "A++"): "Early entry, best R",
    ("short", "B"): "Final confirmation",    ("long", "B"): "Final confirmation",
    ("long", "A+"): "Test, possibly the best signal: reversal off a Cyan 30m",
    ("short", "A+"): "Test, possibly the best signal: reversal off a Red 30m",
    ("long", "B+"): "Test, possibly the best signal: Cyan 30m, 5m fades to Grey",
    ("short", "B+"): "Test, possibly the best signal: Red 30m, 5m fades to Grey",
    ("short", "D"): "Last premium short in a heavy downtrend: price pushes down and holds, 5m Orange to Grey, above the 30m and 5m midlines", ("long", "D"): "Last discount long in a heavy premium trend: price pushes up and holds, 5m Green to Grey, below the 30m and 5m midlines",
    ("long", "G"): "Lowest discount in a strong higher timeframe",
    ("long", "H"): "2nd opportunity: last discount in a premium trend before the uptrend is lost",
    ("short", "G"): "Highest premium in a strong higher timeframe",
    ("short", "K"): "Green 4H, Red 30m: the 5m turns down off Red",
    ("short", "L"): "Green 4H, Orange 30m: the 5m fades from Orange to Grey",
    ("short", "M"): "Trend continuation: 4H and 30m both Green, the 5m dips back into Green after a bounce to Grey (one step after Short E)",
    ("long", "M"): "Trend continuation: 4H and 30m both Orange, the 5m pushes back into Orange after a dip to Grey (one step after Long E)",
    ("short", "N"): "4H Cyan, 30m Green: the 5m turns up into Orange, only with the 5m composite above 60",
    ("long", "N"): "4H Red, 30m Orange: the 5m turns down into Green, only with the 5m composite below 40",
    ("long", "K"): "Orange 4H, Cyan 30m: the 5m turns up off Cyan",
    ("long", "L"): "Orange 4H, Green 30m: the 5m fades from Green to Grey",
    ("long", "J"): "HTF discount, LTF showing strong strength: the 5m comes off Red into Orange, with the trend still up",
    ("short", "J"): "HTF premium, LTF showing strong weakness, recovering the Cyan to Green to Cyan trend",
    ("long", "F"): "All Green long, only when price is below the midline on all three timeframes (4H, 30m and 5m) and the 5m composite is above 15 but not above 30",
    ("short", "F"): "All Orange short, only when price is above the midline on all three timeframes (4H, 30m and 5m) and the 5m composite is below 85",
    ("short", "E"): "Fading the bounce: 5m climbs out of Green into Grey with 4H and 30m both Green (the old Long C, flipped)",
    ("long", "E"): "Flip of the retired Short C: 5m drops from Orange into Grey with 4H and 30m both Orange",
    ("short", "H"): "2nd opportunity: last premium in a discount trend before the downtrend is lost",
    ("long", "O"): "Trend-continuation long: 4H and 30m both Green, the 5m is Orange but not stretched (WPR above -75, below -39). Expect the push up to carry on. Seen on 3 Nov",
    ("long", "FV2"): "Dip-buy long inside an uptrend: 4H, 30m and 5m all Green, price below the 5m midline, 5m WPR above -74 and below -60. Expect the dip to bounce and the uptrend to resume. Version 2 of Long F, kept to compare against it",
}

# Blocked combos: (side, 4H, 30m, 5m now, reason, date added). Also listed on the Blocked tab.
# Blocked combos: (side, 4H, 30m, 5m now). A setup/alignment in this direction is never logged or alerted in that state.
# Add more rows here as impossible/poor states are identified.
BLOCKED = [
    ("short", "*", "Grey", "*", "30m is Grey: no trade in either direction until you decide to trade off the 5m and 4H alone", "25 Oct"),
    ("long",  "*", "Grey", "*", "30m is Grey: no trade in either direction until you decide to trade off the 5m and 4H alone", "25 Oct"),
    ("short", "*", "*", "Cyan", "5m already at the floor: shorting a move that's already done", "24 Oct"),
    ("long", "*", "*", "Red", "5m already at the top: mirror of the short block", "24 Oct"),
    ("short", "*", "*", "Red", "5m at the top: you can't know where the top is, too risky to short into it", "25 Oct"),
    ("long", "*", "*", "Cyan", "5m at the floor: you can't know where the bottom is, too risky to long into it", "25 Oct"),
    ("short", "Cyan", "Green", "Green", "4H Cyan / 30m Green: shorts only allowed on a Grey or Orange 5m", "24 Oct"),
    ("short", "Cyan", "Green", "Cyan", "4H Cyan / 30m Green: shorts only allowed on a Grey or Orange 5m", "24 Oct"),
    ("short", "Red", "Red", "Red", "All Red: no trade either way (logged as a count only)", "24 Oct"),
    ("long", "Red", "Red", "Red", "All Red: no trade either way (logged as a count only)", "24 Oct"),
    ("short", "Green", "Green", "Orange", "5m overshoot above two Green timeframes: no trade either way", "24 Oct"),
    ("long", "Green", "Green", "Orange", "5m overshoot above two Green timeframes: no trade either way", "24 Oct"),
    ("short", "Cyan", "Cyan", "Cyan", "All Cyan: no trade either way (logged as a count only)", "24 Oct"),
    ("long", "Cyan", "Cyan", "Cyan", "All Cyan: no trade either way (logged as a count only)", "24 Oct"),
    ("long", "Cyan", "Orange", "Orange", "4H Cyan / 30m Orange / 5m Orange: no long, manual trades included", "25 Oct"),
    ("long", "Cyan", "Orange", "Green", "4H Cyan / 30m Orange / 5m Green: no long, manual trades included", "25 Oct"),
    ("short", "Red", "Green", "Green", "4H Red / 30m Green / 5m Green: no short, manual trades included", "25 Oct"),
    ("short", "Red", "Green", "Orange", "4H Red / 30m Green / 5m Orange: no short, manual trades included", "25 Oct"),
    ("long", "Red", "Orange", "Orange", "Opposite side of the Short A++ state", "24 Oct"),
    ("short", "Green", "Orange", "Grey", "4H Green / 30m Orange / 5m Grey: weak counter-4H backdrop, no trade either way", "25 Oct"),
    ("long", "Green", "Orange", "Grey", "4H Green / 30m Orange / 5m Grey: weak counter-4H backdrop, no trade either way", "25 Oct"),
    ("short", "Green", "Red", "*", "4H Green / 30m Red: a 4H Green that wants to turn Grey, not a short, whatever the 5m does", "2 Nov"),
]

# Blocked only when the 5m got there from a particular colour: (side, 4H, 30m, 5m BEFORE, 5m now, reason, date added).
BLOCKED_FROM = [
    ("long", "Green", "Orange", "Red", "Orange", "4H Green / 30m Orange / 5m Red→Orange: the 5m only just came off the top, no long", "25 Oct"),
]

# "Episode" setups (Long F = All Green, Short F = All Orange): not a 5m turn, but a state. Fired once when the colours AND the
# midline conditions first line up, and not again until they stop lining up.
EPISODE = {("long", "F"): "Green", ("short", "F"): "Orange", ("long", "O"): "Orange", ("long", "FV2"): "Green"}   # a state, not a 5m turn: fires once per stretch when its colours (4H, 30m, 5m now) and conditions line up

# Setups you want to be able to spot at a glance when looking back at results: shown next to the letter everywhere.
TAGS = {("long", "O"): "\u2b50", ("long", "FV2"): "\u2b50"}


def tag(side, letter):
    return TAGS.get((side, letter), "")

# Setups allowed to fire although their state is on the BLOCKED list (the tracker adds its untested U10 on top of these).
BLOCK_EXEMPT = {("long", "J"), ("short", "J"), ("short", "L"), ("long", "O")}   # Long J ends on Green/Green/Orange, Short J on a 5m Cyan, Short L on a Green / Orange / Grey

# Setups that skip the general midline/WPR gates (Short D has its own midline conditions below; the tracker adds D and its untested U9/U10 on top).
GATE_EXEMPT = {("short", "B"), ("short", "E"), ("long", "E"), ("short", "F"), ("long", "F"), ("short", "M"), ("long", "M"), ("long", "O"), ("long", "FV2")}

# Midline conditions a setup needs at fire: which side of each timeframe's midline price must be on.
SETUP_CONDS = {("long", "FV2"): {"mid5_side": "below"},
               ("short", "F"): {"mid4h_side": "above", "mid30_side": "above", "mid5_side": "above"},
               ("long", "F"): {"mid4h_side": "below", "mid30_side": "below", "mid5_side": "below"},
               ("short", "D"): {"mid30_side": "above", "mid5_side": "above"},
               ("long", "D"): {"mid30_side": "below", "mid5_side": "below"}}

# Per-setup composite limits, read live. Keys: comp5 / comp30 (the 5m / 30m composite) + gt (strictly above), lte (at most), gte (at least), lt (strictly below).
COMP = {("long", "F"): {"comp5_gt": _env("STAIRCASE_LONG_F_5M_MIN", 15), "comp5_lte": _env("STAIRCASE_LONG_F_5M_MAX", 30)},
        ("short", "F"): {"comp5_gte": _env("STAIRCASE_SHORT_F_5M_MIN", 70), "comp5_lt": _env("STAIRCASE_SHORT_F_5M_MAX", 85)},
        ("short", "N"): {"comp5_gt": _env("STAIRCASE_SHORT_N_5M_MIN", 60)},
        ("long", "N"): {"comp5_lt": _env("STAIRCASE_LONG_N_5M_MAX", 40)},
        ("long", "A+"): {"comp30_gt": _env("STAIRCASE_LONG_AP_30M_MIN", 5)},
        ("short", "K"): {"comp30_lte": _env("STAIRCASE_SHORT_K_30M_MAX", 95)}}

# Per-setup 5m WPR limit: ("below"/"above", level) or ("between", low, high) = above low, up to and including high.
WPR5_LIMIT = {("long", "A+"): ("between", _env("STAIRCASE_LONG_AP_WPR5_LOW", -85), _env("STAIRCASE_LONG_AP_WPR5_HIGH", -75)),
              ("long", "O"): ("between", _env("STAIRCASE_LONG_O_WPR5_LOW", -75), _env("STAIRCASE_LONG_O_WPR5_HIGH", -39)),      # above -75, up to -39
              ("long", "FV2"): ("between", _env("STAIRCASE_LONG_FV2_WPR5_LOW", -74), _env("STAIRCASE_LONG_FV2_WPR5_HIGH", -60))}  # above -74, up to -60

# General gates (5m WPR levels) and the Green-30m / All Green / All Orange rules.
SHORT_GATE_WPR = _env("STAIRCASE_SHORT_GATE_WPR", -25)       # no short when price is below the 30m midline and the 5m WPR is above this
SHORT_GATE_OVERRIDE = {("short", "K"): _env("STAIRCASE_SHORT_K_GATE_WPR", -10)}   # per-setup level instead of the one above
LONG_GATE_WPR = _env("STAIRCASE_LONG_GATE_WPR", -75)         # no long when price is above the 30m midline and the 5m WPR is below this
OVERSOLD_WPR = _env("STAIRCASE_LONG_OVERSOLD_WPR", -85)      # no long when the 30m WPR is below this, unless the 5m WPR is back above it
GREEN_BAND = (_env("STAIRCASE_GREEN_BAND_LOW", -85), _env("STAIRCASE_GREEN_BAND_HIGH", -75))   # All Green: 5m WPR band
GREEN30_ABOVE_COMP5_MAX = _env("STAIRCASE_GREEN30_ABOVE_COMP5_MAX", 66)   # Green-30m long above the 30m midline: 5m composite must be below this
GREEN30_BELOW_WPR_MAX = _env("STAIRCASE_GREEN30_BELOW_WPR_MAX", -75)      # Green-30m long below the 30m midline: 5m WPR must be below this
ALLORANGE_WPR = _env("STAIRCASE_ALLORANGE_WPR", -25)         # All Orange short needs the 5m WPR above this
ALLORANGE_5M_MIN = _env("STAIRCASE_ALLORANGE_5M_MIN", 70)    # All Orange needs the 5m composite at or above this
ALLGREEN_5M_MAX = _env("STAIRCASE_ALLGREEN_5M_MAX", 30)      # All Green needs the 5m composite at or below this
NEW_4H_WAIT_MIN = _env("STAIRCASE_NEW_4H_WAIT_MIN", 15)      # no trade in the first N minutes of a new 4H candle


# ---- helpers that give each file the shape it already uses ----
def comp_nested(op):
    """{(side, id): {"comp_l"/"comp_m": value}} for one operator (gt / lte / gte / lt): the tracker's shape."""
    out = {}
    for k, v in COMP.items():
        for f, x in v.items():
            if f.endswith("_" + op):
                out.setdefault(k, {})["comp_l" if f.startswith("comp5") else "comp_m"] = x
    return out


def comp_flat(tf, op, with_conds):
    """{(side, id): value} for one timeframe (5 or 30) and operator. with_conds=True keeps only setups that have SETUP_CONDS, False only the others."""
    return {k: v[f"comp{tf}_{op}"] for k, v in COMP.items() if f"comp{tf}_{op}" in v and ((k in SETUP_CONDS) == with_conds)}


def conds_by_interval():
    """SETUP_CONDS as {(side, id): ("above"/"below", ("4h", "30m", "5m") ...)}: the bot's shape."""
    names = (("mid4h_side", "4h"), ("mid30_side", "30m"), ("mid5_side", "5m"))
    return {k: (next(iter(c.values())), tuple(n for f, n in names if f in c)) for k, c in SETUP_CONDS.items()}
