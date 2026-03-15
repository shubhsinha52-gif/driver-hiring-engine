"""
🚗 Driver Hiring Engine v3.0 — Saudi + Expat Pipeline
============================================================
SFT: Real GA-marginals (profit lookup per branch per size) + staggered schedules
EFT: Auto-optimal with car constraint
Car constraint enforced in GA fitness (allows staggered >max_cars schedules)

Install:  pip install streamlit pandas numpy openpyxl
Run:      streamlit run driver_hiring_engine.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import random
import copy
import io
import time
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional

st.set_page_config(page_title="Driver Hiring Engine", page_icon="🚗", layout="wide")

DAY_NAMES = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']

# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass
class DriverShift:
    start_hour: int
    is_working: bool
    shift_length: int = 8
    def get_hours(self):
        if not self.is_working or self.start_hour < 0: return []
        return [(self.start_hour + i) % 24 for i in range(self.shift_length)]

@dataclass
class DriverSchedule:
    shifts: List[DriverShift] = field(default_factory=list)
    def get_work_days(self): return sum(1 for s in self.shifts if s.is_working)

# ============================================================
# DATA LOADING
# ============================================================

@st.cache_data(show_spinner=False)
def load_sft_data(file_bytes):
    l5 = pd.read_excel(file_bytes, sheet_name='Logics_5D', header=2)
    l5.columns = ['DRIVERS','DEMAND','T_CAP','UTIL','INTERNAL','A_CAP','PROFIT']
    logics_5d = {}
    for _, r in l5.dropna(subset=['DRIVERS']).iterrows():
        logics_5d[(int(r['DRIVERS']), int(r['DEMAND']), float(r['T_CAP']))] = {
            'profit': float(r['PROFIT']), 'util': float(r['UTIL'])}

    l6 = pd.read_excel(file_bytes, sheet_name='Logics_6D', header=2)
    l6.columns = ['DRIVERS','DEMAND','T_CAP','UTIL','INTERNAL','A_CAP','PROFIT']
    logics_6d = {}
    for _, r in l6.dropna(subset=['DRIVERS']).iterrows():
        logics_6d[(int(r['DRIVERS']), int(r['DEMAND']), float(r['T_CAP']))] = {
            'profit': float(r['PROFIT']), 'util': float(r['UTIL'])}

    cdf = pd.read_excel(file_bytes, sheet_name='Capacity', header=2)
    cdf.columns = [str(c).strip() for c in cdf.columns]
    capacity = {}
    for _, r in cdf.dropna(subset=['branch_name']).iterrows():
        capacity[(str(r['branch_name']).strip(), int(r['Weekday (shift)']))] = float(r['Adj.m'])

    sm = pd.read_excel(file_bytes, sheet_name='Staffing model_5D', header=None, skiprows=4)
    demand = {}; branch_types = {}; branches = []; prev_br = ''
    for _, row in sm.iterrows():
        br = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
        if not br or br == 'Branch': continue
        hr = int(row.iloc[1]) if pd.notna(row.iloc[1]) else 0
        for d in range(7):
            v = row.iloc[2 + d]
            demand[(br, hr, d + 1)] = int(v) if pd.notna(v) and str(v).strip() != '' else 0
        if hr == 0:
            if br != prev_br: branches.append(br); prev_br = br
            bs = str(row.iloc[70]).strip().upper() if len(row) > 70 and pd.notna(row.iloc[70]) else ''
            branch_types[br] = '6D' if bs == '6D' else '5D'
    return logics_5d, logics_6d, capacity, demand, branch_types, branches

@st.cache_data(show_spinner=False)
def load_eft_data(file_bytes):
    ldf = pd.read_excel(file_bytes, sheet_name='Logics EFT', header=0)
    ldf.columns = [c.strip() for c in ldf.columns]
    logics_eft = {}
    for _, r in ldf.dropna(subset=['DRIVERS']).iterrows():
        logics_eft[(int(r['DRIVERS']), int(r['DEMAND']), float(r['T. CAPACITY']))] = {
            'profit': float(r['PROFIT/HR. (vs. 3P)']), 'util': float(r['UTILIZATION  %'])}
    cdf = pd.read_excel(file_bytes, sheet_name='Capacity', header=2)
    cdf.columns = [c.strip() for c in cdf.columns]
    cap_eft = {}
    for _, r in cdf.dropna(subset=['branch_name']).iterrows():
        cap_eft[(str(r['branch_name']).strip(), int(r['Weekday (shift)']))] = float(r['Adj.m'])
    return logics_eft, cap_eft

@st.cache_data(show_spinner=False)
def load_cars_data(file_bytes):
    df = pd.read_excel(file_bytes)
    df.columns = ['Branch', 'Hr'] + [str(d) for d in range(1, 8)]
    cars = {}
    for br in df['Branch'].unique():
        bd = df[df['Branch'] == br]; mat = np.zeros((24, 7), int)
        for _, row in bd.iterrows():
            h = int(row['Hr'])
            if 0 <= h < 24:
                for d in range(7): mat[h, d] = int(row[str(d + 1)])
        cars[str(br).strip()] = mat
    return cars

@st.cache_data(show_spinner=False)
def load_restricted(file_bytes):
    if file_bytes is None: return set()
    return set(pd.read_excel(file_bytes).iloc[:, 0].astype(str).str.strip())

# ============================================================
# CASE MATRIX GENERATION (replaces VBA)
# ============================================================

def generate_cases(branches, demand, capacity, logics, branch_types, target_type):
    case_maps = {}
    for br in branches:
        if branch_types.get(br) != target_type: continue
        matrices = []
        for cn in range(1, 7):
            mat = np.zeros((24, 7))
            for hr in range(24):
                for day in range(1, 8):
                    dem = min(demand.get((br, hr, day), 0), 12)
                    cap = capacity.get((br, day), 2.0)
                    lk = logics.get((cn, dem, cap))
                    if lk: mat[hr, day - 1] = lk['profit']
            matrices.append(mat)
        case_maps[br] = matrices
    return case_maps

def compute_remaining(branches, demand, capacity, logics_5d, logics_6d, branch_types, saudi_cov):
    remaining = {}
    for br in branches:
        bt = branch_types.get(br, '5D'); lg = logics_5d if bt == '5D' else logics_6d
        scov = saudi_cov.get(br, np.zeros((24, 7), int))
        for hr in range(24):
            for day in range(1, 8):
                dem = demand.get((br, hr, day), 0); drivers = int(scov[hr, day - 1])
                if drivers == 0 or dem == 0: remaining[(br, hr, day)] = dem; continue
                cap = capacity.get((br, day), 2.0)
                lk = lg.get((drivers, min(dem, 12), cap), {'util': 0})
                util = min(lk['util'], 1.0); served = round(min(util * cap * drivers, dem))
                remaining[(br, hr, day)] = max(dem - served, 0)
    return remaining

# ============================================================
# SFT OPTIMIZER — Real Marginals + Staggered (SFT_Hiring_14032026)
# ============================================================

def create_random_schedule(stype):
    start = random.randint(0, 23)
    if stype == '5D':
        off1 = random.randint(0, 6); offs = {off1, (off1 + 1) % 7}
    else:
        offs = {random.randint(0, 6)}
    return DriverSchedule([DriverShift(-1, False) if d in offs
        else DriverShift((start + random.randint(-2, 2)) % 24, True) for d in range(7)])

def create_staggered_schedule(stype, driver_idx, num_drivers):
    time_bands = [[16,17,18],[19,20,21],[8,9,10],[11,12,13],[14,15],[22,23,0]]
    band = time_bands[driver_idx % len(time_bands)]
    start = random.choice(band)
    if stype == '5D':
        off1 = (driver_idx * 2) % 7; offs = {off1, (off1 + 1) % 7}
    else:
        offs = {(driver_idx * 2) % 7}
    return DriverSchedule([DriverShift(-1, False) if d in offs
        else DriverShift((start + random.randint(-1, 1)) % 24, True) for d in range(7)])

def sft_calc_profit(schedules, cases, car_mat):
    cov = np.zeros((24, 7), int)
    for sc in schedules:
        for di, sh in enumerate(sc.shifts):
            if sh.is_working:
                for h in sh.get_hours(): cov[h, di] += 1
    if car_mat is not None:
        if np.any(cov > car_mat): return -999999.0
    total = 0.0
    for h in range(24):
        for d in range(7):
            n = cov[h, d]
            if n > 0:
                ci = min(n - 1, len(cases) - 1); total += cases[ci][h, d]
    return total

def optimize_at_size(n_drv, stype, cases, car_mat, pop_size=100, gens=60):
    if n_drv == 0: return 0.0, []
    max_cars = int(car_mat.max()); constrained = n_drv > max_cars
    if constrained: pop_size = max(pop_size, 200); gens = max(gens, 100)
    pop = []
    for pi in range(pop_size):
        if constrained and pi < pop_size // 2:
            team = [create_staggered_schedule(stype, i, n_drv) for i in range(n_drv)]
        else:
            team = [create_random_schedule(stype) for _ in range(n_drv)]
        pop.append((team, sft_calc_profit(team, cases, car_mat)))
    for _ in range(gens):
        pop.sort(key=lambda x: x[1], reverse=True)
        el = max(1, int(pop_size * 0.2)); new = pop[:el]
        while len(new) < pop_size:
            par = random.choice(pop[:el])
            child = [copy.deepcopy(s) for s in par[0]]
            idx = random.randint(0, n_drv - 1)
            if random.random() < 0.3:
                if constrained and random.random() < 0.5:
                    child[idx] = create_staggered_schedule(stype, idx, n_drv)
                else:
                    child[idx] = create_random_schedule(stype)
            else:
                for i, sh in enumerate(child[idx].shifts):
                    if sh.is_working:
                        child[idx].shifts[i] = DriverShift(
                            (sh.start_hour + random.choice([-2,-1,0,1,2])) % 24, True); break
            new.append((child, sft_calc_profit(child, cases, car_mat)))
        pop = new
    best = max(pop, key=lambda x: x[1])
    return best[1], best[0]

def build_profit_lookup(branches, branch_types, cases_5d, cases_6d, cars, restricted, pop_size, gens, progress_cb=None):
    lookup = {}
    eligible = [b for b in branches if b not in restricted]
    for idx, br in enumerate(eligible):
        bt = branch_types.get(br, '5D')
        cs = cases_5d.get(br, []) if bt == '5D' else cases_6d.get(br, [])
        car_mat = cars.get(br, np.full((24, 7), 6))
        if not cs: continue
        lookup[br] = {0: (0.0, [])}
        max_size = min(6, len(cs))
        for size in range(1, max_size + 1):
            p, scheds = optimize_at_size(size, bt, cs, car_mat, pop_size, gens)
            lookup[br][size] = (p, scheds)
            if size >= 2:
                m1 = lookup[br][size][0] - lookup[br][size-1][0]
                m0 = lookup[br][size-1][0] - lookup[br][size-2][0]
                if m1 < -500 and m0 < -500:
                    for s in range(size + 1, max_size + 1):
                        lookup[br][s] = (p - 999999, [])
                    break
        if progress_cb: progress_cb(idx + 1, len(eligible), br)
    return lookup

def greedy_allocate_real(target, lookup):
    alloc = {b: 0 for b in lookup}
    for _ in range(target):
        best_br, best_m = None, -float('inf')
        for br in lookup:
            ns = alloc[br] + 1
            if ns not in lookup[br]: continue
            pn = lookup[br][ns][0]
            if pn <= -999000: continue
            m = pn - lookup[br][alloc[br]][0]
            if m > best_m: best_m = m; best_br = br
        if best_br: alloc[best_br] += 1
        else: break
    return {b: n for b, n in alloc.items() if n > 0}

def get_coverage(scheds):
    cov = np.zeros((24, 7), int)
    for sc in scheds:
        for di, sh in enumerate(sc.shifts):
            if sh.is_working:
                for h in sh.get_hours(): cov[h, di] += 1
    return cov

# ============================================================
# EFT OPTIMIZER
# ============================================================

def make_eft_schedule():
    off = random.randint(0, 6)
    return DriverSchedule([DriverShift(-1, False) if d == off
        else DriverShift(random.randint(0, 23), True, random.randint(8, 12)) for d in range(7)])

def fix_eft(s):
    wd = s.get_work_days()
    while wd < 6:
        offs = [i for i, sh in enumerate(s.shifts) if not sh.is_working]
        if not offs: break
        s.shifts[random.choice(offs)] = DriverShift(random.randint(0, 23), True, random.randint(8, 12)); wd += 1
    while wd > 6:
        wks = [i for i, sh in enumerate(s.shifts) if sh.is_working]
        if not wks: break
        s.shifts[random.choice(wks)] = DriverShift(-1, False); wd -= 1

def eft_fitness(scheds, n, cases, car_cap):
    profit = 0.0
    for day in range(7):
        for hr in range(24):
            active = sum(1 for i in range(n) if i < len(scheds) and hr in scheds[i].shifts[day].get_hours())
            if active > int(car_cap[hr, day]): return -999999.0
            if active > 0:
                ci = min(active - 1, len(cases) - 1); profit += cases[ci][hr, day]
    for i in range(n):
        if i < len(scheds) and scheds[i].get_work_days() != 6: profit -= 1000
    return profit

def optimize_eft_size(cases, car_cap, size, pop_size=100, gens=50):
    if size == 0: return 0.0, []
    pop = []
    for _ in range(pop_size):
        t = [make_eft_schedule() for _ in range(size)]
        pop.append((t, eft_fitness(t, size, cases, car_cap)))
    for _ in range(gens):
        pop.sort(key=lambda x: x[1], reverse=True)
        el = int(pop_size * 0.2); new = pop[:el]
        while len(new) < pop_size:
            par = random.choice(pop[:el])
            child = [copy.deepcopy(s) for s in par[0]]; idx = random.randint(0, size - 1)
            if random.random() < 0.35: child[idx] = make_eft_schedule()
            else:
                s = child[idx]; d = random.randint(0, 6)
                if s.shifts[d].is_working:
                    if random.random() < 0.5:
                        s.shifts[d] = DriverShift((s.shifts[d].start_hour + random.randint(-2, 2)) % 24, True, s.shifts[d].shift_length)
                    else:
                        s.shifts[d] = DriverShift(s.shifts[d].start_hour, True, max(8, min(12, s.shifts[d].shift_length + random.choice([-1, 1]))))
                if s.get_work_days() != 6: fix_eft(s)
            new.append((child, eft_fitness(child, size, cases, car_cap)))
        pop = new
    best = max(pop, key=lambda x: x[1])
    return best[1], best[0]

def auto_optimal_eft(cases, car_cap, pop_size=100, gens=50):
    best_sz, best_p, best_sc = 0, 0.0, []; prev = 0.0
    for sz in range(1, 7):
        p, sc = optimize_eft_size(cases, car_cap, sz, pop_size, gens)
        if p > prev and p > 0: best_sz = sz; best_p = p; best_sc = sc; prev = p
        else: break
    return best_sz, best_p, best_sc

# ============================================================
# ATTRIBUTION
# ============================================================

def compute_attribution(branch, scheds, cases, demand_dict, capacity, logics, bt):
    results = []; cum_p = 0.0; cum_o = 0
    for ei in range(len(scheds)):
        cov = np.zeros((24, 7), int)
        for e in range(ei + 1):
            for di, sh in enumerate(scheds[e].shifts):
                if sh.is_working:
                    for h in sh.get_hours(): cov[h, di] += 1
        pn = 0.0
        for h in range(24):
            for d in range(7):
                n = cov[h, d]
                if n > 0: ci = min(n-1, len(cases)-1); pn += cases[ci][h, d]
        on = 0
        for h in range(24):
            for day in range(1, 8):
                n = int(cov[h, day-1]); dem = demand_dict.get((branch, h, day), 0)
                if n == 0 or dem == 0: continue
                cap = capacity.get((branch, day), 2.0)
                lk = logics.get((n, min(dem, 12), cap), {'util': 0})
                util = min(lk['util'], 1.0); served = round(min(util * cap * n, dem)); on += served
        ep = pn - cum_p; eo = on - cum_o; cum_p = pn; cum_o = on
        th = 0; parts = []
        sc = scheds[ei]
        for di, sh in enumerate(sc.shifts):
            dn = DAY_NAMES[di]
            if sh.is_working:
                s = sh.start_hour; e = (s + sh.shift_length) % 24
                parts.append(f"{dn} {s:02d}:00-{e:02d}:00"); th += sh.shift_length
            else: parts.append(f"{dn} OFF")
        results.append({'Branch': branch, 'Employee': f"E{ei+1}", 'Marginal_Profit': round(ep, 2),
            'Cumulative_Profit': round(cum_p, 2), 'Marginal_Orders': eo, 'Cumulative_Orders': cum_o,
            'Weekly_Hours': th, 'Schedule': " | ".join(parts)})
    return results

# ============================================================
# EXPORT
# ============================================================

def to_excel(sft_res, eft_res=None):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        s = {'Saudi_Drivers': sft_res['total_drivers'], 'Saudi_Profit': round(sft_res['total_profit'], 2)}
        if eft_res:
            s['Expat_Drivers'] = eft_res['total_drivers']; s['Expat_Profit'] = round(eft_res['total_profit'], 2)
            s['Combined'] = sft_res['total_drivers'] + eft_res['total_drivers']
        pd.DataFrame([s]).to_excel(w, sheet_name='Summary', index=False)
        sft_res['alloc_df'].to_excel(w, sheet_name='Saudi_Allocations', index=False)
        sft_res['sched_df'].to_excel(w, sheet_name='Saudi_Schedules', index=False)
        sft_res['cov_df'].to_excel(w, sheet_name='Saudi_Coverage', index=False)
        if 'attr_df' in sft_res and not sft_res['attr_df'].empty:
            sft_res['attr_df'].to_excel(w, sheet_name='Saudi_Attribution', index=False)
        if 'marg_df' in sft_res and not sft_res['marg_df'].empty:
            sft_res['marg_df'].to_excel(w, sheet_name='Marginal_Analysis', index=False)
        if eft_res:
            eft_res['alloc_df'].to_excel(w, sheet_name='Expat_Allocations', index=False)
            eft_res['sched_df'].to_excel(w, sheet_name='Expat_Schedules', index=False)
            eft_res['cov_df'].to_excel(w, sheet_name='Expat_Coverage', index=False)
            if 'attr_df' in eft_res and not eft_res['attr_df'].empty:
                eft_res['attr_df'].to_excel(w, sheet_name='Expat_Attribution', index=False)
            if eft_res.get('viol_df') is not None and not eft_res['viol_df'].empty:
                eft_res['viol_df'].to_excel(w, sheet_name='Car_Violations', index=False)
    return buf.getvalue()

def fmt_sched(scheds, dtype="Saudi"):
    texts = []
    for i, sc in enumerate(scheds, 1):
        parts = []
        for di, sh in enumerate(sc.shifts):
            dn = DAY_NAMES[di]
            if sh.is_working:
                s = sh.start_hour; e = (s + sh.shift_length) % 24
                p = f"{dn} {s:02d}:00-{e:02d}:00"
                if dtype == "Expat": p += f"({sh.shift_length}h)"
                parts.append(p)
            else: parts.append(f"{dn} OFF")
        texts.append(f"Driver {i} ({dtype}): " + " | ".join(parts))
    return texts

# ============================================================
# MAIN APP
# ============================================================

def main():
    st.title("🚗 Driver Hiring Engine v3")
    st.caption("Saudi (real marginals + staggered) + Expat Pipeline")

    with st.sidebar:
        st.header("📁 Input Files")
        sft_file = st.file_uploader("1. SFT Developer File", type=['xlsm', 'xlsx'])
        eft_file = st.file_uploader("2. EFT Developer File", type=['xlsm', 'xlsx'])
        cars_file = st.file_uploader("3. Cars Restriction", type=['xlsx'])
        restricted_file = st.file_uploader("4. Restricted Branches (optional)", type=['xlsx'])
        st.divider(); st.header("⚙️ Settings")
        target = st.number_input("Target Saudi Drivers", 1, 500, 150)
        test_mode = st.checkbox("Test mode (10 branches)", False)
        st.divider(); st.header("🧬 GA Tuning")
        pop_size = st.slider("Population", 40, 300, 100)
        gens = st.slider("Generations", 20, 150, 60)
        show_attr = st.checkbox("Employee attribution", True)

    if not sft_file:
        st.info("👈 Upload SFT Developer File to begin")
        return

    with st.spinner("Loading SFT data..."):
        logics_5d, logics_6d, capacity, demand, branch_types, branches = load_sft_data(sft_file)
    restricted = load_restricted(restricted_file) if restricted_file else set()
    if test_mode: branches = branches[:10]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Branches", len(branches))
    c2.metric("5D", sum(1 for b in branches if branch_types.get(b) == '5D'))
    c3.metric("6D", sum(1 for b in branches if branch_types.get(b) == '6D'))
    c4.metric("Restricted", len(restricted))

    # ==================== PHASE 1: SAUDI ====================
    st.header("🟢 Phase 1: Saudi Hiring (Real Marginals)")

    if st.button("🚀 Run Saudi Optimization", type="primary", use_container_width=True):
        t0 = time.time()
        prog = st.progress(0, "Generating case matrices...")
        cases_5d = generate_cases(branches, demand, capacity, logics_5d, branch_types, '5D')
        cases_6d = generate_cases(branches, demand, capacity, logics_6d, branch_types, '6D')
        cars = load_cars_data(cars_file) if cars_file else {}
        prog.progress(5, f"Cases: {len(cases_5d)} 5D + {len(cases_6d)} 6D")

        prog.progress(6, "Building profit lookup (GA per branch per size)...")
        def pb(done, total, br):
            pct = 6 + int(74 * done / max(total, 1))
            prog.progress(pct, f"Lookup {done}/{total}: {br}")

        lookup = build_profit_lookup(branches, branch_types, cases_5d, cases_6d,
                                     cars, restricted, pop_size, gens, pb)
        prog.progress(80, f"Lookup: {len(lookup)} branches. Allocating...")

        alloc = greedy_allocate_real(target, lookup)
        prog.progress(85, f"Allocated {sum(alloc.values())} drivers")

        # Build output
        al_rows, sc_rows, cv_rows, at_rows, mg_rows = [], [], [], [], []
        saudi_cov = {}
        for br in sorted(alloc.keys()):
            n = alloc[br]; p, scheds = lookup[br][n]
            bt = branch_types.get(br, '5D')
            al_rows.append({'Branch': br, '5D/6D': bt, 'Drivers': n, 'Profit': round(p, 2)})
            for txt in fmt_sched(scheds, "Saudi"):
                sc_rows.append({'Branch': br, 'Drivers': n, 'Profit': round(p, 2), 'Schedule': txt})
            cov = get_coverage(scheds); saudi_cov[br] = cov
            for hr in range(24):
                row = {'Branch': br, '5D/6D': bt, 'Hour': hr}
                for d in range(7): row[str(d+1)] = int(cov[hr, d])
                cv_rows.append(row)
            if show_attr and scheds:
                cs = cases_5d.get(br, []) if bt == '5D' else cases_6d.get(br, [])
                lg = logics_5d if bt == '5D' else logics_6d
                if cs: at_rows.extend(compute_attribution(br, scheds, cs, demand, capacity, lg, bt))

        for br in sorted(lookup.keys()):
            for sz in sorted(lookup[br].keys()):
                if sz == 0: continue
                p = lookup[br][sz][0]; prev = lookup[br].get(sz-1, (0.0, []))[0]
                mg_rows.append({'Branch': br, 'Size': sz, 'GA_Profit': round(p, 2),
                    'Marginal': round(p - prev, 2), 'Allocated': alloc.get(br, 0) >= sz})

        sft_res = {
            'alloc_df': pd.DataFrame(al_rows).sort_values('Profit', ascending=False),
            'sched_df': pd.DataFrame(sc_rows), 'cov_df': pd.DataFrame(cv_rows),
            'attr_df': pd.DataFrame(at_rows), 'marg_df': pd.DataFrame(mg_rows),
            'total_drivers': sum(alloc.values()),
            'total_profit': sum(lookup[b][n][0] for b, n in alloc.items()),
            'saudi_cov': saudi_cov, 'alloc': alloc,
        }
        st.session_state['sft'] = sft_res
        st.session_state['cases_5d'] = cases_5d; st.session_state['cases_6d'] = cases_6d
        prog.progress(100, f"Done in {time.time()-t0:.0f}s")

    if 'sft' in st.session_state:
        sft = st.session_state['sft']
        c1, c2, c3 = st.columns(3)
        c1.metric("Saudi Drivers", sft['total_drivers'])
        c2.metric("Branches", len(sft['alloc_df']))
        c3.metric("Profit", f"{sft['total_profit']:,.2f}")

        tab1, tab2, tab3, tab4, tab5 = st.tabs(["Allocations", "Schedules", "Coverage", "Attribution", "Marginal Analysis"])
        with tab1: st.dataframe(sft['alloc_df'], use_container_width=True, height=400)
        with tab2: st.dataframe(sft['sched_df'], use_container_width=True, height=400)
        with tab3: st.dataframe(sft['cov_df'], use_container_width=True, height=400)
        with tab4:
            if not sft['attr_df'].empty: st.dataframe(sft['attr_df'], use_container_width=True, height=400)
        with tab5:
            if not sft['marg_df'].empty: st.dataframe(sft['marg_df'], use_container_width=True, height=400)

        if 'eft' not in st.session_state:
            st.download_button("📥 Download Saudi Results", data=to_excel(sft),
                file_name="Saudi_Results.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # ==================== PHASE 2: EXPAT ====================
        st.header("🔵 Phase 2: Expat Hiring")
        if not eft_file: st.warning("Upload EFT Developer File"); return
        if not cars_file: st.warning("Upload Cars Restriction"); return

        if st.button("🚀 Run Expat Optimization", type="primary", use_container_width=True):
            t0 = time.time(); p2 = st.progress(0, "Loading EFT...")
            logics_eft, cap_eft = load_eft_data(eft_file)
            cars = load_cars_data(cars_file); scov = sft['saudi_cov']
            p2.progress(5, "Computing remaining orders...")
            remaining = compute_remaining(branches, demand, capacity, logics_5d, logics_6d, branch_types, scov)
            p2.progress(15, "Generating EFT cases...")
            eft_cases = {}
            for br in branches:
                mats = []
                for cn in range(1, 7):
                    mat = np.zeros((24, 7))
                    for hr in range(24):
                        for day in range(1, 8):
                            dem = min(remaining.get((br, hr, day), 0), 12)
                            cap = cap_eft.get((br, day), 2.0)
                            lk = logics_eft.get((cn, dem, cap))
                            if lk: mat[hr, day-1] = lk['profit']
                    mats.append(mat)
                if any(m.sum() > 0 for m in mats): eft_cases[br] = mats
            p2.progress(25, f"EFT: {len(eft_cases)} branches")
            ea, es_d, ep_d = {}, {}, {}; done = 0
            for br, cs in eft_cases.items():
                tc = cars.get(br, np.full((24,7),2,int))
                sc = scov.get(br, np.zeros((24,7),int))
                ecap = np.maximum(tc - sc.astype(int), 0)
                sz, p, scheds = auto_optimal_eft(cs, ecap, pop_size, gens)
                if sz > 0: ea[br] = sz; es_d[br] = scheds; ep_d[br] = p
                done += 1; p2.progress(25 + int(65*done/max(len(eft_cases),1)), f"EFT {done}/{len(eft_cases)}: {br}")

            ea_r, es_r, ec_r, ev_r, eat_r = [], [], [], [], []
            for br in sorted(ea.keys()):
                n = ea[br]; p = ep_d[br]; bt = branch_types.get(br, '5D')
                ea_r.append({'Branch': br, '5D/6D': bt, 'EFT_Drivers': n, 'Profit': round(p, 2)})
                for txt in fmt_sched(es_d[br], "Expat"):
                    es_r.append({'Branch': br, 'EFT_Drivers': n, 'Profit': round(p, 2), 'Schedule': txt})
                tc = cars.get(br, np.full((24,7),2,int)); sc = scov.get(br, np.zeros((24,7),int))
                for hr in range(24):
                    row = {'Branch': br, 'Hour': hr}
                    for d in range(7):
                        e_a = sum(1 for s in es_d[br] if s.shifts[d].is_working and hr in s.shifts[d].get_hours())
                        sa = int(sc[hr, d]); row[f'E{d+1}'] = e_a; row[f'S{d+1}'] = sa
                        row[f'T{d+1}'] = e_a+sa; row[f'C{d+1}'] = int(tc[hr, d])
                        if e_a+sa > int(tc[hr, d]):
                            ev_r.append({'Branch': br, 'Hour': hr, 'Day': d+1, 'EFT': e_a, 'Saudi': sa, 'Total': e_a+sa, 'Cars': int(tc[hr, d])})
                    ec_r.append(row)
                if show_attr and es_d[br]:
                    eat_r.extend(compute_attribution(br, es_d[br], eft_cases[br], remaining, cap_eft, logics_eft, bt))

            eft_res = {
                'alloc_df': pd.DataFrame(ea_r).sort_values('Profit', ascending=False) if ea_r else pd.DataFrame(),
                'sched_df': pd.DataFrame(es_r), 'cov_df': pd.DataFrame(ec_r),
                'viol_df': pd.DataFrame(ev_r), 'attr_df': pd.DataFrame(eat_r),
                'total_drivers': sum(ea.values()), 'total_profit': sum(ep_d.values()),
            }
            st.session_state['eft'] = eft_res; st.session_state['remaining'] = remaining
            p2.progress(100, f"Done in {time.time()-t0:.0f}s")

        if 'eft' in st.session_state:
            eft = st.session_state['eft']
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Expat Drivers", eft['total_drivers']); c2.metric("Branches", len(eft['alloc_df']))
            c3.metric("Expat Profit", f"{eft['total_profit']:,.2f}")
            viol = len(eft.get('viol_df', pd.DataFrame()))
            c4.metric("Car Violations", viol)
            tab1, tab2, tab3, tab4 = st.tabs(["Allocations", "Schedules", "Coverage", "Attribution"])
            with tab1: st.dataframe(eft['alloc_df'], use_container_width=True, height=400)
            with tab2: st.dataframe(eft['sched_df'], use_container_width=True, height=400)
            with tab3: st.dataframe(eft['cov_df'], use_container_width=True, height=400)
            with tab4:
                if not eft['attr_df'].empty: st.dataframe(eft['attr_df'], use_container_width=True, height=400)

            st.header("📊 Combined"); sft = st.session_state['sft']
            c1, c2 = st.columns(2)
            c1.metric("Total Drivers", sft['total_drivers'] + eft['total_drivers'])
            c2.metric("Total Profit", f"{sft['total_profit'] + eft['total_profit']:,.2f}")
            st.download_button("📥 Download Complete Results", data=to_excel(sft, eft),
                file_name="Driver_Hiring_Complete.xlsx", type="primary", use_container_width=True,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

if __name__ == "__main__":
    main()
