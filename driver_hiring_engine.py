"""
🍕 Maestro Pizza — Driver & MC Hiring Engine v4.0
Age of Empires Style: 4 Campaign Stages
Stage 1: Saudi Hiring | Stage 2: Expat Hiring | Stage 3: SFT+EFT Scheduling | Stage 4: MC Scheduling
"""
import streamlit as st, pandas as pd, numpy as np, random, copy, io, time, math
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional

st.set_page_config(page_title="Maestro Pizza — Hiring Engine", page_icon="🍕", layout="wide")
DN = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat']

# ═══════════════════ THEME ═══════════════════
st.markdown("""<style>
[data-testid="stAppViewContainer"]{background:linear-gradient(180deg,#f0faf0 0%,#fff 100%)}
[data-testid="stSidebar"]{background:#1b5e20;color:#fff}
[data-testid="stSidebar"] label,.css-1cpxqw2{color:#fff!important}
.stButton>button{background:#2e7d32!important;color:#fff!important;border:none;font-weight:700;border-radius:8px}
.stButton>button:hover{background:#1b5e20!important}
div[data-testid="stMetric"]{background:#e8f5e9;border-radius:12px;padding:16px;border-left:4px solid #2e7d32}
.stage-card{background:#fff;border:2px solid #c8e6c9;border-radius:16px;padding:24px;text-align:center;
  box-shadow:0 4px 12px rgba(0,0,0,.08);transition:.3s}
.stage-card:hover{border-color:#2e7d32;box-shadow:0 6px 20px rgba(46,125,50,.2)}
.stage-locked{opacity:.5;filter:grayscale(60%)}
.stage-done{border-color:#2e7d32;background:#e8f5e9}
h1,h2,h3{color:#1b5e20!important}
</style>""", unsafe_allow_html=True)

# ═══════════════════ AUTH ═══════════════════
USERS = {"admin":"DailyFood@2026","subham":"delivery123"}
def check_login():
    if st.session_state.get('auth'): return True
    st.markdown("<h1 style='text-align:center'>🍕 Maestro Pizza</h1><h3 style='text-align:center;color:#666!important'>Driver & MC Hiring Engine</h3>",unsafe_allow_html=True)
    c1,c2,c3=st.columns([1,2,1])
    with c2:
        u=st.text_input("Username"); p=st.text_input("Password",type="password")
        if st.button("🔓 Login",use_container_width=True):
            if u in USERS and USERS[u]==p: st.session_state['auth']=True; st.session_state['user']=u; st.rerun()
            else: st.error("Invalid credentials")
    return False

# ═══════════════════ DATA STRUCTURES ═══════════════════
@dataclass
class Shift:
    start_hour:int; is_working:bool; length:int=8
    def hrs(self):
        if not self.is_working or self.start_hour<0: return []
        return [(self.start_hour+i)%24 for i in range(self.length)]
    def hr_set(self): return set(self.hrs())

@dataclass
class Schedule:
    shifts:List[Shift]=field(default_factory=list)
    def wdays(self): return sum(1 for s in self.shifts if s.is_working)
    def total_hrs(self): return sum(s.length for s in self.shifts if s.is_working)

@dataclass
class MCSchedule:
    mc_id:int; shifts:List[Shift]=field(default_factory=list)
    def wdays(self): return sum(1 for s in self.shifts if s.is_working)
    def total_hrs(self): return sum(s.length for s in self.shifts if s.is_working)
    def valid(self): return 6<=self.wdays()<=7

@dataclass
class MCTeam:
    n:int; schedules:List[MCSchedule]=field(default_factory=list)
    def active_at(self,day,hr):
        return [i for i,s in enumerate(self.schedules) if i<self.n and s.shifts[day].is_working and hr in s.shifts[day].hr_set()]

# ═══════════════════ DATA LOADING ═══════════════════
@st.cache_data(show_spinner=False)
def load_sft(fb):
    l5=pd.read_excel(fb,sheet_name='Logics_5D',header=2); l5.columns=['D','DM','TC','U','I','AC','P']
    lg5={};
    for _,r in l5.dropna(subset=['D']).iterrows(): lg5[(int(r['D']),int(r['DM']),float(r['TC']))]=({'p':float(r['P']),'u':float(r['U'])})
    l6=pd.read_excel(fb,sheet_name='Logics_6D',header=2); l6.columns=['D','DM','TC','U','I','AC','P']
    lg6={};
    for _,r in l6.dropna(subset=['D']).iterrows(): lg6[(int(r['D']),int(r['DM']),float(r['TC']))]=({'p':float(r['P']),'u':float(r['U'])})
    cd=pd.read_excel(fb,sheet_name='Capacity',header=2); cd.columns=[str(c).strip() for c in cd.columns]
    cap={};
    for _,r in cd.dropna(subset=['branch_name']).iterrows(): cap[(str(r['branch_name']).strip(),int(r['Weekday (shift)']))]=float(r['Adj.m'])
    sm=pd.read_excel(fb,sheet_name='Staffing model_5D',header=None,skiprows=4)
    dem={}; bt={}; brs=[]; pb=''
    for _,row in sm.iterrows():
        b=str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''; 
        if not b or b=='Branch': continue
        h=int(row.iloc[1]) if pd.notna(row.iloc[1]) else 0
        for d in range(7): v=row.iloc[2+d]; dem[(b,h,d+1)]=int(v) if pd.notna(v) and str(v).strip()!='' else 0
        if h==0:
            if b!=pb: brs.append(b); pb=b
            bs=str(row.iloc[70]).strip().upper() if len(row)>70 and pd.notna(row.iloc[70]) else ''
            bt[b]='6D' if bs=='6D' else '5D'
    return lg5,lg6,cap,dem,bt,brs

@st.cache_data(show_spinner=False)
def load_eft(fb):
    ld=pd.read_excel(fb,sheet_name='Logics EFT',header=0); ld.columns=[c.strip() for c in ld.columns]
    le={};
    for _,r in ld.dropna(subset=['DRIVERS']).iterrows(): le[(int(r['DRIVERS']),int(r['DEMAND']),float(r['T. CAPACITY']))]=({'p':float(r['PROFIT/HR. (vs. 3P)']),'u':float(r['UTILIZATION  %'])})
    cd=pd.read_excel(fb,sheet_name='Capacity',header=2); cd.columns=[c.strip() for c in cd.columns]
    ce={};
    for _,r in cd.dropna(subset=['branch_name']).iterrows(): ce[(str(r['branch_name']).strip(),int(r['Weekday (shift)']))]=float(r['Adj.m'])
    return le,ce

@st.cache_data(show_spinner=False)
def load_cars(fb):
    df=pd.read_excel(fb); df.columns=['Branch','Hr']+[str(d) for d in range(1,8)]
    c={};
    for b in df['Branch'].unique():
        bd=df[df['Branch']==b]; m=np.zeros((24,7),int)
        for _,r in bd.iterrows():
            h=int(r['Hr'])
            if 0<=h<24:
                for d in range(7): m[h,d]=int(r[str(d+1)])
        c[str(b).strip()]=m
    return c

@st.cache_data(show_spinner=False)
def load_restr(fb):
    if fb is None: return set()
    return set(pd.read_excel(fb).iloc[:,0].astype(str).str.strip())

# ═══════════════════ CASE GENERATION ═══════════════════
def gen_cases(brs,dem,cap,lg,bt,tt):
    cm={}
    for b in brs:
        if bt.get(b)!=tt: continue
        ms=[]
        for cn in range(1,7):
            m=np.zeros((24,7))
            for h in range(24):
                for d in range(1,8):
                    dm=min(dem.get((b,h,d),0),12); cp=cap.get((b,d),2.0)
                    lk=lg.get((cn,dm,cp))
                    if lk: m[h,d-1]=lk['p']
            ms.append(m)
        cm[b]=ms
    return cm

def gen_mc_cases(brs,rem,cap,lg,bt):
    """Generate MC Case1-3 from remaining orders using same logics"""
    cm={}
    for b in brs:
        ms=[]
        for cn in range(1,4): # MC uses Case1-3
            m=np.zeros((24,7))
            for h in range(24):
                for d in range(1,8):
                    dm=min(rem.get((b,h,d),0),12); cp=cap.get((b,d),2.0)
                    lk=lg.get((cn,dm,cp))
                    if lk:
                        # MC cases compute orders served (using util), not profit
                        u=min(lk['u'],1.0)
                        served=round(min(u*cp*cn,dm))
                        m[h,d-1]=served
            ms.append(m)
        if any(m.sum()>0 for m in ms): cm[b]=ms
    return cm

def comp_remaining(brs,dem,cap,lg5,lg6,bt,scov):
    rem={}
    for b in brs:
        t=bt.get(b,'5D'); lg=lg5 if t=='5D' else lg6
        sc=scov.get(b,np.zeros((24,7),int))
        for h in range(24):
            for d in range(1,8):
                dm=dem.get((b,h,d),0); dr=int(sc[h,d-1])
                if dr==0 or dm==0: rem[(b,h,d)]=dm; continue
                cp=cap.get((b,d),2.0); lk=lg.get((dr,min(dm,12),cp),{'u':0})
                u=min(lk['u'],1.0); sv=round(min(u*cp*dr,dm)); rem[(b,h,d)]=max(dm-sv,0)
    return rem

# ═══════════════════ SFT OPTIMIZER ═══════════════════
def mk_sched(st_):
    s=random.randint(0,23)
    if st_=='5D': o1=random.randint(0,6); os={o1,(o1+1)%7}
    else: os={random.randint(0,6)}
    return Schedule([Shift(-1,False) if d in os else Shift((s+random.randint(-2,2))%24,True) for d in range(7)])

def mk_stag(st_,di,n):
    tb=[[16,17,18],[19,20,21],[8,9,10],[11,12,13],[14,15],[22,23,0]]
    b=tb[di%len(tb)]; s=random.choice(b)
    if st_=='5D': o1=(di*2)%7; os={o1,(o1+1)%7}
    else: os={(di*2)%7}
    return Schedule([Shift(-1,False) if d in os else Shift((s+random.randint(-1,1))%24,True) for d in range(7)])

def sft_profit(scs,cas,cm):
    cv=np.zeros((24,7),int)
    for sc in scs:
        for di,sh in enumerate(sc.shifts):
            if sh.is_working:
                for h in sh.hrs(): cv[h,di]+=1
    if cm is not None and np.any(cv>cm): return -999999.0
    t=0.0
    for h in range(24):
        for d in range(7):
            n=cv[h,d]
            if n>0: ci=min(n-1,len(cas)-1); t+=cas[ci][h,d]
    return t

def opt_size(nd,st_,cas,cm,ps=100,gs=60):
    if nd==0: return 0.0,[]
    mc=int(cm.max()); co=nd>mc
    if co: ps=max(ps,200); gs=max(gs,100)
    pop=[]
    for pi in range(ps):
        if co and pi<ps//2: tm=[mk_stag(st_,i,nd) for i in range(nd)]
        else: tm=[mk_sched(st_) for _ in range(nd)]
        pop.append((tm,sft_profit(tm,cas,cm)))
    for _ in range(gs):
        pop.sort(key=lambda x:x[1],reverse=True); el=max(1,int(ps*0.2)); nw=pop[:el]
        while len(nw)<ps:
            pr=random.choice(pop[:el]); ch=[copy.deepcopy(s) for s in pr[0]]; ix=random.randint(0,nd-1)
            if random.random()<0.3:
                if co and random.random()<0.5: ch[ix]=mk_stag(st_,ix,nd)
                else: ch[ix]=mk_sched(st_)
            else:
                for i,sh in enumerate(ch[ix].shifts):
                    if sh.is_working: ch[ix].shifts[i]=Shift((sh.start_hour+random.choice([-2,-1,0,1,2]))%24,True); break
            nw.append((ch,sft_profit(ch,cas,cm)))
        pop=nw
    best=max(pop,key=lambda x:x[1]); return best[1],best[0]

def build_lookup(brs,bt,c5,c6,cars,restr,ps,gs,cb=None):
    lk={}; el=[b for b in brs if b not in restr]
    for ix,b in enumerate(el):
        t=bt.get(b,'5D'); cs=c5.get(b,[]) if t=='5D' else c6.get(b,[])
        cm=cars.get(b,np.full((24,7),6))
        if not cs: continue
        lk[b]={0:(0.0,[])}
        for sz in range(1,min(7,len(cs)+1)):
            p,sc=opt_size(sz,t,cs,cm,ps,gs); lk[b][sz]=(p,sc)
            if sz>=2:
                m1=lk[b][sz][0]-lk[b][sz-1][0]; m0=lk[b][sz-1][0]-lk[b][sz-2][0]
                if m1<-500 and m0<-500:
                    for s in range(sz+1,min(7,len(cs)+1)): lk[b][s]=(p-999999,[])
                    break
        if cb: cb(ix+1,len(el),b)
    return lk

def greedy_real(tgt,lk):
    al={b:0 for b in lk}
    for _ in range(tgt):
        bb,bm=None,-float('inf')
        for b in lk:
            ns=al[b]+1
            if ns not in lk[b]: continue
            pn=lk[b][ns][0]
            if pn<=-999000: continue
            m=pn-lk[b][al[b]][0]
            if m>bm: bm=m; bb=b
        if bb: al[bb]+=1
        else: break
    return {b:n for b,n in al.items() if n>0}

def get_cov(scs):
    cv=np.zeros((24,7),int)
    for sc in scs:
        for di,sh in enumerate(sc.shifts):
            if sh.is_working:
                for h in sh.hrs(): cv[h,di]+=1
    return cv

# ═══════════════════ EFT OPTIMIZER ═══════════════════
def mk_eft():
    o=random.randint(0,6)
    return Schedule([Shift(-1,False) if d==o else Shift(random.randint(0,23),True,random.randint(8,12)) for d in range(7)])

def fix_eft(s):
    w=s.wdays()
    while w<6:
        os=[i for i,sh in enumerate(s.shifts) if not sh.is_working]
        if not os: break
        s.shifts[random.choice(os)]=Shift(random.randint(0,23),True,random.randint(8,12)); w+=1
    while w>6:
        ws=[i for i,sh in enumerate(s.shifts) if sh.is_working]
        if not ws: break
        s.shifts[random.choice(ws)]=Shift(-1,False); w-=1

def eft_fit(scs,n,cas,cc):
    p=0.0
    for dy in range(7):
        for hr in range(24):
            a=sum(1 for i in range(n) if i<len(scs) and hr in scs[i].shifts[dy].hrs())
            if a>int(cc[hr,dy]): return -999999.0
            if a>0: ci=min(a-1,len(cas)-1); p+=cas[ci][hr,dy]
    for i in range(n):
        if i<len(scs) and scs[i].wdays()!=6: p-=1000
    return p

def opt_eft(cas,cc,sz,ps=100,gs=50):
    if sz==0: return 0.0,[]
    pop=[]
    for _ in range(ps):
        t=[mk_eft() for _ in range(sz)]; pop.append((t,eft_fit(t,sz,cas,cc)))
    for _ in range(gs):
        pop.sort(key=lambda x:x[1],reverse=True); el=int(ps*0.2); nw=pop[:el]
        while len(nw)<ps:
            pr=random.choice(pop[:el]); ch=[copy.deepcopy(s) for s in pr[0]]; ix=random.randint(0,sz-1)
            if random.random()<0.35: ch[ix]=mk_eft()
            else:
                s=ch[ix]; d=random.randint(0,6)
                if s.shifts[d].is_working:
                    if random.random()<0.5: s.shifts[d]=Shift((s.shifts[d].start_hour+random.randint(-2,2))%24,True,s.shifts[d].length)
                    else: s.shifts[d]=Shift(s.shifts[d].start_hour,True,max(8,min(12,s.shifts[d].length+random.choice([-1,1]))))
                if s.wdays()!=6: fix_eft(s)
            nw.append((ch,eft_fit(ch,sz,cas,cc)))
        pop=nw
    return max(pop,key=lambda x:x[1])

def auto_eft(cas,cc,ps=100,gs=50):
    bs,bp,bsc=0,0.0,[]; pv=0.0
    for sz in range(1,7):
        p,sc=opt_eft(cas,cc,sz,ps,gs)
        if p>pv and p>0: bs=sz; bp=p; bsc=sc; pv=p
        else: break
    return bs,bp,bsc

# ═══════════════════ MC OPTIMIZER ═══════════════════
def mk_mc(mid):
    sh=[Shift(random.randint(0,23),True,random.randint(8,12)) if random.random()>0.12 else Shift(-1,False) for _ in range(7)]
    sc=MCSchedule(mid,sh); w=sc.wdays()
    while w<6:
        os=[i for i,s in enumerate(sc.shifts) if not s.is_working]
        if not os: break
        sc.shifts[random.choice(os)]=Shift(random.randint(0,23),True,random.randint(8,12)); w+=1
    while w>7:
        ws=[i for i,s in enumerate(sc.shifts) if s.is_working]
        if not ws: break
        sc.shifts[random.choice(ws)]=Shift(-1,False); w-=1
    return sc

def mc_team_orders(cas,team):
    t=0.0; nc=len(cas)
    for dy in range(7):
        for hr in range(24):
            na=len(team.active_at(dy,hr))
            if na>0: ci=min(na-1,nc-1); t+=cas[ci][hr,dy]
    return t

def mc_indiv_orders(cas,team,mi):
    t=0.0
    for dy in range(7):
        sh=team.schedules[mi].shifts[dy]
        if not sh.is_working: continue
        for hr in sh.hr_set():
            if hr>=24: continue
            act=team.active_at(dy,hr); na=len(act)
            if na==1: t+=cas[0][hr,dy]
            elif na==2:
                v=cas[min(1,len(cas)-1)][hr,dy]; rk=act.index(mi) if mi in act else 0
                t+=math.ceil(v/2) if rk==0 else math.floor(v/2)
            elif na>=3:
                v=cas[min(2,len(cas)-1)][hr,dy]; rk=act.index(mi) if mi in act else 0
                base=math.floor(v/3); rem=v-(base*3)
                t+=base+1 if rk<rem else base
    return t

def mc_fitness(cas,team,thr):
    if team.n==0: return 0.0
    pen=0.0
    for mi in range(team.n):
        if mi>=len(team.schedules): pen+=10000; continue
        sc=team.schedules[mi]
        if not sc.valid(): pen+=5000; continue
        hrs=sc.total_hrs(); ords=mc_indiv_orders(cas,team,mi)
        if ords<int(hrs*thr): pen+=10000
    if pen>0: return -pen
    return mc_team_orders(cas,team)

def opt_mc_branch(cas,max_mc,thr,ps=200,gs=100):
    pop=[]
    for _ in range(ps):
        n=random.randint(0,max_mc); scs=[mk_mc(i) for i in range(n)]
        tm=MCTeam(n,scs); pop.append((tm,mc_fitness(cas,tm,thr)))
    for _ in range(gs):
        pop.sort(key=lambda x:x[1],reverse=True); el=int(ps*0.15); nw=pop[:el]
        while len(nw)<ps:
            pr=random.choice(pop[:el]); ch=MCTeam(pr[0].n,[copy.deepcopy(s) for s in pr[0].schedules])
            r=random.random()
            if r<0.2: # change team size
                ch.n=random.randint(0,max_mc)
                while len(ch.schedules)<ch.n: ch.schedules.append(mk_mc(len(ch.schedules)))
                ch.schedules=ch.schedules[:ch.n]
            elif r<0.5 and ch.n>0: # replace one MC
                ix=random.randint(0,ch.n-1); ch.schedules[ix]=mk_mc(ix)
            elif ch.n>0: # tweak shift
                ix=random.randint(0,ch.n-1); dy=random.randint(0,6)
                sh=ch.schedules[ix].shifts[dy]
                if sh.is_working:
                    ch.schedules[ix].shifts[dy]=Shift((sh.start_hour+random.randint(-2,2))%24,True,max(8,min(12,sh.length+random.choice([-1,0,1]))))
            nw.append((ch,mc_fitness(cas,ch,thr)))
        pop=nw
    return max(pop,key=lambda x:x[1])

# ═══════════════════ ATTRIBUTION ═══════════════════
def attrib(br,scs,cas,dem,cap,lg,bt_):
    res=[]; cp=0.0; co=0
    for ei in range(len(scs)):
        cv=np.zeros((24,7),int)
        for e in range(ei+1):
            for di,sh in enumerate(scs[e].shifts):
                if sh.is_working:
                    for h in sh.hrs(): cv[h,di]+=1
        pn=0.0
        for h in range(24):
            for d in range(7):
                n=cv[h,d]
                if n>0: ci=min(n-1,len(cas)-1); pn+=cas[ci][h,d]
        on=0
        for h in range(24):
            for dy in range(1,8):
                n=int(cv[h,dy-1]); dm=dem.get((br,h,dy),0)
                if n==0 or dm==0: continue
                c=cap.get((br,dy),2.0); lk=lg.get((n,min(dm,12),c),{'u':0})
                u=min(lk['u'],1.0); sv=round(min(u*c*n,dm)); on+=sv
        ep=pn-cp; eo=on-co; cp=pn; co=on; th=0; pts=[]
        sc=scs[ei]
        for di,sh in enumerate(sc.shifts):
            dn=DN[di]
            if sh.is_working: s=sh.start_hour; e=(s+sh.length)%24; pts.append(f"{dn} {s:02d}:00-{e:02d}:00"); th+=sh.length
            else: pts.append(f"{dn} OFF")
        res.append({'Branch':br,'Employee':f"E{ei+1}",'Marginal_Profit':round(ep,2),'Cum_Profit':round(cp,2),
            'Marginal_Orders':eo,'Cum_Orders':co,'Hours':th,'Productivity':round(eo/th,2) if th>0 else 0,'Schedule':" | ".join(pts)})
    return res

# ═══════════════════ EXPORT ═══════════════════
def to_xl(data):
    buf=io.BytesIO()
    with pd.ExcelWriter(buf,engine='openpyxl') as w:
        for name,df in data.items():
            if df is not None and not df.empty: df.to_excel(w,sheet_name=name[:31],index=False)
    return buf.getvalue()

def fmt_sc(scs,dt="Saudi"):
    tx=[]
    for i,sc in enumerate(scs,1):
        pts=[]
        for di,sh in enumerate(sc.shifts):
            dn=DN[di]
            if sh.is_working:
                s=sh.start_hour; e=(s+sh.length)%24; p=f"{dn} {s:02d}:00-{e:02d}:00"
                if dt=="Expat": p+=f"({sh.length}h)"
                pts.append(p)
            else: pts.append(f"{dn} OFF")
        tx.append(f"Driver {i} ({dt}): "+" | ".join(pts))
    return tx

# ═══════════════════ MAIN APP ═══════════════════
def main():
    if not check_login(): return

    st.markdown("<h1 style='text-align:center'>🍕 Maestro Pizza — Hiring Engine</h1>",unsafe_allow_html=True)
    st.markdown(f"<p style='text-align:center;color:#666'>Logged in as: <b>{st.session_state.get('user','')}</b></p>",unsafe_allow_html=True)

    # Stage status
    s1=st.session_state.get('s1_done',False); s2=st.session_state.get('s2_done',False)
    s3=st.session_state.get('s3_done',False); s4=st.session_state.get('s4_done',False)

    # Campaign map
    st.markdown("---")
    c1,c2,c3,c4=st.columns(4)
    for c,num,name,icon,done,unlocked in [
        (c1,1,"Saudi Hiring","🍕",s1,True),(c2,2,"Expat Hiring","🍔",s2,s1),
        (c3,3,"SFT+EFT Scheduling","🥪",s3,s2),(c4,4,"MC Scheduling","🛵",s4,s3)]:
        cls="stage-done" if done else ("stage-card" if unlocked else "stage-card stage-locked")
        status="✅ Complete" if done else ("🔓 Ready" if unlocked else "🔒 Locked")
        c.markdown(f"<div class='{cls}'><h2>{icon}</h2><h3>Stage {num}</h3><p>{name}</p><small>{status}</small></div>",unsafe_allow_html=True)
    st.markdown("---")

    # Sidebar
    with st.sidebar:
        st.markdown("### 📁 Files")
        sft_f=st.file_uploader("SFT Developer",type=['xlsm','xlsx'])
        eft_f=st.file_uploader("EFT Developer",type=['xlsm','xlsx'])
        cars_f=st.file_uploader("Cars Restriction",type=['xlsx'])
        restr_f=st.file_uploader("Restricted Branches",type=['xlsx'])
        mc_restr_f=st.file_uploader("MC Restriction",type=['xlsx'])
        st.markdown("---"); st.markdown("### ⚙️ Settings")
        tgt=st.number_input("Target Saudi",1,500,150)
        test=st.checkbox("Test (10 branches)")
        ps=st.slider("GA Pop",40,300,100); gs=st.slider("GA Gens",20,150,60)

    # Load data
    if not sft_f: st.info("👈 Upload SFT Developer File to begin the campaign"); return
    with st.spinner("Loading..."): lg5,lg6,cap,dem,bt,brs=load_sft(sft_f)
    restr=load_restr(restr_f) if restr_f else set()
    if test: brs=brs[:10]

    # ═══ STAGE 1: Saudi Hiring ═══
    stage=st.radio("Select Stage",["🍕 Stage 1: Saudi Hiring","🍔 Stage 2: Expat Hiring","🥪 Stage 3: SFT+EFT Scheduling","🛵 Stage 4: MC Scheduling"],horizontal=True)

    if "Stage 1" in stage:
        st.header("🍕 Stage 1: Saudi Hiring (Real Marginals)")
        c1,c2,c3=st.columns(3); c1.metric("Branches",len(brs)); c2.metric("5D",sum(1 for b in brs if bt.get(b)=='5D')); c3.metric("6D",sum(1 for b in brs if bt.get(b)=='6D'))

        if st.button("🚀 Launch Saudi Campaign",type="primary",use_container_width=True):
            t0=time.time(); pg=st.progress(0,"Generating cases...")
            c5=gen_cases(brs,dem,cap,lg5,bt,'5D'); c6=gen_cases(brs,dem,cap,lg6,bt,'6D')
            cars=load_cars(cars_f) if cars_f else {}
            pg.progress(5,"Building profit lookup...")
            def cb(d,t,b): pg.progress(5+int(75*d/max(t,1)),f"Lookup {d}/{t}: {b}")
            lk=build_lookup(brs,bt,c5,c6,cars,restr,ps,gs,cb)
            pg.progress(82,"Allocating..."); al=greedy_real(tgt,lk)
            ar,sr,cr,atr,mr=[],[],[],[],[]
            scov={}
            for b in sorted(al):
                n=al[b]; p,scs=lk[b][n]; t_=bt.get(b,'5D')
                ar.append({'Branch':b,'5D/6D':t_,'Drivers':n,'Profit':round(p,2)})
                for tx in fmt_sc(scs,"Saudi"): sr.append({'Branch':b,'Schedule':tx})
                cv=get_cov(scs); scov[b]=cv
                for h in range(24):
                    row={'Branch':b,'Hour':h}
                    for d in range(7): row[str(d+1)]=int(cv[h,d])
                    cr.append(row)
                cs=c5.get(b,[]) if t_=='5D' else c6.get(b,[])
                lg=lg5 if t_=='5D' else lg6
                if cs and scs: atr.extend(attrib(b,scs,cs,dem,cap,lg,t_))
            for b in sorted(lk):
                for sz in sorted(lk[b]):
                    if sz==0: continue
                    p=lk[b][sz][0]; pv=lk[b].get(sz-1,(0.0,[]))[0]
                    mr.append({'Branch':b,'Size':sz,'Profit':round(p,2),'Marginal':round(p-pv,2),'Used':al.get(b,0)>=sz})
            st.session_state['sft']={'al':pd.DataFrame(ar).sort_values('Profit',ascending=False),'sc':pd.DataFrame(sr),
                'cv':pd.DataFrame(cr),'at':pd.DataFrame(atr),'mg':pd.DataFrame(mr),
                'td':sum(al.values()),'tp':sum(lk[b][n][0] for b,n in al.items()),'scov':scov,'al_d':al}
            st.session_state['c5']=c5; st.session_state['c6']=c6; st.session_state['s1_done']=True
            pg.progress(100,f"Done in {time.time()-t0:.0f}s")

        if 'sft' in st.session_state:
            s=st.session_state['sft']; c1,c2,c3=st.columns(3)
            c1.metric("Saudi Drivers",s['td']); c2.metric("Branches",len(s['al'])); c3.metric("Profit",f"{s['tp']:,.2f}")
            tabs=st.tabs(["Allocations","Schedules","Coverage","Attribution","Marginals"])
            with tabs[0]: st.dataframe(s['al'],use_container_width=True,height=400)
            with tabs[1]: st.dataframe(s['sc'],use_container_width=True,height=400)
            with tabs[2]: st.dataframe(s['cv'],use_container_width=True,height=400)
            with tabs[3]:
                if not s['at'].empty: st.dataframe(s['at'],use_container_width=True,height=400)
            with tabs[4]:
                if not s['mg'].empty: st.dataframe(s['mg'],use_container_width=True,height=400)
            st.download_button("📥 Download Saudi Results",to_xl({'Summary':pd.DataFrame([{'Drivers':s['td'],'Profit':round(s['tp'],2)}]),'Allocations':s['al'],'Schedules':s['sc'],'Coverage':s['cv'],'Attribution':s['at'],'Marginals':s['mg']}),file_name="Saudi_Results.xlsx",mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    elif "Stage 2" in stage:
        st.header("🍔 Stage 2: Expat Hiring")
        if not s1: st.warning("🔒 Complete Stage 1 first"); return
        if not eft_f: st.warning("Upload EFT Developer File"); return
        if not cars_f: st.warning("Upload Cars Restriction"); return

        if st.button("🚀 Launch Expat Campaign",type="primary",use_container_width=True):
            t0=time.time(); pg=st.progress(0,"Loading EFT...")
            le,ce=load_eft(eft_f); cars=load_cars(cars_f); scov=st.session_state['sft']['scov']
            pg.progress(5,"Computing remaining orders...")
            rem=comp_remaining(brs,dem,cap,lg5,lg6,bt,scov)
            pg.progress(15,"EFT cases...")
            ec={}
            for b in brs:
                ms=[]
                for cn in range(1,7):
                    m=np.zeros((24,7))
                    for h in range(24):
                        for d in range(1,8):
                            dm=min(rem.get((b,h,d),0),12); cp=ce.get((b,d),2.0)
                            lk=le.get((cn,dm,cp))
                            if lk: m[h,d-1]=lk['p']
                    ms.append(m)
                if any(m.sum()>0 for m in ms): ec[b]=ms
            pg.progress(25,f"EFT: {len(ec)} branches")
            ea,esd,epd={},{},{}; dn_=0
            for b,cs in ec.items():
                tc=cars.get(b,np.full((24,7),2,int)); sc=scov.get(b,np.zeros((24,7),int))
                ecp=np.maximum(tc-sc.astype(int),0)
                sz,p,scs=auto_eft(cs,ecp,ps,gs)
                if sz>0: ea[b]=sz; esd[b]=scs; epd[b]=p
                dn_+=1; pg.progress(25+int(65*dn_/max(len(ec),1)),f"EFT {dn_}/{len(ec)}: {b}")
            ear,esr,ecr,evr,eatr=[],[],[],[],[]
            for b in sorted(ea):
                n=ea[b]; p=epd[b]; t_=bt.get(b,'5D')
                ear.append({'Branch':b,'EFT':n,'Profit':round(p,2)})
                for tx in fmt_sc(esd[b],"Expat"): esr.append({'Branch':b,'Schedule':tx})
                tc=cars.get(b,np.full((24,7),2,int)); sc=scov.get(b,np.zeros((24,7),int))
                for h in range(24):
                    row={'Branch':b,'Hour':h}
                    for d in range(7):
                        ea_=sum(1 for s in esd[b] if s.shifts[d].is_working and h in s.shifts[d].hrs())
                        sa=int(sc[h,d]); row[f'E{d+1}']=ea_; row[f'S{d+1}']=sa; row[f'T{d+1}']=ea_+sa; row[f'C{d+1}']=int(tc[h,d])
                        if ea_+sa>int(tc[h,d]): evr.append({'Branch':b,'Hour':h,'Day':d+1,'Excess':ea_+sa-int(tc[h,d])})
                    ecr.append(row)
            st.session_state['eft']={'al':pd.DataFrame(ear).sort_values('Profit',ascending=False),'sc':pd.DataFrame(esr),
                'cv':pd.DataFrame(ecr),'vl':pd.DataFrame(evr),'td':sum(ea.values()),'tp':sum(epd.values())}
            st.session_state['rem']=rem; st.session_state['s2_done']=True
            pg.progress(100,f"Done in {time.time()-t0:.0f}s")

        if 'eft' in st.session_state:
            e=st.session_state['eft']; c1,c2,c3=st.columns(3)
            c1.metric("Expat Drivers",e['td']); c2.metric("Branches",len(e['al'])); c3.metric("Profit",f"{e['tp']:,.2f}")
            tabs=st.tabs(["Allocations","Schedules","Coverage"])
            with tabs[0]: st.dataframe(e['al'],use_container_width=True,height=400)
            with tabs[1]: st.dataframe(e['sc'],use_container_width=True,height=400)
            with tabs[2]: st.dataframe(e['cv'],use_container_width=True,height=400)

    elif "Stage 3" in stage:
        st.header("🥪 Stage 3: SFT+EFT Combined Scheduling")
        if not s2: st.warning("🔒 Complete Stage 2 first"); return
        st.info("Stage 3 uses the SFT+EFT Scheduling code (Employee_restriction_.xlsx based). Upload the scheduling output or run the standalone SFT_EFT code, then use the Employee Attribution VBA for profit/order/hours/productivity per employee.")
        st.markdown("**Employee Attribution VBA** has been provided separately for the `Th_Saudis_(8h_5d).xlsm` file.")
        st.session_state['s3_done']=True

    elif "Stage 4" in stage:
        st.header("🛵 Stage 4: MC Scheduling")
        if not s3: st.warning("🔒 Complete Stage 3 first"); return

        if st.button("🚀 Launch MC Campaign",type="primary",use_container_width=True):
            t0=time.time(); pg=st.progress(0,"Generating MC cases...")
            rem=st.session_state.get('rem',{})
            if not rem: st.error("No remaining orders from Stage 2"); return

            # MC cases from remaining orders
            mc_cas=gen_mc_cases(brs,rem,cap,lg5,bt)
            pg.progress(10,f"MC cases: {len(mc_cas)} branches")

            # Load MC restrictions
            mc_restr={}
            if mc_restr_f:
                df=pd.read_excel(mc_restr_f)
                for _,r in df.iterrows():
                    b=str(r.iloc[0]).strip()
                    if len(r)>1 and pd.notna(r.iloc[1]): mc_restr[b]=int(r.iloc[1])

            # Dual threshold run
            r1_yes,r1_no=[],[]
            done=0; total=len(mc_cas)
            for b,cas in mc_cas.items():
                mx=mc_restr.get(b,3)
                best=opt_mc_branch(cas,mx,0.8,ps,gs)
                if best[1]>0: r1_yes.append((b,best[0],best[1]))
                else: r1_no.append(b)
                done+=1; pg.progress(10+int(60*done/max(total,1)),f"MC 0.8: {done}/{total} {b}")

            # Run 2: retry at 0.6
            r2=[]
            if r1_no:
                pg.progress(72,f"Retrying {len(r1_no)} branches at 0.6...")
                for ix,b in enumerate(r1_no):
                    if b not in mc_cas: continue
                    cas=mc_cas[b]; mx=mc_restr.get(b,3)
                    best=opt_mc_branch(cas,mx,0.6,ps,gs)
                    if best[1]>0: r2.append((b,best[0],best[1]))
                    pg.progress(72+int(25*(ix+1)/max(len(r1_no),1)),f"MC 0.6: {ix+1}/{len(r1_no)}")

            all_mc=r1_yes+r2
            mar,msr=[],[]
            for b,team,ords in all_mc:
                mar.append({'Branch':b,'MCs':team.n,'Weekly_Orders':round(ords,1)})
                for mi in range(team.n):
                    sc=team.schedules[mi]; pts=[]
                    for di,sh in enumerate(sc.shifts):
                        dn_=DN[di]
                        if sh.is_working: s=sh.start_hour; e=(s+sh.length)%24; pts.append(f"{dn_} {s:02d}:00-{e:02d}:00({sh.length}h)")
                        else: pts.append(f"{dn_} OFF")
                    msr.append({'Branch':b,'MC':f"MC_{mi+1}",'Hours':sc.total_hrs(),'Orders':round(mc_indiv_orders(mc_cas[b],team,mi),1),'Schedule':" | ".join(pts)})

            st.session_state['mc']={'al':pd.DataFrame(mar).sort_values('Weekly_Orders',ascending=False),'sc':pd.DataFrame(msr),
                'td':sum(t.n for _,t,_ in all_mc),'to':sum(o for _,_,o in all_mc)}
            st.session_state['s4_done']=True
            pg.progress(100,f"Done in {time.time()-t0:.0f}s")

        if 'mc' in st.session_state:
            m=st.session_state['mc']; c1,c2,c3=st.columns(3)
            c1.metric("Total MCs",m['td']); c2.metric("Branches",len(m['al'])); c3.metric("Weekly Orders",f"{m['to']:,.0f}")
            tabs=st.tabs(["Allocations","Schedules"])
            with tabs[0]: st.dataframe(m['al'],use_container_width=True,height=400)
            with tabs[1]: st.dataframe(m['sc'],use_container_width=True,height=400)

    # Combined download
    if s1:
        st.markdown("---")
        st.header("📥 Download All Results")
        sheets={}
        if 'sft' in st.session_state:
            s=st.session_state['sft']
            sheets['Saudi_Alloc']=s['al']; sheets['Saudi_Sched']=s['sc']; sheets['Saudi_Cov']=s['cv']
            sheets['Saudi_Attrib']=s['at']; sheets['Saudi_Marginals']=s['mg']
        if 'eft' in st.session_state:
            e=st.session_state['eft']
            sheets['Expat_Alloc']=e['al']; sheets['Expat_Sched']=e['sc']; sheets['Expat_Cov']=e['cv']
        if 'mc' in st.session_state:
            m=st.session_state['mc']
            sheets['MC_Alloc']=m['al']; sheets['MC_Sched']=m['sc']
        if sheets:
            st.download_button("📥 Download Complete Campaign Results",to_xl(sheets),
                file_name="Maestro_Pizza_Campaign_Results.xlsx",type="primary",use_container_width=True,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

if __name__=="__main__": main()
