# CommBox Flow Analyzer — Python/Streamlit (v2.2 + Better Diff)
# -------------------------------------------------------------
# שינויים בגרסה זו:
# - שדרוג טאב Diff: תקציר ברור, Nodes Δ (שדה-לשדה), Transitions Δ (כולל child),
#   Assignments Δ (נוספו/נמחקו/שינוי ערך), Conditions Δ (תנאי (תמצית)), Drill-down צד-לצד.
# - לא נוגעים בלוגיקת הסיווג (נשאר v2.2), כך שהכלי עובד לך כמו קודם — רק ה-Diff טוב בהרבה.
#
# Run:
#   pip install streamlit pandas lxml beautifulsoup4 openpyxl xlsxwriter pyvis jinja2 bs4
#   streamlit run app.py
# -------------------------------------------------------------

import re, io, json, html, tempfile, hashlib, difflib
from typing import Any, Dict, List, Set
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup

st.set_page_config(page_title="CommBox Flow Analyzer v2.2 (Better Diff)", layout="wide")
st.title("CommBox Flow Analyzer — v2.2 (Better Diff)")
st.caption("ניתוח CommBox: נודים, תנאים, השמות, מעברים (כולל child), יתומים, סימולציה, גרף, ודלתא-דיפ משודרג.")

# ------------------------- Helpers -------------------------
OP_TXT = {"0":"שווה ל","1":"שונה מ","2":"גדול מ","3":"קטן מ","4":"גדול או שווה ל","5":"קטן או שווה ל","6":"הוא מספר","7":"ריק","8":"לא ריק"}
OP_SYM = {"0":"==","1":"!=","2":">","3":"<","4":">=","5":"<=","6":"isNumber","7":"isEmpty","8":"isNotEmpty"}
NAV_KEYS = {"step","goto","destination","next","nextNode","goToNodeId","nodeId","targetNode"}

def deep_unescape(s: str, rounds: int = 3) -> str:
    prev = s or ""
    for _ in range(rounds):
        cur = html.unescape(prev).replace("\\r\\n", "\n")
        if cur == prev:
            break
        prev = cur
    return prev

def strip_tags(html_text: str) -> str:
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    txt = soup.get_text(" ")
    return re.sub(r"\s+", " ", txt or "").strip()

def find_vars(s: str) -> List[str]:
    return sorted(set(re.findall(r"\{\{\s*(data\.[^}\s\|]+)\s*\}\}", s or "")))

def norm_node_id(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    m = re.match(r"^(n_\d+)(?:,.*)?$", s, re.I)
    if m: return m.group(1)
    m = re.match(r"^node_(\d+)$", s, re.I)
    if m: return f"n_{m.group(1)}"
    m = re.match(r"^n(?:ode)?[_-]?(\d+)$", s, re.I)
    if m: return f"n_{m.group(1)}"
    m = re.match(r"^(\d+)$", s)
    if m: return f"n_{m.group(1)}"
    return ""

def find_du_transitions(obj: Any) -> List[str]:
    found: List[str] = []
    def walk(x: Any):
        if isinstance(x, list):
            for it in x: walk(it)
        elif isinstance(x, dict):
            for k, v in x.items():
                if isinstance(v, str):
                    if k in NAV_KEYS:
                        tgt = norm_node_id(v)
                        if tgt: found.append(tgt)
                    m = re.match(r"^(n_\d+),([^\"]+)$", v.strip())
                    if m: found.append(m.group(1))
                walk(v)
    walk(obj)
    return found

def is_transfer(stream: Any) -> bool:
    obj = stream
    if isinstance(stream, str):
        try:
            obj = json.loads(deep_unescape(stream))
        except Exception:
            obj = None
    if not isinstance(obj, dict):
        return False
    has_flag = any(
        (obj.get(k) is True)
        or (str(obj.get(k)).lower() == "true")
        or (str(obj.get(k)) == "1")
        for k in ["transfer","Transfer","transferToAgent","toAgent"]
    )
    has_meta = any(k in obj for k in ["encryptedStreamId","subStreamId","moduleId"])
    return bool(has_flag or (has_meta and (str(obj.get("transfer")) in {"1","true","True"})))

def du_has_navigation(du_raw: str) -> bool:
    if not du_raw:
        return False
    try:
        du = json.loads(deep_unescape(du_raw))
    except Exception:
        return False
    def walk(x):
        if isinstance(x, list):
            return any(walk(i) for i in x)
        if isinstance(x, dict):
            for kk, vv in x.items():
                if kk in NAV_KEYS and str(vv or "").strip():
                    return True
                if isinstance(vv, (list, dict)) and walk(vv):
                    return True
        return False
    return walk(du)

# ---------------- Parsers ----------------
def parse_conditions(cond_raw: str) -> List[Dict[str, Any]]:
    if not cond_raw: return []
    try:
        j = json.loads(deep_unescape(cond_raw))
        ctx = j.get("ContextConditions", [])
        return [human_condition(c) for c in ctx]
    except Exception:
        return []

def parse_du(du_raw: str) -> Any:
    if not du_raw: return None
    try:
        return json.loads(deep_unescape(du_raw))
    except Exception:
        return None

def human_condition(c: Dict[str, Any]) -> Dict[str, Any]:
    cid = str(c.get("Id", "?"))
    must = str(c.get("MustApply", "")).lower() == "true"
    op = str(c.get("Operator", ""))
    raw_prop = (c.get("PropertyName", "") or "").strip().strip('"')
    m = re.match(r"\{\{\s*([^}]+)\s*\}\}", raw_prop)
    if m: raw_prop = m.group(1)
    base = raw_prop.split(".")[-1] if raw_prop else ""
    val = str(c.get("ConditionValue", ""))
    shown = ""
    if op not in {"6","7","8"}:
        shown = val if re.fullmatch(r"-?\d+(\.\d+)?", val) else f'"{val}"'
    if op == "6":
        sent = f"תנאי #{cid}{' [חובה]' if must else ''}: בדיקה שהשדה '{base}' ({raw_prop}) הוא מספר"
        comp = f"[#{cid}{' חובה' if must else ''}] {base} isNumber"
    elif op == "7":
        sent = f"תנאי #{cid}{' [חובה]' if must else ''}: בדיקה שהשדה '{base}' ({raw_prop}) ריק"
        comp = f"[#{cid}{' חובה' if must else ''}] {base} isEmpty"
    elif op == "8":
        sent = f"תנאי #{cid}{' [חובה]' if must else ''}: בדיקה שהשדה '{base}' ({raw_prop}) לא ריק"
        comp = f"[#{cid}{' חובה' if must else ''}] {base} isNotEmpty"
    else:
        sent = f"תנאי #{cid}{' [חובה]' if must else ''}: בודקים את '{base}' ({raw_prop}) {OP_TXT.get(op, op)} {shown}"
        comp = f"[#{cid}{' חובה' if must else ''}] {base} {OP_SYM.get(op, op)} {shown}"
    return {
        "Id": cid,
        "MustApply": must,
        "PropertyName": raw_prop,
        "Operator": op,
        "ConditionValue": val,
        "תנאי (מובן)": sent,
        "תנאי (תמצית)": comp,
    }

# ----------------------- Analyzer core (v2.2) -----------------------
def analyze_nodes(nodes: List[Dict[str, Any]]):
    nodes_rows: List[Dict[str, Any]] = []
    vars_rows:  List[Dict[str, Any]] = []
    cond_rows:  List[Dict[str, Any]] = []
    assign_rows:List[Dict[str, Any]] = []
    trans_rows: List[Dict[str, Any]] = []

    id2title: Dict[str,str] = {}
    parent2children: Dict[str, List[str]] = {}

    # map titles + parents first
    for n in nodes:
        nid = n.get("id","")
        id2title[nid] = n.get("text","")
        parent = n.get("parent","")
        if parent:
            parent2children.setdefault(parent, []).append(nid)

    for n in nodes:
        nid = n.get("id","")
        title = n.get("text","")
        body_html = n.get("bodyHtml","") or ""
        body_txt = strip_tags(body_html)
        end_flag = str(n.get("end","")) if ("end" in n) else ""
        is_terminal = end_flag == "2" or (end_flag and end_flag.lower() in {"true","1"})
        is_transfer_flag = is_transfer(n.get("stream"))

        nodes_rows.append({
            "נוד": nid,
            "כותרת נוד": title,
            "הורה": n.get("parent",""),
            "מה הלקוח רואה": body_txt,
            "end": end_flag,
            "is_transfer": "1" if is_transfer_flag else "",
        })

        # var usage (appearances of {{data.*}})
        for key in ("bodyHtml","text","step","du","condition"):
            for v in find_vars(n.get(key,"") or ""):
                vars_rows.append({
                    "נוד": nid,
                    "כותרת נוד": title,
                    "ContextKey (שימוש)": v,
                    "נלקח מ": key,
                })

        # conditions
        conds = parse_conditions(n.get("condition","") or "")
        for c in conds:
            row = {"נוד": nid, "כותרת נוד": title}
            row.update(c)
            cond_rows.append(row)

        # assignments (du)
        du_json = parse_du(n.get("du","") or "")
        if isinstance(du_json, list):
            for item in du_json:
                if isinstance(item, dict) and str(item.get("action","")).lower() == "set":
                    p = item.get("params", {})
                    ctx = p.get("ContextKey") or p.get("DestinationContextKey") or p.get("TargetContextKey") or ""
                    rv = p.get("ReturnValue") or p.get("functionBody") or p.get("code") or p.get("js") or ""
                    stype = p.get("SetType") or p.get("type") or ""
                    otype = p.get("ObjectType", "")
                    used_vars = ", ".join(find_vars(str(rv)))

                    # API enrichment (פענוח resp={{data.xxx}} ואז resp.some.path..)
                    api_src = api_path = api_field = human = ""
                    m = re.search(r"var\s+resp\s*=\s*\{\{\s*(data\.[^}\s\|]+)\s*\}\}", str(rv), re.I)
                    if m:
                        api_src = m.group(1)
                        m2 = re.search(r"resp(?:\[[^\]]+\]|\.[A-Za-z0-9_:@]+)+", str(rv), re.I)
                        if m2:
                            api_path = m2.group(0)
                            toks = re.findall(r'\["([^"]+)"\]|\[\'([^\']+)\'\]|\.([A-Za-z0-9_:@]+)', api_path)
                            flat = [a or b or c for a,b,c in toks]
                            if flat:
                                api_field = flat[-1]
                            nil = re.search(r'hasOwnProperty\(\s*["\']@i:nil["\']\s*\)', str(rv))
                            human = f"שולף מ־{api_src} את '{api_field}'" + (f" (נתיב: {api_path})" if api_path else "") + (", רק אם אינו @i:nil" if nil else ".")

                    assign_rows.append({
                        "נוד": nid,
                        "כותרת נוד": title,
                        "ContextKey (היעד)": ctx,
                        "SetType": stype,
                        "ObjectType": otype,
                        "מקור API": api_src,
                        "נתיב API": api_path,
                        "שדה API": api_field,
                        "תיאור אנושי": human or ("ממשתנים אחרים" if used_vars else ""),
                        "ReturnValue (נקי)": str(rv).replace("\r",""),
                        "משתני מקור שזוהו": used_vars,
                    })

            # transitions inside du
            for tgt in find_du_transitions(du_json):
                trans_rows.append({
                    "מקור": nid, "מקור_שם": title,
                    "יעד": tgt, "יעד_שם": "", "אופן": "du"
                })

        # step transition
        step = n.get("step","") or ""
        m = re.match(r"^(n_\d+),([^\"]+)$", str(step))
        if m:
            trans_rows.append({
                "מקור": nid, "מקור_שם": title,
                "יעד": m.group(1), "יעד_שם": "", "אופן": "step"
            })

    # Add parent→child edges into Transitions (as 'child') so graph/diff see them
    for parent, kids in parent2children.items():
        for kid in kids:
            trans_rows.append({
                "מקור": parent, "מקור_שם": id2title.get(parent, ""),
                "יעד": kid,    "יעד_שם": id2title.get(kid, ""),
                "אופן": "child"
            })

    # Build dataframes
    nodes_df = pd.DataFrame(nodes_rows).drop_duplicates()
    vars_df  = pd.DataFrame(vars_rows).drop_duplicates()
    conds_df = pd.DataFrame(cond_rows).drop_duplicates()
    assign_df= pd.DataFrame(assign_rows).drop_duplicates()
    trans_df = pd.DataFrame(trans_rows).drop_duplicates()

    # Orphans — variables
    assigned = set([r for r in assign_df["ContextKey (היעד)"] if isinstance(r, str) and r])
    checked  = set([r for r in conds_df["PropertyName"] if isinstance(r, str) and r])
    used_all = set([r for r in vars_df["ContextKey (שימוש)"] if isinstance(r, str) and r])
    used_excl_condition = set(vars_df[vars_df["נלקח מ"]!="condition"]["ContextKey (שימוש)"].astype(str).tolist())

    defined_not_used = sorted(v for v in assigned if v not in used_all and v not in checked)
    checked_only    = sorted(v for v in checked if v not in used_excl_condition)

    # Terminal / Transfer views (פשוט מציגים לפי השדות)
    terminal_df = nodes_df[nodes_df["end"].isin(["2","true","True","1"])][["נוד","כותרת נוד","end"]].copy()
    if not terminal_df.empty:
        terminal_df.rename(columns={"end":"טיפוס"}, inplace=True)
        terminal_df["טיפוס"] = "סיום שיחה (end)"

    transfer_df = nodes_df[nodes_df["is_transfer"]=="1"][["נוד","כותרת נוד"]].copy()
    if not transfer_df.empty:
        transfer_df["טיפוס"] = "העברה לנציג (transfer)"

    # Orphan nodes (Heuristic: אין יציאות מוצהרות)
    exits = set(trans_df["מקור"]) if not trans_df.empty else set()
    end_nodes = set(terminal_df["נוד"]) if not terminal_df.empty else set()
    transfer_nodes = set(transfer_df["נוד"]) if not transfer_df.empty else set()
    parents_with_children = set(nodes_df["הורה"]) - {""} if not nodes_df.empty else set()
    has_exit = exits | end_nodes | transfer_nodes | parents_with_children
    all_nodes = list(nodes_df["נוד"]) if not nodes_df.empty else []
    no_exit_nodes = [nid for nid in all_nodes if nid and nid not in has_exit]

    orph_vars_df = pd.DataFrame(
        [{"סוג":"מוגדר אבל לא בשימוש","ContextKey":v} for v in defined_not_used] +
        [{"סוג":"נבדק אבל לא בשימוש","ContextKey":v} for v in checked_only]
    )
    orph_nodes_df = pd.DataFrame({
        "נוד": no_exit_nodes,
        "כותרת נוד": [id2title.get(nid,"") for nid in no_exit_nodes],
        "בעיה": ["ללא יציאות (אין step/du/ילד/סיום/העברה לנציג)"] * len(no_exit_nodes),
    })

    return {
        "Nodes": nodes_df,
        "Variables (usage)": vars_df,
        "Conditions (friendly)": conds_df,
        "Assignments (du sets)": assign_df,
        "Transitions (step+du)": trans_df,     # includes 'child'
        "Orphans — Variables": orph_vars_df,
        "Orphans — Nodes": orph_nodes_df,
        "Terminal (end)": terminal_df,
        "Transfer nodes": transfer_df,
    }

def analyze_xml_text(xml_text: str):
    m = re.search(r'<SCRIPT[^>]*\bValue=\"(.*?)\"', xml_text, flags=re.S|re.I)
    if not m:
        raise ValueError("לא נמצא SCRIPT Value בקובץ.")
    val = m.group(1)
    json_text = deep_unescape(val, rounds=3)
    nodes = json.loads(json_text)
    return analyze_nodes(nodes), nodes

# ---------------- Simulation ----------------
def render_template_vars(s: str, ctx: Dict[str, Any]) -> str:
    def repl(m):
        key = m.group(1)
        cur = ctx
        for part in key.split('.'):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return ""
        return str(cur)
    return re.sub(r"\{\{\s*(data\.[^}\s\|]+)\s*\}\}", repl, s or "")

def apply_assignments(node: Dict[str, Any], ctx: Dict[str, Any], trace: List[Dict[str,Any]]):
    du = parse_du(node.get("du","") or "")
    if not isinstance(du, list): return
    for item in du:
        if isinstance(item, dict) and str(item.get("action","")).lower() == "set":
            p = item.get("params", {})
            k = p.get("ContextKey") or p.get("DestinationContextKey") or p.get("TargetContextKey")
            rv = p.get("ReturnValue") or p.get("functionBody") or p.get("code") or p.get("js") or ""
            if not k: continue
            rendered = render_template_vars(str(rv), ctx)
            cur = ctx
            parts = k.split('.')
            for pp in parts[:-1]:
                if pp not in cur or not isinstance(cur[pp], dict):
                    cur[pp] = {}
                cur = cur[pp]
            cur[parts[-1]] = rendered
            trace.append({"event":"assign","ContextKey":k,"value":rendered})

def node_end_or_transfer(node: Dict[str, Any]) -> str:
    end_flag = str(node.get("end","")) if ("end" in node) else ""
    if end_flag == "2" or (end_flag and end_flag.lower() in {"true","1"}): return "END"
    if is_transfer(node.get("stream")): return "TRANSFER"
    return ""

def next_by_step(node: Dict[str, Any]) -> str:
    step = node.get("step","")
    m = re.match(r"^(n_\d+),([^\"]+)$", str(step))
    if m: return m.group(1)
    return ""

def next_by_du(node: Dict[str, Any]) -> List[str]:
    du = parse_du(node.get("du","") or "")
    return find_du_transitions(du) if isinstance(du, list) else []

def run_simulation(nodes: List[Dict[str,Any]], start: str, init_ctx: Dict[str,Any], max_steps: int = 200):
    id2node = {n.get("id",""): n for n in nodes}
    trace: List[Dict[str,Any]] = []
    ctx = init_ctx.copy()
    cur = start
    steps = 0
    visited = set()
    while steps < max_steps and cur in id2node:
        node = id2node[cur]
        steps += 1
        kind = node_end_or_transfer(node)
        from_html = strip_tags(node.get("bodyHtml",""))
        trace.append({"event":"enter","node":cur,"title":node.get("text",""),"kind":kind,"msg":from_html})
        if kind: break
        apply_assignments(node, ctx, trace)
        step_id = next_by_step(node)
        du_ids = next_by_du(node)
        nxt = step_id or (du_ids[0] if du_ids else "")
        if not nxt:
            children = [n.get("id","") for n in nodes if n.get("parent","") == cur]
            if children: nxt = children[0]
        if not nxt:
            trace.append({"event":"halt","reason":"no-exit"}); break
        cur = nxt
        if (cur, steps) in visited:
            trace.append({"event":"loop","node":cur}); break
        visited.add((cur, steps))
    trace.append({"event":"context","ctx":json.dumps(ctx, ensure_ascii=False)})
    return trace

# ---------------- Graph (pyvis via write_html) ----------------
def make_graph(trans_df: pd.DataFrame, nodes_df: pd.DataFrame) -> str:
    from pyvis.network import Network
    if nodes_df is None or nodes_df.empty:
        raise ValueError("אין צמתים להצגה בגרף.")
    g = Network(height="720px", width="100%", directed=True, notebook=False)

    end_nodes = set(nodes_df[nodes_df["end"].isin(["2","true","True","1"])]["נוד"]) if not nodes_df.empty else set()
    transfer_nodes = set(nodes_df[nodes_df["is_transfer"]=="1"]["נוד"]) if not nodes_df.empty else set()

    for _, r in nodes_df.iterrows():
        nid = r["נוד"]
        label = f"{nid}\n{r.get('כותרת נוד','') or ''}"
        color = "#D1FAE5" if nid in end_nodes else ("#DBEAFE" if nid in transfer_nodes else None)
        g.add_node(nid, label=label, color=color)

    if trans_df is not None and not trans_df.empty:
        for _, r in trans_df.iterrows():
            src = r.get("מקור"); dst = r.get("יעד")
            if src and dst:
                g.add_edge(src, dst, title=r.get("אופן",""))

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
    tmp_path = tmp.name
    tmp.close()
    g.write_html(tmp_path)  # יציב ל-headless/Streamlit
    return tmp_path

# ---------------- Diff helpers (Better Diff) ----------------
def normalize_text(s: str) -> str:
    s = str(s or "")
    s = deep_unescape(s, rounds=2)
    s = strip_tags(s)
    return re.sub(r"\s+", " ", s).strip()

def hash_sig(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()[:10]

def norm_trans(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["מקור","יעד","אופן"])
    return df[["מקור","יעד","אופן"]].drop_duplicates().sort_values(["מקור","יעד","אופן"]).reset_index(drop=True)

def node_compare_frame(dfA: pd.DataFrame, dfB: pd.DataFrame) -> pd.DataFrame:
    keep = ["נוד","כותרת נוד","מה הלקוח רואה","end","is_transfer"]
    A = dfA[keep].copy() if not dfA.empty else pd.DataFrame(columns=keep)
    B = dfB[keep].copy() if not dfB.empty else pd.DataFrame(columns=keep)
    A["_msg_norm"] = A["מה הלקוח רואה"].map(normalize_text)
    B["_msg_norm"] = B["מה הלקוח רואה"].map(normalize_text)
    merged = A.merge(B, on="נוד", how="outer", suffixes=("_A","_B"), indicator=True)
    merged["Δ כותרת"]  = merged["כותרת נוד_A"].fillna("") != merged["כותרת נוד_B"].fillna("")
    merged["Δ טקסט"]   = merged["_msg_norm_A"].fillna("") != merged["_msg_norm_B"].fillna("")
    merged["Δ end"]     = merged["end_A"].fillna("") != merged["end_B"].fillna("")
    merged["Δ transfer"]= merged["is_transfer_A"].fillna("") != merged["is_transfer_B"].fillna("")
    out_cols = [
        "נוד",
        "כותרת נוד_A","כותרת נוד_B","Δ כותרת",
        "מה הלקוח רואה_A","מה הלקוח רואה_B","Δ טקסט",
        "end_A","end_B","Δ end",
        "is_transfer_A","is_transfer_B","Δ transfer",
        "_merge"
    ]
    return merged[out_cols].sort_values("נוד")

# -------------------------- UI --------------------------
uploaded = st.file_uploader("בחר קובץ XML של CommBox", type=["xml"])
data = None
nodes = None
if uploaded:
    xml_text = uploaded.read().decode("utf-8", errors="ignore")
    try:
        data, nodes = analyze_xml_text(xml_text)
    except Exception as e:
        st.error(f"שגיאה בניתוח: {e}")

if data:
    tabs = st.tabs(list(data.keys()) + ["Graph", "Simulator", "Diff"])

    # datasets
    for i, name in enumerate(list(data.keys())):
        with tabs[i]:
            df = data[name]
            st.subheader(f"{name} ({len(df):,})")
            c1, c2 = st.columns([3,1])
            with c1:
                q = st.text_input(f"חיפוש ב-{name}", key=f"q_{name}")
            with c2:
                csv = df.to_csv(index=False).encode('utf-8-sig')
                st.download_button("הורדת CSV", csv, file_name=f"{name}.csv", mime="text/csv")
            if q:
                mask = df.apply(lambda col: col.astype(str).str.contains(q, case=False, na=False))
                df2 = df[mask.any(axis=1)]
            else:
                df2 = df
            st.dataframe(df2, use_container_width=True, hide_index=True)

    # Graph
    with tabs[-3]:
        try:
            html_path = make_graph(data["Transitions (step+du)"], data["Nodes"])
            st.components.v1.html(open(html_path, 'r', encoding='utf-8').read(), height=760, scrolling=True)
        except Exception as e:
            st.error(f"שגיאה בבניית הגרף: {e}")

    # Simulator
    with tabs[-2]:
        start_node = st.text_input("Node התחלה (למשל n_3)")
        init_json = st.text_area("Initial context JSON (לדוגמה {\"data\": {}})", value='{"data": {}}')
        if st.button("הרץ סימולציה"):
            if not nodes:
                st.warning("לא נטענו נודים.")
            else:
                try:
                    ctx = json.loads(init_json)
                except Exception as e:
                    st.error(f"שגיאת JSON בקונטקסט: {e}")
                    ctx = {"data": {}}
                trace = run_simulation(nodes, start_node.strip(), ctx)
                st.subheader("Trace")
                st.dataframe(pd.DataFrame(trace), use_container_width=True, hide_index=True)

    # Diff (Better)
    with tabs[-1]:
        st.write("השווה מול XML נוסף:")
        other = st.file_uploader("בחר XML שני להשוואה", type=["xml"], key="other")
        if other:
            xml2 = other.read().decode("utf-8", errors="ignore")
            try:
                dataB, nodesB = analyze_xml_text(xml2)
            except Exception as e:
                st.error(f"שגיאה בניתוח הקובץ השני: {e}")
                dataB = None

            if dataB:
                st.subheader("תקציר שינויים")

                # Nodes Δ
                comp_nodes = node_compare_frame(data["Nodes"], dataB["Nodes"])
                added_nodes   = comp_nodes[comp_nodes["_merge"]=="right_only"][["נוד","כותרת נוד_B"]].rename(columns={"כותרת נוד_B":"כותרת"})
                removed_nodes = comp_nodes[comp_nodes["_merge"]=="left_only"][["נוד","כותרת נוד_A"]].rename(columns={"כותרת נוד_A":"כותרת"})
                changed_mask = (comp_nodes["_merge"]=="both") & (
                    comp_nodes["Δ כותרת"] | comp_nodes["Δ טקסט"] | comp_nodes["Δ end"] | comp_nodes["Δ transfer"]
                )
                changed_nodes = comp_nodes[changed_mask].copy()

                c1, c2, c3 = st.columns(3)
                c1.metric("נודים שנוספו", len(added_nodes))
                c2.metric("נודים שנמחקו", len(removed_nodes))
                c3.metric("נודים שהשתנו", len(changed_nodes))

                with st.expander("נוסף (Nodes Added)"):
                    st.dataframe(added_nodes.reset_index(drop=True), use_container_width=True, hide_index=True)
                    st.download_button("CSV", added_nodes.to_csv(index=False).encode('utf-8-sig'), "nodes_added.csv")

                with st.expander("נמחק (Nodes Removed)"):
                    st.dataframe(removed_nodes.reset_index(drop=True), use_container_width=True, hide_index=True)
                    st.download_button("CSV", removed_nodes.to_csv(index=False).encode('utf-8-sig'), "nodes_removed.csv")

                with st.expander("השתנה (Nodes Changed) – שדה-לשדה"):
                    st.dataframe(changed_nodes.reset_index(drop=True), use_container_width=True, hide_index=True)
                    st.download_button("CSV", changed_nodes.to_csv(index=False).encode('utf-8-sig'), "nodes_changed.csv")

                st.markdown("---")

                # Transitions Δ (כולל child)
                st.markdown("**Transitions Δ** (כולל child)")
                TA, TB = norm_trans(data["Transitions (step+du)"]), norm_trans(dataB["Transitions (step+du)"])
                onlyA = TA.merge(TB, on=["מקור","יעד","אופן"], how="left", indicator=True)
                onlyA = onlyA[onlyA["_merge"]=="left_only"][["מקור","יעד","אופן"]]
                onlyB = TB.merge(TA, on=["מקור","יעד","אופן"], how="left", indicator=True)
                onlyB = onlyB[onlyB["_merge"]=="left_only"][["מקור","יעד","אופן"]]
                c4, c5 = st.columns(2)
                with c4:
                    st.caption("נמחקו/השתנו")
                    st.dataframe(onlyA, use_container_width=True, hide_index=True)
                    st.download_button("CSV", onlyA.to_csv(index=False).encode('utf-8-sig'), "transitions_removed.csv")
                with c5:
                    st.caption("נוספו")
                    st.dataframe(onlyB, use_container_width=True, hide_index=True)
                    st.download_button("CSV", onlyB.to_csv(index=False).encode('utf-8-sig'), "transitions_added.csv")

                st.markdown("---")

                # Assignments Δ
                st.markdown("**Assignments Δ** (ContextKey per Node)")
                A_asg = data["Assignments (du sets)"].copy()
                B_asg = dataB["Assignments (du sets)"].copy()
                for df in (A_asg, B_asg):
                    df["RV_sig"] = df["ReturnValue (נקי)"].map(hash_sig)
                    df["_key"] = df["נוד"].astype(str)+"||"+df["ContextKey (היעד)"].astype(str)

                keysA = set(A_asg["_key"]) ; keysB = set(B_asg["_key"])
                added_keys   = sorted(keysB - keysA)
                removed_keys = sorted(keysA - keysB)
                inter_keys   = sorted(keysA & keysB)

                added_asg   = B_asg[B_asg["_key"].isin(added_keys)][["נוד","כותרת נוד","ContextKey (היעד)","ObjectType","SetType","שדה API","RV_sig"]]
                removed_asg = A_asg[A_asg["_key"].isin(removed_keys)][["נוד","כותרת נוד","ContextKey (היעד)","ObjectType","SetType","שדה API","RV_sig"]]

                A_map = A_asg.set_index("_key")["RV_sig"].to_dict()
                B_map = B_asg.set_index("_key")["RV_sig"].to_dict()
                changed_keys = [k for k in inter_keys if A_map.get(k) != B_map.get(k)]
                changed_asg = []
                for k in changed_keys:
                    nid, ctx = k.split("||",1)
                    a = A_asg[A_asg["_key"]==k].iloc[0]
                    b = B_asg[B_asg["_key"]==k].iloc[0]
                    changed_asg.append({
                        "נוד": nid,
                        "ContextKey": ctx,
                        "RV_sig_A": a["RV_sig"],
                        "RV_sig_B": b["RV_sig"],
                        "API_A": a["שדה API"],
                        "API_B": b["שדה API"],
                        "כותרת_A": a["כותרת נוד"],
                        "כותרת_B": b["כותרת נוד"],
                    })
                changed_asg = pd.DataFrame(changed_asg)

                c6, c7, c8 = st.columns(3)
                c6.metric("השמות שנוספו", len(added_asg))
                c7.metric("השמות שנמחקו", len(removed_asg))
                c8.metric("השמות עם שינוי ערך", len(changed_asg))

                with st.expander("נוספו (Assignments Added)"):
                    st.dataframe(added_asg, use_container_width=True, hide_index=True)
                    st.download_button("CSV", added_asg.to_csv(index=False).encode('utf-8-sig'), "assignments_added.csv")
                with st.expander("נמחקו (Assignments Removed)"):
                    st.dataframe(removed_asg, use_container_width=True, hide_index=True)
                    st.download_button("CSV", removed_asg.to_csv(index=False).encode('utf-8-sig'), "assignments_removed.csv")
                with st.expander("השתנו (Assignments Changed Value)"):
                    st.dataframe(changed_asg, use_container_width=True, hide_index=True)
                    st.download_button("CSV", changed_asg.to_csv(index=False).encode('utf-8-sig'), "assignments_changed.csv")

                st.markdown("---")

                # Conditions Δ
                st.markdown("**Conditions Δ** (תנאי (תמצית))")
                A_c = data["Conditions (friendly)"][["נוד","תנאי (תמצית)"]].dropna().copy()
                B_c = dataB["Conditions (friendly)"][["נוד","תנאי (תמצית)"]].dropna().copy()
                A_set = A_c.groupby("נוד")["תנאי (תמצית)"].apply(lambda s: set(s.tolist()))
                B_set = B_c.groupby("נוד")["תנאי (תמצית)"].apply(lambda s: set(s.tolist()))

                all_ids = sorted(set(A_set.index) | set(B_set.index))
                cond_rows = []
                for nid in all_ids:
                    a = A_set.get(nid, set())
                    b = B_set.get(nid, set())
                    add = sorted(b - a)
                    rem = sorted(a - b)
                    if add or rem:
                        cond_rows.append({"נוד":nid, "נוספו תנאים": "; ".join(add), "נמחקו תנאים": "; ".join(rem)})
                cond_delta = pd.DataFrame(cond_rows)

                st.dataframe(cond_delta, use_container_width=True, hide_index=True)
                st.download_button("CSV", cond_delta.to_csv(index=False).encode('utf-8-sig'), "conditions_delta.csv")

                st.markdown("---")

                # Drill-down לנוד שהשתנה
                st.markdown("**Drill-down לנוד**")
                changed_ids = sorted(set(changed_nodes["נוד"].tolist()) |
                                     set(onlyA["יעד"].tolist()) | set(onlyA["מקור"].tolist()) |
                                     set(onlyB["יעד"].tolist()) | set(onlyB["מקור"].tolist()) )
                nid_pick = st.selectbox("בחר נוד להשוואה צד-לצד", options=changed_ids)
                if nid_pick:
                    row = comp_nodes[comp_nodes["נוד"]==nid_pick].iloc[:1]
                    cA = row[["כותרת נוד_A","מה הלקוח רואה_A","end_A","is_transfer_A"]].rename(columns={
                        "כותרת נוד_A":"כותרת","מה הלקוח רואה_A":"מה הלקוח רואה","end_A":"end","is_transfer_A":"is_transfer"
                    })
                    cB = row[["כותרת נוד_B","מה הלקוח רואה_B","end_B","is_transfer_B"]].rename(columns={
                        "כותרת נוד_B":"כותרת","מה הלקוח רואה_B":"מה הלקוח רואה","end_B":"end","is_transfer_B":"is_transfer"
                    })
                    colA, colB = st.columns(2)
                    with colA:
                        st.caption("גרסה A")
                        st.dataframe(cA, use_container_width=True, hide_index=True)
                    with colB:
                        st.caption("גרסה B")
                        st.dataframe(cB, use_container_width=True, hide_index=True)

# Export full Excel
if data:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        for name, df in data.items():
            df.to_excel(writer, index=False, sheet_name=name[:31])
    st.download_button("💾 הורדת Excel מלא", buf.getvalue(), file_name="commbox_analysis.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
else:
    st.info("טען קובץ XML כדי להתחיל ⬆️")
