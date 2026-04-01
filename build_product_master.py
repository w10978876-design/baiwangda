"""
构建商品主数据中间表（第0~2步）
================================
输入：
  - data/壹度商品表.xlsx
  - data/安达商品表.xlsx

输出：
  - 商品主数据_中间表.xlsx
    - 商品主数据（全量）
    - 低置信待审池
"""

import os
import re
from typing import List, Tuple

import pandas as pd

from fmcg_analysis_v3 import CAT_MAP, FLAVOR_WORDS_SORTED, parse_product, apply_parse_refinements


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILES = [
    os.path.join(SCRIPT_DIR, "data", "壹度商品表.xlsx"),
    os.path.join(SCRIPT_DIR, "data", "安达商品表.xlsx"),
]
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "商品主数据_中间表.xlsx")
MASTER_OVERRIDE_FILE = os.path.join(SCRIPT_DIR, "商品解析覆盖表.xlsx")
# 中间表构建默认不回读覆盖表，避免历史错误“自我强化”污染主数据。
# 如需启用（例如仅做手工修订回灌），请改为 True。
APPLY_OVERRIDES_IN_BUILD = False

UNKNOWN_BRANDS = {"", "-", "—", "未知", "未知[-]", "nan", "None", "null", "NULL"}
SUSPECT_BODIES = {
    "味",
    "荷荷",
    "酵母",
    "鲜",
    "香",
    "新",
    "轻",
    "爽",
    "真",
    "浓",
    "醇",
}

# 低置信池“主体白名单”：主体在这些集合内时，即便置信略低也通常是正确拆解
# （避免出现“主体=醋/盐/糖”等短词被规则误伤）
ACCEPT_BODIES_BY_CAT = {
    "粮油调味": {"醋", "食糖", "食盐", "油", "酱油", "调味料", "盐", "糖", "食用油"},
    "酒类": {"酒类", "白酒", "啤酒", "梅酒", "葡萄酒", "鸡尾酒"},
    "日化美护": {"面巾纸", "纸品"},
    "日配烘焙": {"面包", "起酥面包", "迷你牛角包", "牛角包", "蛋糕", "面包"},
}

# 乳品类：主体识别到位但口味为空，通常属于可接受的低风险情形
DAIRY_ACCEPT_MISSING_FLAVOR_BODIES = {"发酵乳", "酸奶", "酸牛奶", "牛奶", "椰子水", "轻食杯", "含乳饮料", "乳酸菌"}

# 半自动：自动尝试修复后，达到该置信度阈值则不进入人工待审池
AUTO_ACCEPT_CONF = 0.80

# 生成候选解析时，统一大分类尝试集合（更通用的兜底：unified_cat 为空）
AUTO_RETRY_UNIFIED_CATS = lambda uc: [str(uc).strip(), ""]

def clean_brand(raw_brand: str) -> str:
    b = str(raw_brand).strip()
    b = re.sub(r"\[.*?\]", "", b).strip()
    if b in UNKNOWN_BRANDS:
        return ""
    return b


def normalize_name(name: str) -> str:
    s = str(name).strip()
    s = re.sub(r"^[【\[]\s*新\s*[】\]]\s*", "", s)
    s = re.sub(r"^[（(]\s*新\s*[）)]\s*", "", s)
    s = re.sub(r"^[^一-龥A-Za-z0-9]+", "", s)
    s = re.sub(r"^[A-Za-z]{1,3}\s*", "", s)
    return s.strip()


def map_channel(ch: str) -> str:
    c = str(ch).strip()
    if "壹度" in c:
        return "渠道A"
    if "安达" in c or "安哒" in c:
        return "渠道B"
    return c


def map_unified_cat(channel_raw: str, raw_cat: str) -> str:
    key = (map_channel(channel_raw), str(raw_cat).strip())
    return CAT_MAP.get(key, str(raw_cat).strip())


def build_sku_key(channel: str, item_code: str, norm_name: str) -> str:
    c = str(channel).strip()
    code = str(item_code).strip()
    if code and code.lower() != "nan":
        return f"{c}::{code}"
    return f"{c}::N::{norm_name}"


def score_confidence(
    raw_name: str,
    brand_clean: str,
    brand: str,
    body: str,
    flavor: str,
    unified_cat: str,
) -> Tuple[float, List[str]]:
    score = 1.0
    reasons: List[str] = []

    b = str(body).strip()
    f = str(flavor).strip()
    r = str(raw_name).strip()
    uc = str(unified_cat).strip()

    if not brand:
        score -= 0.12
        reasons.append("品牌为空，依赖名称推断失败")
    elif brand_clean and brand != brand_clean and not brand.startswith(brand_clean):
        score -= 0.08
        reasons.append("品牌与品牌字段差异较大")

    if not b:
        score -= 0.50
        reasons.append("主体为空")
    else:
        if len(b) <= 1:
            score -= 0.35
            reasons.append("主体过短")
        if b in SUSPECT_BODIES:
            score -= 0.45
            reasons.append("主体命中可疑词")
        if b in r and any(w in b for w in FLAVOR_WORDS_SORTED if len(w) >= 2):
            # 主体中含明显口味词，常见于切分错误（如 巧克力菠萝包）
            score -= 0.22
            reasons.append("主体包含疑似口味词")
        if re.search(r"[+＋]", b):
            score -= 0.12
            reasons.append("主体含连接符，疑似主体丢失")

    if uc in ("日配冷藏", "常温乳品"):
        if b in DAIRY_ACCEPT_MISSING_FLAVOR_BODIES and not f:
            # 只扣很小幅度：口味为空更多是“词典覆盖不足/命中顺序导致”，通常不致命
            score -= 0.03
            reasons.append("乳品主体已识别但口味为空")

    score = max(0.0, min(1.0, round(score, 4)))
    return score, reasons


def load_master_overrides(master_file: str) -> pd.DataFrame:
    """可选：读取 sku_key -> 解析结果 覆盖表（若不存在则返回空表）。"""
    if not os.path.exists(master_file):
        return pd.DataFrame()
    try:
        xls = pd.ExcelFile(master_file)
        sheet = "master_overrides" if "master_overrides" in xls.sheet_names else xls.sheet_names[0]
        df = pd.read_excel(master_file, sheet_name=sheet, dtype=str).fillna("")
        return df
    except Exception:
        # 覆盖表格式不对时直接忽略，保证主流程不被阻断
        return pd.DataFrame()


def parse_source_label(brand_clean: str, brand: str) -> str:
    if brand_clean and brand and (brand == brand_clean or brand.startswith(brand_clean)):
        return "rule+brand_field"
    if brand:
        return "rule+name_prefix"
    return "rule_low"


def _split_by_plus(raw_name: str) -> List[str]:
    """按 '+' / '＋' 拆分，返回可能的候选子串（去掉空串、保留原值）。"""
    s = str(raw_name)
    if "+" not in s and "＋" not in s:
        return [s]
    parts = []
    for sep in ["+", "＋"]:
        if sep in s:
            parts = re.split(re.escape(sep), s)
            break
    parts = [p for p in (p.strip() for p in parts) if p]
    if not parts:
        return [s]
    # 返回“完整串 + 两边”最大化保留信息
    return [s] + parts


def _remove_parentheses_content(raw_name: str) -> str:
    """去掉括号内容：形如 'x（y味）' 时，y味可能被当成主体/规格噪音。"""
    s = str(raw_name)
    s = re.sub(r"[（(][^）)]*[）)]", "", s)
    return s.strip()


def auto_retry_parse_row(row, raw_brand: str, brand_clean: str, norm_name: str) -> Tuple[pd.Series, float, List[str]]:
    """
    对单行生成候选拆解：重新调用 parse_product，并用 score_confidence 选最大置信度结果。
    返回(best_fields_series, best_confidence, best_reasons)
    """
    raw_name = row["raw_name"]
    unified_cat = row["unified_cat"]
    init_uc = str(unified_cat).strip()

    name_candidates = []
    name_candidates.extend(_split_by_plus(raw_name))
    # 额外候选：去括号内容（减少“主体=口味括号前遗留”的概率）
    no_paren = _remove_parentheses_content(raw_name)
    if no_paren and no_paren != raw_name:
        name_candidates.append(no_paren)

    # 去重保序
    seen = set()
    name_candidates2 = []
    for n in name_candidates:
        if n not in seen:
            seen.add(n)
            name_candidates2.append(n)

    unified_cats = AUTO_RETRY_UNIFIED_CATS(init_uc)
    unified_cats = [u for u in unified_cats if u is not None]
    best_conf = -1.0
    best_reasons: List[str] = []
    best_parsed = None
    best_parse_source = ""

    for cand_name in name_candidates2[:6]:  # 控制候选数量
        for cand_uc in unified_cats[:2]:
            # parse_product 返回的是 pd.Series（brand/body/flavor/spec_val/spec_unit/package）
            parsed = parse_product(cand_name, brand_clean, cand_uc)
            # parse_product 的 Series 没有显式列名，用固定位置取值
            cand_brand = str(parsed.iloc[0]).strip()
            cand_body = str(parsed.iloc[1]).strip()
            cand_flavor = str(parsed.iloc[2]).strip()
            cand_spec_val = parsed.iloc[3]
            cand_spec_unit = parsed.iloc[4]
            cand_package = parsed.iloc[5]

            # 重新计算置信度（基于当前候选拆解）
            conf, reasons = score_confidence(
                raw_name=cand_name,
                brand_clean=brand_clean,
                brand=cand_brand,
                body=cand_body,
                flavor=cand_flavor,
                unified_cat=cand_uc,
            )
            if conf > best_conf:
                best_conf = conf
                best_reasons = reasons
                best_parsed = (cand_brand, cand_body, cand_flavor, cand_spec_val, cand_spec_unit, cand_package)
                best_parse_source = parse_source_label(brand_clean, cand_brand)

    # best_parsed 作为 Series：brand/body/...
    best_fields = row.copy()
    best_fields["brand"] = best_parsed[0]
    best_fields["body"] = best_parsed[1]
    best_fields["flavor"] = best_parsed[2]
    best_fields["spec_val"] = best_parsed[3]
    best_fields["spec_unit"] = best_parsed[4]
    best_fields["package"] = best_parsed[5]
    best_fields["parse_source"] = best_parse_source

    return best_fields, best_conf, best_reasons


def main() -> None:
    dfs = []
    for fp in INPUT_FILES:
        if not os.path.exists(fp):
            raise FileNotFoundError(f"缺少输入文件：{fp}")
        df = pd.read_excel(fp, dtype=str).fillna("")
        dfs.append(df)

    raw = pd.concat(dfs, ignore_index=True)
    required_cols = {"渠道", "商品编码", "商品名称", "商品品牌", "大分类名称"}
    miss = required_cols - set(raw.columns)
    if miss:
        raise ValueError(f"输入缺少字段：{sorted(miss)}")

    work = raw.copy()
    work["raw_name"] = work["商品名称"].astype(str).str.strip()
    work["raw_brand"] = work["商品品牌"].astype(str).str.strip()
    work["brand_clean"] = work["raw_brand"].apply(clean_brand)
    work["raw_cat"] = work["大分类名称"].astype(str).str.strip()
    work["unified_cat"] = work.apply(lambda x: map_unified_cat(x["渠道"], x["raw_cat"]), axis=1)
    work["norm_name"] = work["raw_name"].apply(normalize_name)
    work["item_code"] = work["商品编码"].astype(str).str.strip()
    work["sku_key"] = work.apply(
        lambda x: build_sku_key(x["渠道"], x["item_code"], x["norm_name"]),
        axis=1,
    )

    parsed = work.apply(
        lambda x: parse_product(x["raw_name"], x["brand_clean"], x["unified_cat"]),
        axis=1,
    )
    parsed.columns = ["brand", "body", "flavor", "spec_val", "spec_unit", "package"]
    out = pd.concat([work, parsed], axis=1)

    # ── 可选：覆盖表回填（用于稳定修复低置信“沉淀”问题）────────────────
    overrides = load_master_overrides(MASTER_OVERRIDE_FILE) if APPLY_OVERRIDES_IN_BUILD else pd.DataFrame()
    if not overrides.empty and "sku_key" in overrides.columns:
        key_col = "sku_key"
        ov = overrides.set_index(key_col)
        # 逐行回填：覆盖表优先级最高
        def apply_row_override(x):
            k = x["sku_key"]
            if k not in ov.index:
                return x
            row = ov.loc[k]
            x["_override_applied"] = True
            # 约定覆盖表空字符串不覆盖（防止误填）
            for col in ["brand", "body", "flavor", "spec_val", "spec_unit", "package"]:
                if col in ov.columns:
                    v = str(row.get(col, "")).strip()
                    if v != "":
                        x[col] = v
            if "confidence" in ov.columns:
                cv = str(row.get("confidence", "")).strip()
                if cv != "":
                    try:
                        x["_override_conf"] = float(cv)
                    except Exception:
                        pass
            if "parse_source" in ov.columns:
                ps = str(row.get("parse_source", "")).strip()
                if ps != "":
                    x["_override_parse_source"] = ps
            return x

        out = out.apply(apply_row_override, axis=1)

    # 与主分析脚本一致：覆盖表之后仍走全表 refine，避免旧覆盖污染主数据
    apply_parse_refinements(
        out,
        name_col="raw_name",
        brand_field_col="brand_clean",
        uc_col="unified_cat",
        out_brand_col="brand",
        out_body_col="body",
        out_flavor_col="flavor",
        out_pkg_col="package",
    )

    conf = out.apply(
        lambda x: score_confidence(
            x["raw_name"],
            x["brand_clean"],
            x["brand"],
            x["body"],
            x["flavor"],
            x["unified_cat"],
        ),
        axis=1,
    )
    out["confidence_calc"] = conf.apply(lambda x: x[0])
    out["low_conf_reasons"] = conf.apply(lambda x: "；".join(x[1]))
    out["parse_source_calc"] = out.apply(lambda x: parse_source_label(x["brand_clean"], x["brand"]), axis=1)

    # 覆盖表优先
    out["confidence"] = out["confidence_calc"]
    out["_override_applied"] = out.get("_override_applied", False)
    if "_override_conf" in out.columns:
        out.loc[out["_override_applied"] & out["_override_conf"].notna(), "confidence"] = out.loc[
            out["_override_applied"] & out["_override_conf"].notna(), "_override_conf"
        ]

    out["parse_source"] = out["parse_source_calc"]
    if "_override_parse_source" in out.columns:
        mask = out["_override_applied"] & out["_override_parse_source"].notna() & (out["_override_parse_source"] != "")
        out.loc[mask, "parse_source"] = out.loc[mask, "_override_parse_source"]

    out["is_whitelisted_body"] = out.apply(
        lambda x: x["body"] in ACCEPT_BODIES_BY_CAT.get(x["unified_cat"], set()),
        axis=1,
    )

    # 低置信待审池：
    # 1) 非白名单主体才进入；
    # 2) 以置信阈值 + 风险理由组合为准，减少误伤。
    low_pool = out[
        (~out["is_whitelisted_body"])
        & (
            (out["confidence"] < 0.70)
            | ((out["low_conf_reasons"].astype(str) != "") & (out["confidence"] < 0.82))
        )
    ]
    low_pool = low_pool.sort_values(
        ["confidence", "unified_cat", "raw_name"], ascending=[True, True, True]
    )

    def bucketize(x) -> str:
        if x.get("is_whitelisted_body", False):
            return "auto_pass"
        if "主体命中可疑词" in str(x.get("low_conf_reasons", "")):
            return "rule_fix"
        if "主体包含疑似口味词" in str(x.get("low_conf_reasons", "")):
            return "rule_fix"
        if x.get("confidence", 1.0) < 0.55:
            return "manual"
        # 默认归为 rule_fix，让后续进入规则回填/词典回灌
        return "rule_fix"

    out["review_bucket"] = out.apply(bucketize, axis=1)

    # ── 半自动：对低于阈值但不在白名单的记录，尝试系统自动更正 ────────────────
    # 只对当前 low_pool 做自动重试，避免全量成本。
    to_retry = low_pool.index
    if len(to_retry) > 0:
        for idx in to_retry:
            row = out.loc[idx]
            best_fields, best_conf, best_reasons = auto_retry_parse_row(
                row=row,
                raw_brand=str(row.get("raw_brand", "")).strip(),
                brand_clean=str(row.get("brand_clean", "")).strip(),
                norm_name=str(row.get("norm_name", "")).strip(),
            )
            out.loc[idx, "brand"] = best_fields["brand"]
            out.loc[idx, "body"] = best_fields["body"]
            out.loc[idx, "flavor"] = best_fields["flavor"]
            out.loc[idx, "spec_val"] = best_fields["spec_val"]
            out.loc[idx, "spec_unit"] = best_fields["spec_unit"]
            out.loc[idx, "package"] = best_fields["package"]
            out.loc[idx, "confidence"] = best_conf
            out.loc[idx, "low_conf_reasons"] = "；".join(best_reasons)
            out.loc[idx, "parse_source"] = best_fields["parse_source"]
            out.loc[idx, "auto_retry_best_conf"] = best_conf

            if best_conf >= AUTO_ACCEPT_CONF:
                out.loc[idx, "auto_corrected"] = True
            else:
                out.loc[idx, "auto_corrected"] = False

    # 重新构建 low_pool：达到阈值的自动通过；其余进入人工确认池
    out["is_whitelisted_body"] = out["body"].isin(
        [b for s in ACCEPT_BODIES_BY_CAT.values() for b in s]
    )

    low_pool = out[
        (~out["is_whitelisted_body"])
        & (out["confidence"] < AUTO_ACCEPT_CONF)
    ].copy()

    low_pool = low_pool.sort_values(
        ["confidence", "unified_cat", "raw_name"], ascending=[True, True, True]
    )

    cols = [
        "sku_key",
        "渠道",
        "item_code",
        "raw_name",
        "norm_name",
        "raw_brand",
        "brand_clean",
        "brand",
        "body",
        "flavor",
        "spec_val",
        "spec_unit",
        "package",
        "raw_cat",
        "unified_cat",
        "parse_source",
        "confidence",
        "auto_retry_best_conf",
        "review_bucket",
        "is_whitelisted_body",
        "low_conf_reasons",
    ]
    out = out[cols].sort_values(["unified_cat", "body", "raw_name"], ascending=[True, True, True])

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as w:
        out.to_excel(w, sheet_name="商品主数据", index=False)
        low_pool[cols].to_excel(w, sheet_name="低置信待审池", index=False)

    print("=== 商品主数据构建完成 ===")
    print(f"输入记录：{len(out):,}")
    print(f"低置信待审：{len(low_pool):,}")
    print(f"输出文件：{OUTPUT_FILE}")


if __name__ == "__main__":
    main()
