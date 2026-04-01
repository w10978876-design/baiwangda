"""
从 `商品主数据_中间表.xlsx` 的「低置信待审池」生成 `商品解析覆盖表.xlsx`
============================================================
目的：
  - 不要求你手写覆盖表
  - 只要你把低置信池补全了，系统就能把补全结果沉淀为覆盖表
"""

import os
import re

import pandas as pd


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INTERMEDIATE_FILE = os.path.join(SCRIPT_DIR, "商品主数据_中间表.xlsx")
OUTPUT_OVERRIDE_FILE = os.path.join(SCRIPT_DIR, "商品解析覆盖表.xlsx")


def main() -> None:
    if not os.path.exists(INTERMEDIATE_FILE):
        raise FileNotFoundError(f"缺少中间表：{INTERMEDIATE_FILE}")

    # 覆盖表应该包含：
    # 1) 高置信度部分（从商品主数据 confidence 截断）
    # 2) 人工确认/已解决的低置信部分（通常在本流程里表现为 confidence >= 0.99）
    HIGH_CONF = 0.95
    MANUAL_CONF = 0.99

    full = pd.read_excel(INTERMEDIATE_FILE, sheet_name="商品主数据")
    if "confidence" not in full.columns:
        raise ValueError("商品主数据缺少 confidence 列，无法从中间表自动生成覆盖表")

    # low sheet 可能为空，也可能仍保留人工补全的条目；尽可能并入
    low = None
    try:
        low = pd.read_excel(INTERMEDIATE_FILE, sheet_name="低置信待审池")
    except Exception:
        low = None

    manual_keys = set()
    if low is not None and len(low) > 0:
        # 手动补全的条目通常也会出现在低置信池里（但你也可能已将其回填导致低置信池为空）
        if "sku_key" in low.columns:
            manual_keys = set(low["sku_key"].astype(str).str.strip().tolist())

    body_str = full["body"].astype(str).fillna("").str.strip()
    flavor_str = full["flavor"].astype(str).fillna("").str.strip()
    # 质量过滤：主体不应是纯口味词（例如「香草味」）、也不应是明显过泛的类目
    # 另外：如果主体里已经包含了口味词，通常说明“主体-口味切分反了”，应排除进入覆盖表
    contains_its_flavor = [
        (fs != "" and fs in bs) for fs, bs in zip(flavor_str.tolist(), body_str.tolist())
    ]
    valid_body = (
        (body_str != "")
        & (~body_str.str.match(r".+味$"))
        & (~body_str.isin({"粉面原料"}))
        & (~pd.Series(contains_its_flavor, index=full.index))
    )

    # 筛选：高置信且通过质量过滤 或 人工确认
    mask_high = (full["confidence"].astype(float) >= HIGH_CONF) & valid_body
    mask_manual = full["confidence"].astype(float) >= MANUAL_CONF
    mask_manual = mask_manual & valid_body
    if manual_keys:
        mask_manual = mask_manual | full["sku_key"].astype(str).isin(manual_keys)

    master = full[(mask_high | mask_manual)].copy()

    # 无编码情况下的匹配键：渠道 + 归一化名称
    # fmcg_analysis_v3 里 join_key 的“渠道来源”最终会映射到壹度/安达，
    # 所以这里也做同样映射，确保命中。
    def _map_channel(v: str) -> str:
        s = str(v).strip()
        if s == "渠道A":
            return "壹度"
        if s == "渠道B":
            return "安达"
        return s

    master["渠道_mapped"] = master["渠道"].apply(_map_channel)
    master["join_key_no_code"] = master["渠道_mapped"].astype(str).str.strip() + "::N::" + master["norm_name"].astype(str).str.strip()

    # 写出到覆盖表（供 fmcg_analysis_v3.py 可选读取）
    with pd.ExcelWriter(OUTPUT_OVERRIDE_FILE, engine="openpyxl") as w:
        cols = [
            "join_key_no_code",
            "sku_key",
            "brand",
            "body",
            "flavor",
            "spec_val",
            "spec_unit",
            "package",
        ]
        if "parse_source" in master.columns:
            cols.append("parse_source")
        if "confidence" in master.columns:
            cols.append("confidence")
        cols = [c for c in cols if c in master.columns]
        master[cols].to_excel(w, sheet_name="master_overrides", index=False)

    print("=== 覆盖表已生成 ===")
    print(f"输入：{INTERMEDIATE_FILE}")
    print(f"输出：{OUTPUT_OVERRIDE_FILE}")
    print(f"覆盖行数（高置信+人工已确认）：{len(master):,}")


if __name__ == "__main__":
    main()

