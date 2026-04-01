import pathlib

import streamlit as st
import streamlit.components.v1 as components
import json


st.set_page_config(page_title="百旺达选品分析", layout="wide")

project_root = pathlib.Path(__file__).parent
report_file = project_root / "选品分析报告_v3.html"
json_file = project_root / "商品标签热度分析_热度数据.json"

if report_file.exists():
    html = report_file.read_text(encoding="utf-8")
    if json_file.exists():
        data_obj = json.loads(json_file.read_text(encoding="utf-8"))
        inject = "<script>window.__EMBEDDED_DATA__ = " + json.dumps(data_obj, ensure_ascii=False) + ";</script>"
        html = html.replace("<script>", inject + "\n<script>", 1)
    components.html(html, height=1800, scrolling=True)
else:
    st.error(f"未找到报告文件：{report_file.name}")
