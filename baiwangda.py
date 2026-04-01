import pathlib

import streamlit as st
import streamlit.components.v1 as components
import json


st.set_page_config(page_title="百旺达选品分析", layout="wide")
st.markdown(
    """
    <style>
    /* 关闭 Streamlit 外层滚动，只保留内嵌页面滚动 */
    html, body, [data-testid="stAppViewContainer"] {
      overflow: hidden !important;
      height: 100%;
    }
    [data-testid="stAppViewContainer"] > .main {
      height: 100vh;
      overflow: hidden;
    }
    [data-testid="stAppViewContainer"] > .main > div {
      padding-top: 0.5rem;
      padding-bottom: 0.5rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

project_root = pathlib.Path(__file__).parent
report_file = project_root / "选品分析报告_v3.html"
json_file = project_root / "商品标签热度分析_热度数据.json"

if report_file.exists():
    html = report_file.read_text(encoding="utf-8")
    if json_file.exists():
        data_obj = json.loads(json_file.read_text(encoding="utf-8"))
        inject = "<script>window.__EMBEDDED_DATA__ = " + json.dumps(data_obj, ensure_ascii=False) + ";</script>"
        html = html.replace("<script>", inject + "\n<script>", 1)
    components.html(html, height=3600, scrolling=False)
else:
    st.error(f"未找到报告文件：{report_file.name}")
