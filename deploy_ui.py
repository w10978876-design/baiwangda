import pathlib

import streamlit as st
import streamlit.components.v1 as components


st.set_page_config(page_title="百旺达选品分析", layout="wide")
st.title("百旺达选品分析")

project_root = pathlib.Path(__file__).parent
report_file = project_root / "选品分析报告_v3.html"
json_file = project_root / "商品标签热度分析_热度数据.json"

st.caption("已发布为 Streamlit Cloud 页面。")

if report_file.exists():
    html = report_file.read_text(encoding="utf-8")
    components.html(html, height=1800, scrolling=True)
else:
    st.error(f"未找到报告文件：{report_file.name}")

with st.expander("部署检查"):
    st.write(f"- 报告文件：{'✅' if report_file.exists() else '❌'} `{report_file.name}`")
    st.write(f"- 数据文件：{'✅' if json_file.exists() else '❌'} `{json_file.name}`")
    st.write("- Streamlit Cloud 的 Main file path 请填写：`deploy_ui.py`")
