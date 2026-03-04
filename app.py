import json

import pandas as pd
import requests
import streamlit as st

API_URL = "http://localhost:8000"

INTENT_LABEL = {
    "통계_분석": ("📊 통계 분석", "#1f77b4"),
    "축제_정보": ("📋 축제 정보", "#2ca02c"),
    "축제_목록": ("📑 축제 목록", "#ff7f0e"),
    "fallback":  ("💬 일반 답변", "#7f7f7f"),
}

SAMPLE_QUESTIONS = [
    "2025년에 진행한 수원축제 시간대별 방문인구를 알려줘",
    "화성문화제 2025년 20대 매출 분석해줘",
    "가장 최근에 수원에서 진행한 축제 알고싶어",
    "올해 진행한 축제 갯수를 알려줘",
]

# ── 페이지 설정 ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="축제 데이터 분석 챗봇",
    page_icon="🎪",
    layout="wide",
)

# ── 세션 초기화 ────────────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = []
if "last_meta" not in st.session_state:
    st.session_state.last_meta = {}
if "last_analysis_context" not in st.session_state:
    st.session_state.last_analysis_context = []

# ── 사이드바 ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🎪 축제 챗봇")
    st.caption("축제 데이터 분석 AI")

    if st.button("🗑️ 대화 초기화", use_container_width=True):
        st.session_state.messages = []
        st.session_state.last_meta = {}
        st.session_state.last_analysis_context = []
        st.rerun()

    st.divider()

    # 마지막 응답 intent
    meta = st.session_state.last_meta
    intent = meta.get("intent")
    if intent and intent in INTENT_LABEL:
        label, color = INTENT_LABEL[intent]
        st.markdown(
            f'<span style="background:{color};color:white;padding:3px 10px;'
            f'border-radius:12px;font-size:13px">{label}</span>',
            unsafe_allow_html=True,
        )
        st.write("")

    # 축제 컨텍스트
    ctx = meta.get("festival_context")
    if ctx:
        st.subheader("📋 분석 축제 정보")
        if ctx.get("event_nm"):
            st.markdown(f"**{ctx['event_nm']}**")
        if ctx.get("event_bgnde") and ctx.get("event_endde"):
            st.info(f"📅 {ctx['event_bgnde']} ~ {ctx['event_endde']}")
        region = f"{ctx.get('sido_nm', '')} {ctx.get('cty_nm', '')}".strip()
        if region:
            st.caption(f"📍 {region}")
        if ctx.get("event_plc"):
            st.caption(f"🏟 {ctx['event_plc']}")
        if ctx.get("event_site"):
            st.caption(f"🔗 {ctx['event_site']}")
    else:
        st.caption("축제 관련 질문을 하면\n여기에 축제 정보가 표시됩니다.")

    st.divider()
    st.caption("Powered by Gemini 2.5 Flash + PostgreSQL\n 현재는 정형 데이터를 사용하므로 RAG 시스템은 사용하지 않고 있습니다.")


# ── 메인 헤더 ─────────────────────────────────────────────────────────────────
st.title("🎪 축제 데이터 분석 챗봇")
st.caption("축제 방문인구, 매출, 연령대별 소비 패턴 등을 자연어로 질문해보세요.")

# ── 예시 질문 버튼 (대화 없을 때만) ───────────────────────────────────────────
if not st.session_state.messages:
    st.markdown("#### 💡 이런 질문을 해보세요")
    cols = st.columns(2)
    for i, q in enumerate(SAMPLE_QUESTIONS):
        if cols[i % 2].button(q, use_container_width=True, key=f"sample_{i}"):
            st.session_state.pending_question = q
            st.rerun()
    st.divider()


# ── 차트 렌더링 ────────────────────────────────────────────────────────────────
def render_charts(chart_data: list | None):
    if not chart_data:
        return
    st.markdown("---")
    st.markdown("##### 📈 데이터 시각화")
    for chart in chart_data:
        chart_type = chart.get("chart_type", "bar")
        title = chart.get("title", "")
        index = chart.get("index", [])
        series = chart.get("series", {})
        if not index or not series:
            continue
        try:
            df = pd.DataFrame(series, index=index)
            st.caption(title)
            if chart_type == "line":
                st.line_chart(df)
            else:
                st.bar_chart(df)
        except Exception:
            pass


# ── 대화 기록 표시 ────────────────────────────────────────────────────────────
def render_intent_badge(intent: str | None):
    if not intent or intent not in INTENT_LABEL:
        return
    label, color = INTENT_LABEL[intent]
    st.markdown(
        f'<span style="background:{color};color:white;padding:2px 9px;'
        f'border-radius:10px;font-size:12px">{label}</span>',
        unsafe_allow_html=True,
    )
    st.write("")


for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        if msg["role"] == "assistant":
            render_intent_badge(msg.get("intent"))
            st.markdown(msg["content"])
            render_charts(msg.get("chart_data"))
            if msg.get("sql_list"):
                with st.expander("🔍 실행된 SQL"):
                    for sql in msg["sql_list"]:
                        st.code(sql, language="sql")
        else:
            st.markdown(msg["content"])


# ── SSE 스트리밍 함수 ─────────────────────────────────────────────────────────
def _extract_analysis_context(chart_data: list | None) -> list[dict]:
    """chart_data에서 핵심 수치를 추출해 이전 분석 컨텍스트로 변환 (Method B)"""
    if not chart_data:
        return []
    context = []
    for chart in chart_data:
        idx = chart.get("index", [])
        series = chart.get("series", {})
        for series_name, values in series.items():
            if not idx or not values:
                continue
            peak_i = max(range(len(values)), key=lambda i: values[i])
            context.append({
                "table": chart.get("table", ""),
                "title": chart.get("title", ""),
                "series": series_name,
                "peak": {"label": idx[peak_i], "value": values[peak_i]},
                "total": sum(values),
            })
    return context


def _build_conversation_history(messages: list, max_turns: int = 5) -> list[dict]:
    """
    session_state.messages에서 최근 N턴의 대화 기록을 API 전달용으로 변환.
    - Method C: user 300자 / assistant 800자로 제한 완화
    - Method A: assistant 메시지에 chart_data 기반 핵심 수치 요약 추가
    """
    recent = messages[-(max_turns * 2):]
    result = []
    for m in recent:
        if m["role"] == "user":
            result.append({"role": "user", "content": m["content"][:300]})
        else:
            content = m["content"][:800]
            # chart_data에서 피크 수치를 한 줄 요약으로 추가 (Method A)
            for chart in m.get("chart_data") or []:
                idx = chart.get("index", [])
                for sname, vals in (chart.get("series") or {}).items():
                    if idx and vals:
                        peak_i = max(range(len(vals)), key=lambda i: vals[i])
                        content += (f" [분석요약-{chart.get('title', '')}:"
                                    f" {sname} 최대 {idx[peak_i]} = {vals[peak_i]:,}]")
            result.append({"role": "assistant", "content": content[:1100]})
    return result


def call_stream(question: str,
                conversation_history: list[dict] | None = None,
                previous_festival_context: dict | None = None,
                previous_analysis_context: list[dict] | None = None):
    """
    /query/stream SSE에서 meta와 텍스트 청크를 분리.
    Returns: (generator, meta_dict)
    """
    meta = {}

    def _gen():
        with requests.post(
            f"{API_URL}/query/stream",
            json={
                "question": question,
                "conversation_history": conversation_history,
                "previous_festival_context": previous_festival_context,
                "previous_analysis_context": previous_analysis_context,
            },
            stream=True,
            timeout=120,
        ) as resp:
            for raw in resp.iter_lines():
                if not raw:
                    continue
                line = raw.decode("utf-8") if isinstance(raw, bytes) else raw
                if not line.startswith("data:"):
                    continue
                data_str = line[5:].strip()
                if data_str == "[DONE]":
                    break
                try:
                    chunk = json.loads(data_str)
                    if chunk["type"] == "meta":
                        meta.update(chunk)
                    elif chunk["type"] == "chunk":
                        yield chunk["text"]
                except json.JSONDecodeError:
                    continue

    return _gen(), meta


# ── 질문 처리 ─────────────────────────────────────────────────────────────────
question = st.chat_input("질문을 입력하세요...")

if "pending_question" in st.session_state:
    question = st.session_state.pop("pending_question")

if question:
    st.session_state.messages.append({"role": "user", "content": question})

    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        with st.spinner("분석 중..."):
            history = _build_conversation_history(st.session_state.messages)
            prev_ctx = st.session_state.last_meta.get("festival_context")
            prev_analysis = st.session_state.last_analysis_context
            gen, meta = call_stream(question,
                                    conversation_history=history,
                                    previous_festival_context=prev_ctx,
                                    previous_analysis_context=prev_analysis or None)

        full_answer = st.write_stream(gen)

        render_intent_badge(meta.get("intent"))
        render_charts(meta.get("chart_data"))

        if meta.get("sql_list"):
            with st.expander("🔍 실행된 SQL"):
                for sql in meta["sql_list"]:
                    st.code(sql, language="sql")

    st.session_state.messages.append({
        "role": "assistant",
        "content": full_answer,
        "intent": meta.get("intent"),
        "sql_list": meta.get("sql_list"),
        "chart_data": meta.get("chart_data"),
    })

    st.session_state.last_meta = meta
    # 분석 컨텍스트 갱신 (통계 분석인 경우에만) (Method B)
    if meta.get("intent") == "통계_분석":
        st.session_state.last_analysis_context = _extract_analysis_context(meta.get("chart_data"))
    else:
        st.session_state.last_analysis_context = []
    st.rerun()
