"""
개선된 LLM 기반 Text-to-SQL 챗봇 (옵션 3: 개선된 하이브리드)

주요 개선사항:
1. ✅ INTENT_TABLE_MAP 제거 - LLM이 88개 테이블 중 직접 선택
2. ✅ 키워드 매칭 제거 - LLM이 질문 의도 분석
3. ✅ LLM이 핵심 결정 담당 (테이블 선택, SQL 생성)
4. ✅ 코드는 보안 검증만 (WHERE 조건, SQL Injection 방지)

기존 text2sql.py와 비교:
- 기존: 규칙 기반 + LLM 보조 (하드코딩 많음)
- 개선: LLM 중심 + 최소한의 검증 (유연성 향상)
"""

import os
from sqlalchemy import create_engine, inspect, text
from langchain_google_genai import ChatGoogleGenerativeAI
import pandas as pd
from dotenv import load_dotenv
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

DB_SCHEMA = "regionmonitor"

# ❌ 제거: INTENT_TABLE_MAP (하드코딩)
# ✅ 개선: LLM이 전체 테이블 목록에서 직접 선택

# ✅ 유지: ADMI_CD 테이블 목록 (보안 검증용)
# LLM이 선택한 테이블이 어떤 기준 키를 사용하는지 검증
ADMI_CD_TABLES = {
    "tb_cnsmp_amount",
    "tb_tmzon_cnsmp_amount",
    "tb_dayt_popltn",
    "tb_tmzon_dayt_popltn",
    "tb_reside_popltn",
    "tb_wrc_popltn",
    "tb_income_amount",
    "tb_bsnes_info",
}


class ImprovedTextToSQL:
    def __init__(self):
        db_uri = (
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        try:
            self.engine = create_engine(
                db_uri,
                connect_args={"options": f"-csearch_path={DB_SCHEMA},public"}
            )
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ PostgreSQL 연결 성공!")
        except Exception as e:
            print(f"❌ DB 연결 실패: {e}")
            raise

        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash-lite-preview-09-2025",
            temperature=0,
            google_api_key=os.getenv("GOOGLE_API_KEY")
        )
        self.column_definitions = self._load_column_definitions()
        self.schema_info = self._get_schema_info()
        print(f"✨ [개선된 버전] LLM이 {len(self.schema_info['details'])}개 테이블 중 직접 선택합니다")
        print(f"📖 컬럼 정의서: {len(self.column_definitions)}개 테이블\n")

    # ──────────────────────────────────────────────
    # 초기화
    # ──────────────────────────────────────────────

    def _load_column_definitions(self):
        try:
            with open('column_definitions.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
            return {t['table_name'].lower(): t for t in data if 'table_name' in t}
        except Exception as e:
            print(f"⚠️  컬럼 정의서 로드 실패: {e}")
            return {}

    def _get_schema_info(self):
        inspector = inspect(self.engine)
        tables = inspector.get_table_names(schema=DB_SCHEMA)
        schema = {'tables': tables, 'details': {}}
        for table in tables:
            try:
                columns = inspector.get_columns(table, schema=DB_SCHEMA)
                pks = inspector.get_pk_constraint(table, schema=DB_SCHEMA)
                schema['details'][table] = {
                    'columns': [c['name'] for c in columns],
                    'types': {c['name']: str(c['type']) for c in columns},
                    'primary_keys': pks.get('constrained_columns', []),
                }
            except Exception:
                pass
        return schema

    # ──────────────────────────────────────────────
    # 컬럼 정의서 조회
    # ──────────────────────────────────────────────

    def _get_table_def(self, table):
        return self.column_definitions.get(table.lower())

    def _get_col_kr(self, table, col):
        td = self._get_table_def(table)
        if not td:
            return ""
        ci = next((c for c in td.get('columns', [])
                   if c.get('column_name', '').lower() == col.lower()), None)
        return ci.get('column_name_kr', '') if ci else ''

    def _get_table_kr(self, table):
        td = self._get_table_def(table)
        return td.get('table_name_kr', '') if td else ''

    def _format_table_schema(self, table):
        info = self.schema_info['details'].get(table)
        if not info:
            return ""
        kr = self._get_table_kr(table)
        lines = [f'📌 "{DB_SCHEMA}"."{table}"  ({kr})']
        if info['primary_keys']:
            lines.append(f"   PK: {', '.join(info['primary_keys'])}")
        for col, col_type in info['types'].items():
            col_kr = self._get_col_kr(table, col)
            suffix = f"  → {col_kr}" if col_kr else ""
            lines.append(f"   • {col} ({col_type}){suffix}")
        lines.append("")
        return "\n".join(lines)

    def _get_all_tables_summary(self) -> str:
        """전체 테이블 목록을 요약 형태로 반환 (LLM에게 제공용)"""
        summary = []

        # 테이블 분류
        metadata_tables = ['tb_analysis_report', 'tb_analysis_report_schedule', 'suwon', 'suwon_20251118']
        visit_tables = [t for t in self.schema_info['tables'] if 'visit_popltn' in t]
        selng_tables = [t for t in self.schema_info['tables'] if 'selng' in t and 'visit_popltn' not in t]
        other_tables = [t for t in self.schema_info['tables']
                       if t not in metadata_tables and t not in visit_tables and t not in selng_tables]

        # 1. 메타데이터 테이블 (통계 분석 금지)
        if metadata_tables:
            summary.append("\n[메타데이터 테이블 - 통계 분석 사용 금지]")
            for table in metadata_tables:
                if table in self.schema_info['details']:
                    kr = self._get_table_kr(table)
                    summary.append(f"  ❌ {table} ({kr}) - 축제 기본정보 전용")

        # 2. 방문인구 테이블
        if visit_tables:
            summary.append("\n[방문인구 테이블 - 방문객/방문인구 질문에 사용]")
            for table in visit_tables:
                kr = self._get_table_kr(table)
                info = self.schema_info['details'].get(table, {})
                # 방문인구 관련 컬럼만 표시
                vipop_cols = [c for c in info.get('columns', []) if 'vipop' in c][:5]
                col_preview = ', '.join(vipop_cols) if vipop_cols else '...'
                summary.append(f"  ✅ {table} ({kr}) - {col_preview}")

        # 3. 매출 테이블
        if selng_tables:
            summary.append("\n[매출/소비금액 테이블 - 매출/소비 질문에 사용]")
            for table in selng_tables:
                kr = self._get_table_kr(table)
                info = self.schema_info['details'].get(table, {})
                # 매출 관련 컬럼만 표시
                selng_cols = [c for c in info.get('columns', []) if 'selng' in c or 'amount' in c][:5]
                col_preview = ', '.join(selng_cols) if selng_cols else '...'
                summary.append(f"  ✅ {table} ({kr}) - {col_preview}")

        # 4. 기타 테이블
        if other_tables:
            summary.append("\n[기타 테이블]")
            for table in other_tables[:20]:  # 너무 많으면 20개만
                kr = self._get_table_kr(table)
                info = self.schema_info['details'].get(table, {})
                cols = info.get('columns', [])[:3]
                col_preview = ', '.join(cols)
                summary.append(f"  • {table} ({kr}) - {col_preview}")

        return "\n".join(summary)

    # ──────────────────────────────────────────────
    # STEP 1: 축제 컨텍스트 추출 (LLM 활용)
    # ──────────────────────────────────────────────

    def _classify_question_intent(self, question: str) -> str:
        """
        ✅ 개선: 키워드 매칭 제거, LLM이 직접 분류
        """
        prompt = f"""다음 질문의 의도를 분류하세요.

질문: {question}

분류 기준:
- "축제_목록": 축제 개수/갯수/몇 개, 축제 목록/리스트, 전체 축제, 어떤 축제들, 진행한 축제 등 여러 축제 정보 요청
  ⚠️ "몇 개", "갯수", "개수", "몇 곳", "몇 건"이 포함된 질문은 반드시 "축제_목록"
- "축제_정보": 특정 축제의 기간, 장소, 주최 등 기본 정보 요청
- "통계_분석": 방문인구, 매출, 연령대, 성별, 시간대 등 데이터 분석 요청

예시:
- "수원 축제 개수를 알려줘" → "축제_목록"
- "올해 진행한 축제 갯수를 알려줘" → "축제_목록"
- "축제가 몇 개야?" → "축제_목록"
- "2025년 축제 몇 곳이야?" → "축제_목록"
- "수원에서 열리는 축제 리스트" → "축제_목록"
- "올해 진행한 축제" → "축제_목록"
- "수원 축제 정보 알려줘" → "축제_정보"
- "수원축제 2025 방문인구" → "통계_분석"

"축제_목록", "축제_정보", "통계_분석" 중 하나만 반환:"""

        response = self.llm.invoke(prompt)
        intent = response.content.strip()

        # 검증 (안전장치)
        if intent not in ["축제_목록", "축제_정보", "통계_분석"]:
            return "통계_분석"  # 기본값
        return intent

    def _answer_festival_info(self, festival_ctx: dict) -> str:
        """TB_ANALYSIS_REPORT의 축제 컨텍스트를 자연어로 설명"""
        from datetime import datetime

        try:
            start_date = datetime.strptime(festival_ctx['event_bgnde'], '%Y%m%d')
            end_date = datetime.strptime(festival_ctx['event_endde'], '%Y%m%d')
            start_str = start_date.strftime('%Y년 %m월 %d일')
            end_str = end_date.strftime('%Y년 %m월 %d일')
            duration = (end_date - start_date).days + 1
        except:
            start_str = festival_ctx['event_bgnde']
            end_str = festival_ctx['event_endde']
            duration = "?"

        info = f"""
축제명: {festival_ctx['event_nm']}
행사 기간: {start_str} ~ {end_str} ({duration}일간)
장소: {festival_ctx.get('event_plc', '정보 없음')}
지역: {festival_ctx.get('sido_nm', '')} {festival_ctx.get('cty_nm', '')}
"""

        if festival_ctx.get('event_site'):
            info += f"홈페이지: {festival_ctx['event_site']}\n"
        if festival_ctx.get('evnet_dc'):
            info += f"축제 설명: {festival_ctx['evnet_dc']}\n"
        if festival_ctx.get('event_auspc'):
            info += f"주최/주관: {festival_ctx['event_auspc']}\n"

        prompt = f"""다음 축제 정보를 바탕으로 사용자에게 친절하고 자연스럽게 설명해주세요.

{info}

[작성 규칙]
1. 축제명을 굵게 표시하고 간단한 소개로 시작
2. 기간, 장소, 주최 등을 읽기 쉽게 정리
3. 축제 설명이 있으면 포함
4. 홈페이지가 있으면 링크 제공
5. 마지막에 "더 자세한 통계 정보가 필요하시면 물어보세요" 같은 안내 추가
6. 간결하고 친근하게 (3-5문단 정도)

답변:"""

        return self.llm.invoke(prompt).content.strip()

    def _answer_festival_list(self, festivals_df, question: str,
                              search_year: str = None, search_region: str = None) -> str:
        """검색된 축제 목록을 마크다운 표로 직접 포맷 (LLM 요약 없이 전체 출력)"""
        from datetime import datetime

        total_count = len(festivals_df)

        # 헤더 구성
        conditions = []
        if search_year:
            conditions.append(f"{search_year}년")
        if search_region:
            conditions.append(search_region)
        cond_str = " · ".join(conditions)
        header = f"### {cond_str + ' ' if cond_str else ''}축제 목록 ({total_count}개)\n\n"

        # 연도별 그룹화
        festivals_by_year: dict[str, list] = {}
        for _, row in festivals_df.iterrows():
            year = str(row['event_bgnde'])[:4]
            festivals_by_year.setdefault(year, []).append(row)

        # 연도별 마크다운 테이블 생성
        body = ""
        for year in sorted(festivals_by_year.keys(), reverse=True):
            rows = festivals_by_year[year]
            body += f"**{year}년** ({len(rows)}개)\n\n"
            body += "| # | 축제명 | 기간 | 지역 |\n"
            body += "|---|-------|------|------|\n"
            for i, row in enumerate(rows, 1):
                name = row.get('event_nm', '')
                try:
                    start = datetime.strptime(str(row['event_bgnde']), '%Y%m%d').strftime('%m/%d')
                    end   = datetime.strptime(str(row['event_endde']), '%Y%m%d').strftime('%m/%d')
                    period = f"{start}~{end}" if start != end else start
                except Exception:
                    period = f"{row['event_bgnde']}~{row['event_endde']}"
                sido = row.get('sido_nm', '') or ''
                cty  = row.get('cty_nm', '') or ''
                region = f"{sido} {cty}".strip()
                body += f"| {i} | {name} | {period} | {region} |\n"
            body += "\n"

        footer = "\n> 특정 축제의 상세 정보나 통계가 필요하면 축제명을 말씀해주세요."
        return header + body + footer

    def _extract_festival_context(self, question: str,
                                   conversation_history: list[dict] | None = None,
                                   previous_festival_context: dict | None = None) -> dict | None:
        """
        LLM이 질문에서 지역명·연도·특정날짜 추출 후
        코드에서 SQL 생성하여 tb_analysis_report 조회
        """
        from datetime import datetime
        current_year = datetime.now().year

        # 대화 기록 섹션 구성
        history_section = ""
        if conversation_history:
            lines = []
            for msg in conversation_history[-6:]:  # 최근 3턴
                role_kr = "사용자" if msg["role"] == "user" else "어시스턴트"
                content = msg["content"][:200]
                lines.append(f"  {role_kr}: {content}")
            history_section = "\n[이전 대화 기록]\n" + "\n".join(lines) + "\n"

        # 직전 축제 힌트 구성
        festival_hint = ""
        if previous_festival_context and previous_festival_context.get("event_nm"):
            nm   = previous_festival_context.get("event_nm", "")
            sido = previous_festival_context.get("sido_nm", "") or ""
            cty  = previous_festival_context.get("cty_nm", "") or ""
            bgnde = previous_festival_context.get("event_bgnde", "") or ""
            endde = previous_festival_context.get("event_endde", "") or ""
            festival_hint = f"\n[직전에 언급된 축제]\n  {nm} ({sido} {cty} / {bgnde}~{endde})\n"

        extract_prompt = f"""이전 대화 맥락을 참고하여 현재 질문에서 검색 키워드(지역명 또는 축제명)와 날짜 정보를 추출하세요.
{history_section}{festival_hint}
[현재 질문]
{question}

[추출 규칙]
1. region: 지역명 또는 축제명 중 DB 검색에 가장 유용한 키워드를 추출
   - 지역명 예시: "수원", "화성시", "서울"
   - 축제명 예시: "정조대왕 능행차", "화성문화제", "수원화성문화제"
   - 둘 다 있으면 더 구체적인 것(축제명 우선)
   - ⚠️ "수원축제", "서울축제" 처럼 [지역명+축제/행사] 형태는 일상어이며 DB 축제명이 아님
     → 지역명만 추출 (예: "수원축제" → "수원", "서울행사" → "서울")
2. 질문에 구체적인 연도가 있으면 year에 추출
   - "올해", "이번 해" → "{current_year}"
   - "작년", "지난해" → "{current_year - 1}"
   - "재작년", "2년 전" → "{current_year - 2}"
3. "최근에", "가장 최근", "요즘" 등은 연도를 특정하지 않는 표현 → year: null
4. "이 축제", "그 행사", "해당 축제" 등 지시어가 있으면:
   - [직전에 언급된 축제]가 있으면 그 축제명 또는 지역명을 region으로, year는 null로 추출
   - [직전에 언급된 축제]가 없으면 region, year 모두 null
5. 이전 대화에서 이어지는 질문이면 이전 대화의 축제/지역 정보를 활용

[의도 분류 기준]
- "축제_목록": 축제 개수/갯수, 목록/리스트, 어떤 축제들 요청
- "축제_정보": 특정 축제의 기간/장소/주최 등 기본 정보 요청
- "통계_분석": 방문인구/매출/연령대/성별/시간대 등 데이터 분석 요청 (기본값)

JSON 형식으로만 반환 (다른 텍스트 없이):
{{"region": "지역명 또는 축제명 또는 null", "year": "연도 4자리 또는 null", "specific_date": "YYYYMMDD 또는 null", "month": "MM 또는 null", "intent": "축제_목록|축제_정보|통계_분석"}}

예시:
- "정조대왕 능행차 성별 소비금액" → {{"region": "정조대왕 능행차", "year": null, "specific_date": null, "month": null, "intent": "통계_분석"}}
- "수원축제 2025년 시간대별 방문인구" → {{"region": "수원", "year": "2025", "specific_date": null, "month": null, "intent": "통계_분석"}}
- "2025 수원화성문화제 방문인구" → {{"region": "수원화성문화제", "year": "2025", "specific_date": null, "month": null, "intent": "통계_분석"}}
- "최근에 수원에서 진행한 축제" → {{"region": "수원", "year": null, "specific_date": null, "month": null, "intent": "축제_목록"}}
- "이 축제 20대 방문인구" (직전 축제: 정조대왕 능행차, 수원시) → {{"region": "정조대왕 능행차", "year": null, "specific_date": null, "month": null, "intent": "통계_분석"}}
- "2025년 화성문화제 매출" → {{"region": "화성문화제", "year": "2025", "specific_date": null, "month": null, "intent": "통계_분석"}}
- "25.10.10 방문객" → {{"region": null, "year": "2025", "specific_date": "20251010", "month": "10", "intent": "통계_분석"}}
- "수원 축제 몇 개야?" → {{"region": "수원", "year": null, "specific_date": null, "month": null, "intent": "축제_목록"}}
- "수원축제 기간 알려줘" → {{"region": "수원", "year": null, "specific_date": null, "month": null, "intent": "축제_정보"}}"""

        resp = self.llm.invoke(extract_prompt)
        raw = resp.content.strip().replace('```json', '').replace('```', '').strip()

        try:
            extracted = json.loads(raw)
            def _to_none(v):
                return None if v in (None, 'null', 'NULL', '') else v
            region = _to_none(extracted.get('region'))
            year = _to_none(extracted.get('year'))
            specific_date = _to_none(extracted.get('specific_date'))
            month = _to_none(extracted.get('month'))
            raw_intent = extracted.get('intent', '통계_분석')
            intent = raw_intent if raw_intent in ["축제_목록", "축제_정보", "통계_분석"] else "통계_분석"
        except Exception:
            region, year, specific_date, month = None, None, None, None
            intent = "통계_분석"

        print(f"   🔎 추출된 키워드 → 지역: {region}, 연도: {year}, 특정날짜: {specific_date}, 월: {month}")

        if not region and not year and not specific_date:
            if previous_festival_context:
                print(f"   ♻️  추출 실패 → 이전 축제 컨텍스트 재사용: [{previous_festival_context.get('event_nm')}]")
                return previous_festival_context
            return None

        # ✅ 개선: 축제 데이터 질문인지 확인
        # 지역명만 있고 축제 관련 키워드가 없으면 일반 질문으로 간주
        festival_keywords = [
            '축제', '행사', '문화제', '능행차', '미디어아트', '드론',  # 축제명
            '방문', '방문인구', '방문객', '유동인구',  # 방문인구
            '매출', '소비', '판매', '거래', '결제', '지출',  # 매출/소비
            '데이터', '분석', '통계', '수치', '현황',  # 데이터 분석
            '연령', '나이', '세대', '20대', '30대', '40대', '50대', '60대',  # 연령대
            '성별', '남', '여', '남성', '여성',  # 성별
            '시간', '시간대', '오전', '오후',  # 시간대
        ]

        has_festival_keyword = any(keyword in question for keyword in festival_keywords)

        if not has_festival_keyword:
            print(f"   ℹ️  축제 관련 키워드 없음 → 일반 질문으로 판단")
            return None

        # "수원축제" 같이 [지역명+일반축제어] 조합이면 접미어 제거 후 지역명만 사용
        # LLM이 올바르게 추출하지 못한 경우의 코드 레벨 안전장치
        _FESTIVAL_SUFFIXES = ['문화제', '대축제', '축제', '행사', '페스티벌']
        def _strip_festival_suffix(keyword: str) -> str | None:
            for suffix in _FESTIVAL_SUFFIXES:
                if keyword.endswith(suffix) and len(keyword) - len(suffix) >= 2:
                    return keyword[:-len(suffix)]
            return None

        def _build_sql(search_region):
            conds = []
            if search_region:
                parts = [p for p in search_region.split() if len(p) >= 2]
                if not parts:
                    parts = [search_region]
                clauses = [
                    f"(event_nm LIKE '%{p}%' OR reprt_nm LIKE '%{p}%' "
                    f"OR event_plc LIKE '%{p}%' OR cty_nm LIKE '%{p}%' "
                    f"OR sido_nm LIKE '%{p}%')"
                    for p in parts
                ]
                conds.append("(" + " OR ".join(clauses) + ")")
            if specific_date:
                conds.append(f"event_bgnde <= '{specific_date}' AND event_endde >= '{specific_date}'")
            elif year:
                conds.append(f"event_bgnde LIKE '{year}%'")
            if not conds:
                return None
            return f"""
SELECT reprt_id, event_nm, region_cd, admi_cd,
       sido_nm, cty_nm, event_bgnde, event_endde, event_plc,
       event_site, evnet_dc, event_auspc
FROM "{DB_SCHEMA}"."tb_analysis_report"
WHERE {" AND ".join(conds)}
ORDER BY event_bgnde DESC, reprt_id DESC
"""

        sql = _build_sql(region)
        if not sql:
            return None

        print(f"   📋 실행 SQL: {sql.strip()}")
        result = self.execute_query(sql)

        if not result['success']:
            print(f"   ⚠️  SQL 오류: {result['error']}")
            return None

        # 0행이고 region에 축제 접미어가 있으면 접미어 제거 후 재시도
        # 예: "수원축제" → "수원" 으로 재검색
        if result['rows'] == 0 and region:
            stripped = _strip_festival_suffix(region)
            if stripped:
                print(f"   🔄 0행 → '{region}' 접미어 제거 후 '{stripped}'로 재시도")
                sql2 = _build_sql(stripped)
                if sql2:
                    result2 = self.execute_query(sql2)
                    if result2['success'] and result2['rows'] > 0:
                        region = stripped
                        result = result2
                        sql = sql2
                        print(f"   📋 재시도 SQL: {sql2.strip()}")

        if result['rows'] == 0:
            print("   ⚠️  조건에 맞는 축제가 없습니다.")
            # 연도 검색인데 결과 없으면 빈 결과를 담은 컨텍스트 반환 (fallback 방지)
            if year and not region and not specific_date:
                return {'_empty': True, 'year': year, 'all_festivals': result['data'], '_intent': intent}
            return None

        if result['rows'] > 1:
            print(f"\n   ℹ️  {result['rows']}개 축제 검색됨 (전체 목록):")
            print(result['data'][['reprt_id', 'event_nm', 'event_bgnde', 'event_endde']].to_string(index=False))

            # "최근" 유형 질문은 날짜순 첫 번째 항목 사용
            if any(kw in question for kw in ['최근', '최신', '가장 최근', '마지막', '요즘']):
                row = result['data'].iloc[0]
                print(f"\n   → 최신 날짜 기준 선택: [{row['event_nm']}] (event_bgnde={row['event_bgnde']})\n")
            else:
                # 이전 축제 힌트를 LLM에게 제공해서 연속 질문 vs 새 질문을 스스로 판단
                row = self._pick_best_festival(question, result['data'], previous_festival_context)
                print(f"\n   → LLM 선택: [{row['event_nm']}] (reprt_id={int(row['reprt_id'])})\n")
        else:
            row = result['data'].iloc[0]

        ctx = {k: row.get(k) for k in [
            'reprt_id', 'event_nm', 'region_cd', 'admi_cd',
            'sido_nm', 'cty_nm', 'event_bgnde', 'event_endde', 'event_plc',
            'event_site', 'evnet_dc', 'event_auspc'
        ]}
        ctx['specific_date'] = specific_date
        ctx['all_festivals'] = result['data']  # ✅ 전체 축제 목록도 저장
        ctx['_intent'] = intent

        date_info = f"특정 날짜: {specific_date}" if specific_date else f"기간: {ctx['event_bgnde']}~{ctx['event_endde']}"
        print(f"   ✅ 축제: [{ctx['event_nm']}] | "
              f"{date_info} | "
              f"REGION_CD: {ctx['region_cd']} | ADMI_CD: {ctx['admi_cd']}")
        return ctx

    def _pick_best_festival(self, question: str, df, previous_festival_context: dict | None = None) -> dict:
        """여러 축제 후보 중 질문과 가장 관련성 높은 것을 LLM이 선택"""
        candidates = df[['reprt_id', 'event_nm', 'event_bgnde', 'event_endde']].to_string(index=False)

        prev_hint = ""
        if previous_festival_context and previous_festival_context.get("event_nm"):
            prev_hint = f"""
[직전 대화에서 언급된 축제]
축제명: {previous_festival_context.get('event_nm')} (reprt_id={int(previous_festival_context.get('reprt_id', 0))})

[직전 축제 활용 규칙]
- 질문에 "이 축제", "그 축제", "위 축제", "해당 축제" 등 지시어만 있을 경우 → 직전 축제의 reprt_id 선택
- 질문에 새로운 축제명이 구체적으로 명시된 경우 → 해당 축제명과 가장 유사한 것 선택 (직전 축제 무시)
"""

        prompt = f"""다음 축제 목록 중 아래 질문에 가장 적합한 축제의 reprt_id를 숫자만 반환하세요.

[질문]
{question}
{prev_hint}
[축제 목록]
{candidates}

[선택 기준 - 우선순위 순]
1. 질문에 특정 축제명이 명시된 경우 → 해당 축제명과 가장 유사한 것 선택
2. 지시어만 있고 직전 축제가 있는 경우 → 직전 축제의 reprt_id 선택
3. "수원축제 2025년도", "수원축제 2025년" 처럼 연도만 있고 구체적 축제명이 없는 경우 → "수원시 3대 축제" 선택
4. 질문에 특정 장소(화성행궁, 행궁광장 등)가 명시된 경우 → 그 장소 관련 축제 선택
5. 질문이 "축제 전체" 또는 포괄적인 경우 → "연계포함" 또는 범위가 넓은 축제 선택
6. 단순 지역명만 있는 경우(수원시, 수원역인근 등)보다 구체적 축제명 우선
7. 분석 요청(상권, 영역, 구간 등 지역 분석)은 해당 지역/상권 관련 항목 선택

reprt_id 숫자만 반환 (다른 텍스트 없이):"""
        response = self.llm.invoke(prompt)
        try:
            chosen_id = float(response.content.strip())
            row = df[df['reprt_id'] == chosen_id].iloc[0]
        except Exception:
            row = df.iloc[0]
        return row

    # ──────────────────────────────────────────────
    # STEP 2: 질문 분해 → 필요한 테이블 목록 결정 (✅ 개선)
    # ──────────────────────────────────────────────

    def _decompose_question(self, question: str) -> list[str]:
        """
        ✅ 개선: INTENT_TABLE_MAP 제거
        LLM이 88개 테이블 목록에서 직접 필요한 테이블 선택
        """
        # 전체 테이블 목록 요약
        tables_summary = self._get_all_tables_summary()

        prompt = f"""당신은 데이터베이스 전문가입니다.

[질문]
{question}

[사용 가능한 테이블 목록 ({len(self.schema_info['tables'])}개)]
{tables_summary}

[테이블 선택 규칙]
1. ⚠️ **절대 선택 금지 테이블:**
   - tb_analysis_report : 축제 메타데이터 전용 (통계 데이터 없음)
   - tb_cnsmp_amount, tb_tmzon_cnsmp_amount : 데이터 갱신 중단
   - 날짜 접미어가 붙은 테이블 (예: tb_agrde_selng_20260223, tb_visit_popltn_20251201 등)
     → 이름에 8자리 날짜(YYYYMMDD)가 포함된 테이블은 임시/백업 테이블이므로 선택 금지
     → 날짜 접미어 없는 기본 테이블(tb_agrde_selng, tb_agrde_visit_popltn 등)을 사용할 것

2. **질문 유형별 필수 테이블:**
   - "방문인구" 또는 "방문객" 질문 → 반드시 *_visit_popltn 계열 선택
   - "매출" 또는 "소비금액" 질문 → 반드시 *_selng 계열 선택

3. **테이블명 패턴:**
   - tb_agrde_* : 연령대별 (10대, 20대, 30대...)
   - tb_sexdstn_* : 성별 (남성, 여성)
   - tb_tmzon_* : 시간대별 (0시~23시)
   - tb_nation_* : 내/외국인
   - tb_inflow_* : 유입지 (어디서 왔는지)
   - *_visit_popltn : 방문인구 수치
   - *_selng : 매출/소비금액 수치
   - *_dayt_popltn : 생활인구
   - *_reside_popltn : 거주인구
   - *_wrc_popltn : 직장인구

4. **복합 조건 처리:**
   - 연령대 + 방문인구 → tb_agrde_visit_popltn
   - 성별 + 매출 → tb_sexdstn_selng
   - 시간대 + 방문인구 → tb_tmzon_visit_popltn
   - 여러 조건 → 각각의 테이블 모두 선택

[예시]
- 질문: "방문인구는?" → tb_agrde_visit_popltn
- 질문: "20대 방문인구는?" → tb_agrde_visit_popltn
- 질문: "시간대별 방문인구는?" → tb_tmzon_visit_popltn
- 질문: "남녀 매출은?" → tb_sexdstn_selng

필요한 테이블명을 쉼표로 구분해서 반환 (설명 없이 테이블명만):
예시) tb_sexdstn_visit_popltn,tb_tmzon_selng"""

        response = self.llm.invoke(prompt)
        raw = response.content.strip()

        # 응답 파싱
        tables = [t.strip() for t in raw.split(',') if t.strip()]

        # 검증: 실제 존재하는 테이블만 필터링 + 날짜 접미어 임시 테이블 제외
        valid = [
            t for t in tables
            if t in self.schema_info['details']
            and not re.search(r'_\d{8}$', t)  # _20260223 같은 날짜 접미어 테이블 제외
        ]

        if valid:
            print(f"   ✅ LLM이 선택한 테이블: {', '.join(valid)}")
        else:
            print("   ⚠️  LLM이 테이블 선택 실패 → 재시도")
            # 재시도 로직 추가 가능

        return valid

    # ──────────────────────────────────────────────
    # STEP 3: 테이블별 SQL 생성 (LLM)
    # ──────────────────────────────────────────────

    def _generate_sql_per_table(self, question: str, table: str,
                                festival_ctx: dict) -> str:
        """
        하나의 테이블에 대해 SQL 생성 (LLM)
        """
        table_schema = self._format_table_schema(table)
        date_from = festival_ctx['event_bgnde']
        date_to = festival_ctx['event_endde']
        specific_date = festival_ctx.get('specific_date')

        # 변화량 관련 키워드 감지
        needs_change = any(keyword in question for keyword in
                           ['변화량', '증감', '증가율', '감소율', '전일대비', '전날', '비교', '추이'])

        # 시간대별 전체 합계가 필요한지 감지
        needs_time_aggregation = '시간대' in question and not needs_change and not specific_date

        # 날짜 조건 생성
        has_date_range = date_from is not None and date_to is not None
        if needs_change:
            date_condition = f"stdr_ymd BETWEEN '{date_from}' AND '{date_to}'" if has_date_range else "1=1"
            date_desc = f"행사 기간: {date_from} ~ {date_to} (일별 데이터 조회)" if has_date_range else "날짜 조건 없음"
            group_by_hint = "일별 데이터를 위해 SELECT에 stdr_ymd 포함하고 GROUP BY stdr_ymd, ORDER BY stdr_ymd 사용"
        elif needs_time_aggregation:
            date_condition = f"stdr_ymd BETWEEN '{date_from}' AND '{date_to}'" if has_date_range else "1=1"
            date_desc = f"행사 기간: {date_from} ~ {date_to} (전체 기간 합계)" if has_date_range else "날짜 조건 없음"
            group_by_hint = "전체 기간을 합산하여 시간대별 총합만 계산 (stdr_ymd 없이 SUM만 사용, GROUP BY 없음)"
        elif specific_date:
            date_condition = f"stdr_ymd = '{specific_date}'"
            date_desc = f"특정 날짜: {specific_date}"
            group_by_hint = "단일 날짜 집계만 필요"
        else:
            date_condition = f"stdr_ymd BETWEEN '{date_from}' AND '{date_to}'" if has_date_range else "1=1"
            date_desc = f"행사 기간: {date_from} ~ {date_to}" if has_date_range else "날짜 조건 없음"
            group_by_hint = "전체 기간 데이터 조회"

        # 테이블 기준 키 분기
        if table in ADMI_CD_TABLES:
            filter_col = "admi_cd"
            filter_val = festival_ctx['admi_cd']
        else:
            filter_col = "region_cd"
            filter_val = festival_ctx['region_cd']

        prompt = f"""당신은 PostgreSQL 전문가입니다.

[테이블 스키마]
{table_schema}

[축제 컨텍스트]
- 축제명: {festival_ctx['event_nm']}
- {filter_col.upper()}: {filter_val}  ← 이 값으로 관심지역을 필터링합니다
- {date_desc}

[사용자 질문]
{question}

[SQL 규칙]
1. 테이블 참조: "{DB_SCHEMA}"."{table}"
2. WHERE 조건 필수:
   - {filter_col} = '{filter_val}'
   - {date_condition}
3. 질문에서 이 테이블과 관련된 조건만 추출해 SELECT/WHERE 작성
   (예: tb_tmzon_visit_popltn이면 t6_vipop ~ t23_vipop 컬럼 사용)
   (예: tb_tmzon_selng이면 t6_salamt ~ t23_salamt 컬럼 사용)
   (예: tb_sexdstn_visit_popltn이면 mvipop(남성), fvipop(여성) 컬럼 사용)
   (예: tb_sexdstn_selng이면 mdcnt, fdcnt, msalamt, fsalamt 컬럼 사용)
4. {group_by_hint}
5. 시간대별 전체 합계 조회 시:
   - SELECT SUM(t6_vipop) AS visitor_6h_cnt, SUM(t7_vipop) AS visitor_7h_cnt, ... SUM(t23_vipop) AS visitor_23h_cnt
   - 또는 SELECT SUM(t6_salamt) AS spending_6h_amt, SUM(t7_salamt) AS spending_7h_amt, ... SUM(t23_salamt) AS spending_23h_amt
   - GROUP BY 없이 전체 합산
   - stdr_ymd 포함하지 않음
6. 일별 데이터 조회 시:
   - SELECT stdr_ymd, SUM(컬럼) AS 별칭
   - GROUP BY stdr_ymd
   - ORDER BY stdr_ymd
   - **HAVING 절 사용 금지**
7. 단일 집계 시: SUM(컬럼)만 사용
8. 주석 없이 SQL만 반환

**중요:** WHERE 절로 기간을 필터링한 후, 적절한 GROUP BY 사용. HAVING으로 특정 날짜를 선택하지 마세요.

SQL:"""

        response = self.llm.invoke(prompt)
        return response.content.strip().replace('```sql', '').replace('```', '').strip()

    # ──────────────────────────────────────────────
    # SQL 실행 & 보안 검증
    # ──────────────────────────────────────────────

    def _validate_sql(self, sql: str, table: str, festival_ctx: dict) -> bool:
        """
        ✅ 추가: SQL 보안 검증 (SQL Injection 방지)
        """
        # 1. 위험한 키워드 차단
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
        sql_upper = sql.upper()
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                print(f"   ❌ 보안: 위험한 키워드 '{keyword}' 감지")
                return False

        # 2. WHERE 절 필수 검증 (region_cd 또는 admi_cd)
        if table in ADMI_CD_TABLES:
            required_key = "admi_cd"
            required_val = festival_ctx.get('admi_cd')
        else:
            required_key = "region_cd"
            required_val = festival_ctx.get('region_cd')

        if required_key not in sql.lower():
            print(f"   ❌ 보안: WHERE {required_key} 조건 누락")
            return False

        # 3. 테이블명 검증
        if f'"{DB_SCHEMA}"."{table}"' not in sql and f'{DB_SCHEMA}.{table}' not in sql:
            print(f"   ❌ 보안: 잘못된 테이블 참조")
            return False

        return True

    def execute_query(self, sql: str) -> dict:
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(sql), conn)
            return {'success': True, 'data': df, 'rows': len(df), 'columns': list(df.columns)}
        except Exception as e:
            return {'success': False, 'error': str(e), 'error_type': type(e).__name__}

    def _fix_sql(self, sql: str, error: str, table: str) -> str:
        """LLM이 오류난 SQL 자동 수정"""
        if table in ADMI_CD_TABLES:
            filter_hint = "admi_cd 조건 유지 (region_cd 아님)"
        else:
            filter_hint = "region_cd 조건 유지"

        prompt = f"""SQL 오류 수정:

원본 SQL:
{sql}

오류:
{error}

수정 규칙:
- 테이블 참조: "{DB_SCHEMA}"."{table}"
- {filter_hint}
- stdr_ymd는 'YYYYMMDD' 형식
- HAVING 절 사용 금지

수정된 SQL만 반환."""
        response = self.llm.invoke(prompt)
        return response.content.strip().replace('```sql', '').replace('```', '').strip()

    # ──────────────────────────────────────────────
    # STEP 3 병렬 처리용 헬퍼
    # ──────────────────────────────────────────────

    def _process_single_table(self, question: str, table: str,
                              festival_ctx: dict, show_sql: bool) -> dict | None:
        """테이블 1개에 대한 SQL 생성 → 검증 → 실행 (병렬 실행용)"""
        print(f"\n   📋 {table} ({self._get_table_kr(table)})")
        sql = self._generate_sql_per_table(question, table, festival_ctx)

        if not self._validate_sql(sql, table, festival_ctx):
            print(f"   ❌ 보안 검증 실패 → 스킵")
            return None

        if show_sql:
            print(f"   SQL: {sql}")

        result = self.execute_query(sql)
        if not result['success']:
            print(f"   ❌ 오류: {result['error']} → 자동 수정 시도...")
            fixed = self._fix_sql(sql, result['error'], table)

            if not self._validate_sql(fixed, table, festival_ctx):
                print(f"   ❌ 수정된 SQL도 보안 검증 실패 → 스킵")
                return None

            result = self.execute_query(fixed)
            sql = fixed

        if result['success']:
            print(f"   ✅ {result['rows']}행 조회 | 컬럼: {', '.join(result['columns'])}")
            if result['rows'] > 0:
                print(result['data'].to_string())
            return {'table': table, 'sql': sql, 'data': result['data'], 'rows': result['rows']}
        else:
            print(f"   ❌ 최종 실패: {result['error']}")
            return None

    # ──────────────────────────────────────────────
    # STEP 4: 다중 결과 통합 답변 생성
    # ──────────────────────────────────────────────

    def _build_combined_answer_prompt(self, question: str,
                                      results: list[dict],
                                      festival_ctx: dict,
                                      previous_analysis_context: list[dict] | None = None) -> str:
        """통합 답변용 프롬프트 생성 (invoke/stream 공통)"""
        from datetime import datetime, timedelta

        results_text = ""
        specific_date = festival_ctx.get('specific_date')
        needs_change = any(keyword in question for keyword in
                           ['변화량', '증감', '증가율', '감소율', '전일대비', '전날', '비교'])

        for r in results:
            table_kr = self._get_table_kr(r['table'])
            results_text += f"\n[{r['table']} / {table_kr}]\n"
            results_text += f"SQL: {r['sql']}\n"

            if needs_change and specific_date and 'stdr_ymd' in r['data'].columns:
                current_date = datetime.strptime(specific_date, '%Y%m%d')
                previous_date = (current_date - timedelta(days=1)).strftime('%Y%m%d')
                current_row = r['data'][r['data']['stdr_ymd'] == specific_date]
                previous_row = r['data'][r['data']['stdr_ymd'] == previous_date]

                results_text += f"\n[{specific_date} 데이터]\n"
                results_text += current_row.to_string(index=False) + "\n" if not current_row.empty else "데이터 없음\n"
                results_text += f"\n[{previous_date} 데이터 (전날 비교용)]\n"
                results_text += previous_row.to_string(index=False) + "\n" if not previous_row.empty else "데이터 없음\n"

                if not current_row.empty and not previous_row.empty:
                    results_text += "\n[증감율 계산 필수]\n"
                    results_text += f"각 지표별로 ({specific_date} 값 - {previous_date} 값) / {previous_date} 값 * 100 으로 계산하세요.\n"
            else:
                results_text += f"결과:\n{r['data'].to_string()}\n"

        date_info = f"특정 날짜 {specific_date}" if specific_date else f"기간 {festival_ctx['event_bgnde']} ~ {festival_ctx['event_endde']}"
        change_instruction = ""
        if needs_change and specific_date:
            change_instruction = """
[증감율 계산 지시사항 - 반드시 수행]
- 전날 대비 증감율을 각 지표별로 계산하여 표시하세요
- 증감율 공식: (당일 값 - 전일 값) / 전일 값 * 100
- 소수점 둘째자리까지 표시 (+12.44%, -0.54% 형식)
- 증가는 + 기호, 감소는 - 기호 사용
- 전날 데이터가 없는 지표는 "데이터 없음"으로 표시
"""
        # 이전 분석 참고 섹션 (연속 질문 지원)
        prev_analysis_section = ""
        if previous_analysis_context:
            lines = []
            for ac in previous_analysis_context:
                peak = ac.get('peak', {})
                lines.append(
                    f"  - {ac.get('title', '')} ({ac.get('series', '')}): "
                    f"최대 {peak.get('label', '')} = {peak.get('value', 0):,}, "
                    f"합계 = {ac.get('total', 0):,}"
                )
            prev_analysis_section = "\n[이전 분석 참고 - 연속 질문 시 활용]\n" + "\n".join(lines) + "\n"

        return f"""질문: {question}

축제: {festival_ctx['event_nm']} ({date_info})
{prev_analysis_section}
각 테이블 조회 결과:
{results_text}

[답변 작성 규칙]
1. 각 테이블 결과를 독립적인 수치로 명확하게 제시하세요.
2. 숫자는 천 단위 콤마(1,000,000) 또는 억/만 단위로 읽기 쉽게 표시
3. 마크다운 테이블 또는 리스트로 깔끔하게 정리
4. 시간대별 데이터는 6시~23시까지 모든 시간대를 빠짐없이 표시
5. 짧고 명확하게 (불필요한 주의/면책 문구 없이)
{change_instruction}

답변:"""

    def _generate_combined_answer(self, question: str,
                                  results: list[dict],
                                  festival_ctx: dict) -> str:
        prompt = self._build_combined_answer_prompt(question, results, festival_ctx)
        return self.llm.invoke(prompt).content.strip()

    # ──────────────────────────────────────────────
    # 차트 데이터 생성
    # ──────────────────────────────────────────────

    def _detect_chart(self, table: str, df) -> dict | None:
        """단일 쿼리 결과를 Streamlit 차트용 데이터로 변환"""
        import math

        def _safe_int(v):
            try:
                f = float(v)
                return 0 if math.isnan(f) else int(f)
            except Exception:
                return 0

        def _col_sum(col):
            return _safe_int(df[col].sum())

        def _extract_hour(col: str) -> int | None:
            """컬럼명에서 시간대 번호 추출 (원본·별칭 모두 지원)"""
            for pat in (r'^t(\d+)_', r'_(\d+)h(?:_|$)', r'hour[_]?(\d+)', r'(\d+)시'):
                m = re.search(pat, col)
                if m:
                    return int(m.group(1))
            return None

        def _extract_age(col: str) -> int | None:
            """컬럼명에서 연령대 번호 추출 (원본·별칭 모두 지원)"""
            for pat in (r'^a(\d+)_', r'age[_]?(\d+)', r'_(\d{2})[_]'):
                m = re.search(pat, col)
                if m:
                    age = int(m.group(1))
                    if age in (10, 20, 30, 40, 50, 60):
                        return age
            return None

        try:
            table_kr = self._get_table_kr(table)
            num_cols = [c for c in df.columns
                        if c != 'stdr_ymd'
                        and pd.api.types.is_numeric_dtype(df[c])
                        and df[c].notna().any()]
            if not num_cols:
                return None

            # Case 1: stdr_ymd 컬럼이 있고 행이 여러 개 → 일별 추이 line chart
            if 'stdr_ymd' in df.columns and len(df) > 1:
                df_sorted = df.sort_values('stdr_ymd')
                index = [str(d) for d in df_sorted['stdr_ymd']]
                series = {}
                for col in num_cols[:3]:
                    col_kr = self._get_col_kr(table, col) or col
                    series[col_kr] = [_safe_int(v) for v in df_sorted[col]]
                return {"table": table, "title": f"{table_kr} (일별 추이)",
                        "chart_type": "line", "index": index, "series": series}

            # Case 2: 시간대별 테이블 (tmzon)
            if 'tmzon' in table:
                hour_cols = [(h, c) for c in num_cols if (h := _extract_hour(c)) is not None]
                if hour_cols:
                    hour_cols.sort(key=lambda x: x[0])
                    index = [f"{h}시" for h, _ in hour_cols]
                    values = [_col_sum(c) for _, c in hour_cols]
                    first_col = hour_cols[0][1]
                    is_vipop = any(kw in first_col for kw in ('vipop', 'cnt', 'visit', 'popltn'))
                    label = "방문인구" if is_vipop else "매출 (원)"
                    return {"table": table, "title": table_kr, "chart_type": "bar",
                            "index": index, "series": {label: values}}

            # Case 3: 연령대별 테이블 (agrde)
            if 'agrde' in table:
                age_cols = [(a, c) for c in num_cols if (a := _extract_age(c)) is not None]
                if age_cols:
                    age_cols.sort(key=lambda x: x[0])
                    index = [f"{a}대" for a, _ in age_cols]
                    values = [_col_sum(c) for _, c in age_cols]
                    first_col = age_cols[0][1]
                    is_vipop = any(kw in first_col for kw in ('vipop', 'cnt', 'visit'))
                    label = "방문인구" if is_vipop else "매출 (원)"
                    return {"table": table, "title": table_kr, "chart_type": "bar",
                            "index": index, "series": {label: values}}

            # Case 4: 성별 테이블 (sexdstn)
            if 'sexdstn' in table:
                male_cols, female_cols = [], []
                for col in num_cols:
                    col_l = col.lower()
                    if col.startswith('m') or any(kw in col_l for kw in ('male', 'man', '남')):
                        male_cols.append(col)
                    elif col.startswith('f') or any(kw in col_l for kw in ('female', 'woman', '여')):
                        female_cols.append(col)
                if male_cols or female_cols:
                    all_gender_cols = male_cols + female_cols
                    is_vipop = any(kw in c for c in all_gender_cols
                                   for kw in ('vipop', 'cnt', 'visit'))
                    label = "방문인구" if is_vipop else "매출 (원)"
                    index, values = [], []
                    if male_cols:
                        index.append("남성")
                        values.append(sum(_col_sum(c) for c in male_cols))
                    if female_cols:
                        index.append("여성")
                        values.append(sum(_col_sum(c) for c in female_cols))
                    return {"table": table, "title": table_kr, "chart_type": "bar",
                            "index": index, "series": {label: values}}

            return None
        except Exception as e:
            print(f"   ⚠️ 차트 데이터 생성 실패 ({table}): {e}")
            return None

    def _make_chart_data(self, query_results: list[dict]) -> list[dict]:
        """쿼리 결과 목록을 차트 데이터 목록으로 변환"""
        charts = []
        for r in query_results:
            if r.get('data') is None or r['data'].empty:
                continue
            chart = self._detect_chart(r['table'], r['data'])
            if chart:
                charts.append(chart)
        return charts

    def _make_safe_ctx(self, festival_ctx: dict) -> dict:
        """festival_ctx를 JSON 직렬화 가능한 dict로 변환 (DataFrame 제거, NaN → None)"""
        import math
        result = {}
        for k, v in festival_ctx.items():
            if isinstance(v, pd.DataFrame):
                continue
            # float NaN (pandas/numpy NaN 포함) → None
            try:
                if isinstance(v, float) and math.isnan(v):
                    result[k] = None
                    continue
            except (TypeError, ValueError):
                pass
            # 그 외 JSON 직렬화 불가 타입 → str 변환
            try:
                json.dumps(v)
                result[k] = v
            except (TypeError, ValueError):
                result[k] = str(v)
        return result

    def _stream_combined_answer(self, question: str,
                                results: list[dict],
                                festival_ctx: dict,
                                previous_analysis_context: list[dict] | None = None):
        """통합 답변을 스트리밍으로 생성"""
        prompt = self._build_combined_answer_prompt(question, results, festival_ctx, previous_analysis_context)
        for chunk in self.llm.stream(prompt):
            if chunk.content:
                yield chunk.content

    # ──────────────────────────────────────────────
    # Fallback: DB에 없는 질문은 Gemini가 일반 답변
    # ──────────────────────────────────────────────

    def _generate_fallback_answer(self, question: str) -> str:
        """
        ✅ 개선: 범용 답변 가능 + 안전장치

        - 일반 상식, 인사 → 답변 O
        - 실시간 정보, 전문 조언 → 답변 X (거부)
        - 면책 문구 자동 추가
        """
        prompt = f"""당신은 축제 데이터 분석을 도와주는 친절한 AI 챗봇입니다.

[질문]
{question}

[상황]
데이터베이스에서 이 질문에 대한 답을 찾을 수 없습니다.

[답변 가이드]
✅ **답변 가능:**
   - 인사, 감사 표현
   - 일반 지식: 역사, 문화, 과학, 기술, 프로그래밍, 지리, 인물
   - 지역 기본 정보: 면적, 인구, 일반적 위치, 주소, 역사적 사실
     (예: 시청 위치, 관공서 주소, 유명 랜드마크 위치 - 일반 상식으로 답변 가능)
   - 챗봇 사용법, 축제 관련 지식

❌ **답변 불가:**
   - 실시간 정보: 날씨, 뉴스, 주가, 환율, 교통 상황, 정확한 실시간 GPS 좌표
   - 전문 조언: 의료, 법률, 재무, 투자
   - 지역 실시간 정보: 맛집 추천, 영업 시간, 부동산 시세

⚠️ 중요: "위치"는 두 가지로 구분하세요
   - ✅ 일반적 위치/주소 (시청, 관공서, 유명 건물) → 답변 가능
   - ❌ 실시간 위치 추적, GPS 좌표, 길찾기 → 답변 불가

[톤 & 스타일]
- 친근하고 자연스럽게 대화하듯이 답변하세요
- 축제 데이터 분석이 주 업무임을 자연스럽게 언급
- 답변 불가 시: 이유를 친절히 설명하고 대신 도울 수 있는 것 제안
- 적절한 경우 이모지 사용 가능 (과하지 않게)

[답변 예시]

인사 질문 ("안녕하세요!"):
"안녕하세요! 반갑습니다 😊 저는 축제 데이터를 분석하는 AI 챗봇이에요.
특정 축제의 방문인구, 매출, 연령대별 소비 패턴 같은 데이터를 분석해드릴 수 있습니다.
궁금하신 축제가 있으시면 편하게 물어보세요!"

실시간 정보 질문 ("오늘 날씨?"):
"아, 죄송해요! 실시간 날씨 정보는 제가 제공하기 어렵습니다.
날씨 앱을 확인해보시는 게 정확할 거예요 ☀️

대신 축제 데이터 분석은 제가 잘할 수 있어요!
예를 들어 '수원축제 2025 시간대별 방문인구' 같은 질문이면
자세한 분석을 해드릴 수 있습니다."

상식 질문 ("수원 화성 언제 지어졌어?"):
"수원 화성은 1794년부터 1796년까지 약 2년에 걸쳐 축조되었어요.
정조대왕이 아버지 사도세자를 위해 만든 계획도시의 핵심이었죠 🏯

참고로 수원 화성과 관련된 축제 데이터도 분석할 수 있어요!
'수원화성문화제 방문인구' 같은 질문도 환영합니다."

기술/프로그래밍 질문 ("jinja prompt templates이 뭐야?"):
"Jinja는 Python에서 사용하는 템플릿 엔진이에요! 📝
주로 HTML 파일에 동적으로 데이터를 넣을 때 사용하죠.

예를 들어 '{{ 변수명 }}' 이런 식으로 사용하고,
반복문이나 조건문도 템플릿 안에서 쓸 수 있어서 편리합니다.

저는 축제 데이터 분석이 주 업무지만, 일반적인 프로그래밍 질문도 도와드릴 수 있어요! 😊"

지역 기본 정보 질문 ("수원시의 크기가 얼마야?" / "수원시 규모를 알고 싶어"):
"수원시는 경기도에 위치한 도시로, 면적은 약 121.1km²이고,
인구는 약 120만 명 정도입니다 (2024년 기준). 🏙️

수원시는 수원화성으로 유명한 역사 도시이자 현대적인 도시랍니다!

혹시 수원시에서 열리는 축제 데이터 분석이 필요하시면
'수원화성문화제 방문인구' 같은 질문도 환영합니다! 😊"

위치 정보 질문 ("수원 시청 위치 알려줘"):
"수원시청은 경기도 수원시 팔달구 효원로 241에 위치해 있습니다! 🏛️

수원시청은 팔달구에 있으며, 수원화성과도 가까운 곳에 자리잡고 있어요.
대중교통으로 접근하기도 편리한 곳입니다.

저는 축제 데이터 분석이 주 업무이지만,
이런 기본적인 위치 정보는 도움드릴 수 있어요! 😊"

중요: 지역의 "면적", "인구", "크기", "규모", "위치", "주소" 같은 기본 정보는 일반 상식이므로 답변 가능합니다.
시청, 관공서, 유명 랜드마크의 위치는 "실시간 정보"가 아니라 "일반 상식"입니다.

답변:"""

        answer = self.llm.invoke(prompt).content.strip()

        # 면책 문구 자동 추가
        disclaimer = """

---
💡 **참고 사항**
- 실시간 정보는 정확하지 않을 수 있습니다
- 전문적인 조언(의료/법률/재무)은 전문가와 상담하세요
- 저는 축제 데이터 분석에 특화된 챗봇입니다

📊 **제가 도울 수 있는 질문 예시:**
- "수원축제 2025년 시간대별 방문인구"
- "화성문화제 20대 매출 분석"
- "정조대왕 능행차 성별 소비금액"
"""

        return answer + disclaimer

    # ──────────────────────────────────────────────
    # 메인 파이프라인
    # ──────────────────────────────────────────────

    def query(self, question: str, show_sql: bool = True,
              conversation_history: list[dict] | None = None,
              previous_festival_context: dict | None = None) -> dict | None:
        print(f"\n{'=' * 80}")
        print(f"❓ 질문: {question}")
        print('=' * 80)

        # ── STEP 1: 축제 컨텍스트 ──
        print("\n🔍 [1단계] TB_ANALYSIS_REPORT 축제 탐색...")
        festival_ctx = self._extract_festival_context(question, conversation_history, previous_festival_context)
        if not festival_ctx:
            print("   ℹ️  축제 컨텍스트 없음 → Gemini 일반 답변 모드")
            print("\n💬 [Fallback] Gemini가 일반 답변을 생성합니다...")
            fallback_answer = self._generate_fallback_answer(question)
            print(f"\n📝 답변:\n{fallback_answer}")
            print('\n' + '=' * 80)
            return {
                'question': question,
                'intent': 'fallback',
                'answer': fallback_answer
            }

        # ── 빈 결과 처리 (해당 연도 데이터 없음) ──
        if festival_ctx.get('_empty'):
            year = festival_ctx['year']
            answer = f"{year}년에 등록된 축제 데이터가 없습니다.\n\n데이터가 있는 연도의 축제를 조회하시려면 연도를 지정해주세요.\n예: \"2025년 수원 축제 갯수를 알려줘\""
            print(f"\n📝 답변:\n{answer}")
            print('\n' + '=' * 80)
            return {
                'question': question,
                'intent': '축제_목록',
                'answer': answer
            }

        # ── 질문 의도 분류 (_extract_festival_context에서 통합 추출) ──
        intent = festival_ctx.get('_intent', '통계_분석')
        print(f"\n🎯 질문 의도 (컨텍스트 추출): {intent}")

        if intent == "축제_목록":
            print("\n📋 [축제 목록 제공 모드]")
            answer = self._answer_festival_list(
                festival_ctx['all_festivals'], question,
                search_year=festival_ctx.get('_year'),
                search_region=festival_ctx.get('_region'),
            )
            print(f"\n📝 답변:\n{answer}")
            print('\n' + '=' * 80)
            return {
                'question': question,
                'festival_count': len(festival_ctx['all_festivals']),
                'intent': '축제_목록',
                'answer': answer
            }

        if intent == "축제_정보":
            print("\n📋 [축제 정보 제공 모드]")
            answer = self._answer_festival_info(festival_ctx)
            print(f"\n📝 답변:\n{answer}")
            print('\n' + '=' * 80)
            return {
                'question': question,
                'festival_context': festival_ctx,
                'intent': '축제_정보',
                'answer': answer
            }

        # ── STEP 2: 질문 분해 → 테이블 목록 (LLM이 선택) ──
        print("\n🗂  [2단계] LLM이 필요 테이블 선택 중...")
        tables = self._decompose_question(question)
        if not tables:
            print("   ❌ LLM이 테이블을 선택하지 못했습니다.")
            print("\n💬 [Fallback] Gemini가 일반 답변을 생성합니다...")
            fallback_answer = self._generate_fallback_answer(question)
            print(f"\n📝 답변:\n{fallback_answer}")
            print('\n' + '=' * 80)
            return {
                'question': question,
                'intent': 'fallback',
                'answer': fallback_answer
            }

        # ── STEP 3: 테이블별 SQL 생성 & 실행 (병렬) ──
        print(f"\n🔄 [3단계] 테이블별 SQL 생성 & 실행 ({len(tables)}개) - 병렬 처리...")
        query_results = []
        with ThreadPoolExecutor(max_workers=min(len(tables), 4)) as executor:
            futures = {
                executor.submit(self._process_single_table, question, table, festival_ctx, show_sql): table
                for table in tables
            }
            for future in as_completed(futures):
                result = future.result()
                if result:
                    query_results.append(result)

        if not query_results:
            print("\n❌ 모든 쿼리 실패")
            print("\n💬 [Fallback] Gemini가 일반 답변을 생성합니다...")
            fallback_answer = self._generate_fallback_answer(question)
            print(f"\n📝 답변:\n{fallback_answer}")
            print('\n' + '=' * 80)
            return {
                'question': question,
                'intent': 'fallback',
                'festival_context': festival_ctx,
                'answer': fallback_answer
            }

        # ── STEP 4: 통합 답변 생성 ──
        print("\n💬 [4단계] 통합 자연어 답변 생성...")
        answer = self._generate_combined_answer(question, query_results, festival_ctx)
        print(f"\n📝 답변:\n{answer}")
        print('\n' + '=' * 80)

        return {
            'question': question,
            'intent': '통계_분석',
            'festival_context': festival_ctx,
            'tables': tables,
            'query_results': query_results,
            'answer': answer,
        }

    def query_stream(self, question: str, show_sql: bool = True,
                     conversation_history: list[dict] | None = None,
                     previous_festival_context: dict | None = None,
                     previous_analysis_context: list[dict] | None = None):
        """
        query()와 동일한 흐름이지만 최종 답변을 SSE 형식으로 스트리밍.
        yields: str (SSE 포맷)
        """
        print(f"\n{'=' * 80}")
        print(f"❓ [스트리밍] 질문: {question}")
        print('=' * 80)

        festival_ctx = self._extract_festival_context(question, conversation_history, previous_festival_context)
        if not festival_ctx:
            fallback_answer = self._generate_fallback_answer(question)
            meta = json.dumps({'type': 'meta', 'intent': 'fallback', 'festival_context': None, 'sql_list': None}, ensure_ascii=False)
            yield f"data: {meta}\n\n"
            yield f"data: {json.dumps({'type': 'chunk', 'text': fallback_answer}, ensure_ascii=False)}\n\n"
            yield "data: [DONE]\n\n"
            return

        intent = festival_ctx.get('_intent', '통계_분석')

        if intent == "축제_목록":
            answer = self._answer_festival_list(
                festival_ctx['all_festivals'], question,
                search_year=festival_ctx.get('_year'),
                search_region=festival_ctx.get('_region'),
            )
            meta = json.dumps({'type': 'meta', 'intent': '축제_목록', 'festival_context': None, 'sql_list': None}, ensure_ascii=False)
            yield f"data: {meta}\n\n"
            yield f"data: {json.dumps({'type': 'chunk', 'text': answer}, ensure_ascii=False)}\n\n"
            yield "data: [DONE]\n\n"
            return

        if intent == "축제_정보":
            answer = self._answer_festival_info(festival_ctx)
            safe_ctx = self._make_safe_ctx(festival_ctx)
            meta = json.dumps({'type': 'meta', 'intent': '축제_정보', 'festival_context': safe_ctx, 'sql_list': None}, ensure_ascii=False)
            yield f"data: {meta}\n\n"
            yield f"data: {json.dumps({'type': 'chunk', 'text': answer}, ensure_ascii=False)}\n\n"
            yield "data: [DONE]\n\n"
            return

        tables = self._decompose_question(question)
        if not tables:
            fallback_answer = self._generate_fallback_answer(question)
            meta = json.dumps({'type': 'meta', 'intent': 'fallback', 'festival_context': None, 'sql_list': None}, ensure_ascii=False)
            yield f"data: {meta}\n\n"
            yield f"data: {json.dumps({'type': 'chunk', 'text': fallback_answer}, ensure_ascii=False)}\n\n"
            yield "data: [DONE]\n\n"
            return

        # 병렬 SQL 실행
        query_results = []
        with ThreadPoolExecutor(max_workers=min(len(tables), 4)) as executor:
            futures = {
                executor.submit(self._process_single_table, question, table, festival_ctx, show_sql): table
                for table in tables
            }
            for future in as_completed(futures):
                result = future.result()
                if result:
                    query_results.append(result)

        if not query_results:
            fallback_answer = self._generate_fallback_answer(question)
            meta = json.dumps({'type': 'meta', 'intent': 'fallback', 'festival_context': None, 'sql_list': None}, ensure_ascii=False)
            yield f"data: {meta}\n\n"
            yield f"data: {json.dumps({'type': 'chunk', 'text': fallback_answer}, ensure_ascii=False)}\n\n"
            yield "data: [DONE]\n\n"
            return

        # 메타 정보 먼저 전송
        safe_ctx = self._make_safe_ctx(festival_ctx)
        sql_list = [r['sql'] for r in query_results]
        chart_data = self._make_chart_data(query_results)
        meta = json.dumps({
            'type': 'meta',
            'intent': '통계_분석',
            'festival_context': safe_ctx,
            'sql_list': sql_list,
            'chart_data': chart_data,
        }, ensure_ascii=False)
        yield f"data: {meta}\n\n"

        # 답변 스트리밍
        for chunk in self._stream_combined_answer(question, query_results, festival_ctx, previous_analysis_context):
            yield f"data: {json.dumps({'type': 'chunk', 'text': chunk}, ensure_ascii=False)}\n\n"

        yield "data: [DONE]\n\n"

    def _query_without_festival(self, question: str, show_sql: bool) -> dict | None:
        """축제 컨텍스트 없이 일반 통계 쿼리"""
        all_schemas = "\n".join(
            self._format_table_schema(t)
            for t in list(self.schema_info['details'].keys())[:20]  # 처음 20개만
        )

        prompt = f"""당신은 PostgreSQL 전문가입니다.

[사용 가능한 테이블 스키마]
{all_schemas}

[질문] {question}

[규칙]
1. 테이블 참조: "{DB_SCHEMA}"."테이블명" 형식 필수
2. region_cd 기준 테이블: 매출·방문인구 계열
3. admi_cd 기준 테이블: 소비금액·생활인구·거주인구·직장인구·소득·사업체 계열
4. stdr_ymd는 'YYYYMMDD' 형식
5. 주석 없이 SQL만 반환

SQL:"""
        response = self.llm.invoke(prompt)
        sql = response.content.strip().replace('```sql', '').replace('```', '').strip()

        if show_sql:
            print(f"\n📝 SQL:\n{sql}")

        result = self.execute_query(sql)
        if not result['success']:
            fixed = self._fix_sql(sql, result['error'], '')
            result = self.execute_query(fixed)
            sql = fixed

        if not result['success']:
            print(f"❌ 실패: {result['error']}")
            return None

        print(f"✅ {result['rows']}행 조회")
        if result['rows'] > 0:
            print(result['data'].head(10).to_string())

        answer_prompt = f"질문: {question}\nSQL: {sql}\n결과: {result['data'].to_string()}\n\n한국어로 답변:"
        answer = self.llm.invoke(answer_prompt).content.strip()
        print(f"\n📝 답변:\n{answer}")
        return {'question': question, 'sql': sql, 'data': result['data'], 'answer': answer}

    # ──────────────────────────────────────────────
    # 유틸리티
    # ──────────────────────────────────────────────

    def interactive_mode(self):
        print("\n" + "=" * 80)
        print("🤖 개선된 LLM Text-to-SQL 챗봇 (옵션 3)")
        print("=" * 80)
        print("명령어: exit\n")
        while True:
            try:
                user_input = input("💬 질문: ").strip()
                if not user_input:
                    continue
                if user_input.lower() in ['exit', 'quit', '종료']:
                    print("👋 종료합니다.")
                    break
                self.query(user_input)
            except KeyboardInterrupt:
                print("\n👋 종료합니다.")
                break
            except Exception as e:
                print(f"❌ 오류: {e}")


if __name__ == "__main__":
    try:
        chatbot = ImprovedTextToSQL()

        print("\n" + "=" * 80)
        print("🧪 테스트 (개선된 버전)")
        print("=" * 80)
        chatbot.query("수원축제 2025년도 시간대별 방문인구와 소비금액을 알려줘")

        choice = input("\n대화형 모드 시작? (y/n): ").strip().lower()
        if choice == 'y':
            chatbot.interactive_mode()
    except Exception as e:
        print(f"❌ 시스템 오류: {e}")
