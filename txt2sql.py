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
import logging
import logging.handlers
import queue
from sqlalchemy import create_engine, inspect, text
from langchain_google_genai import ChatGoogleGenerativeAI
from google import genai as google_genai
from google.genai import types as genai_types
import pandas as pd
from dotenv import load_dotenv
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from prompts import (
    prompt_classify_intent,
    prompt_festival_info,
    prompt_extract_festival_context,
    prompt_pick_best_festival,
    prompt_decompose_question,
    prompt_generate_sql,
    prompt_fix_sql,
    prompt_combined_answer,
    prompt_legacy_answer,
    prompt_query_without_festival,
    prompt_simple_answer,
)

load_dotenv()

_log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
_file_handler = logging.FileHandler("app.log", encoding="utf-8")
_file_handler.setFormatter(_log_formatter)
_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(_log_formatter)

_log_queue: queue.Queue = queue.Queue()
_queue_handler = logging.handlers.QueueHandler(_log_queue)
_queue_listener = logging.handlers.QueueListener(_log_queue, _file_handler, _stream_handler, respect_handler_level=True)
_queue_listener.start()

logging.basicConfig(level=logging.INFO, handlers=[_queue_handler])
logger = logging.getLogger(__name__)

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

# 랭킹/비교 질문 감지 키워드
_RANKING_KEYWORDS = [
    '가장 많은', '가장 높은', '가장 적은', '가장 낮은',
    '최다', '최고', '최저', '1등', '순위', '랭킹',
    '많은 순', '높은 순', '낮은 순', '상위',
]

# 랭킹 메트릭별 테이블/컬럼 매핑
_RANKING_METRICS = {
    'visitor': ('tb_nation_visit_popltn', 'region_cd', 'tot_vipop', '방문객수', '명'),
    'revenue': ('tb_nation_selng',        'region_cd', 'salamt',    '매출액',   '원'),
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
                connect_args={"options": f"-csearch_path={DB_SCHEMA},public -c statement_timeout=15000"}
            )
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("PostgreSQL 연결 성공")
        except Exception as e:
            logger.error("DB 연결 실패: %s", e)
            raise

        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash-lite",
            temperature=0,
            google_api_key=os.getenv("GOOGLE_API_KEY")
        )
        self.search_client = google_genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))
        self.column_definitions = self._load_column_definitions()
        self.schema_info = self._get_schema_info()
        self._tables_summary_cache = self._get_all_tables_summary()  # 한 번만 계산
        logger.info("[개선된 버전] LLM이 %d개 테이블 중 직접 선택합니다", len(self.schema_info['details']))
        logger.info("컬럼 정의서: %d개 테이블", len(self.column_definitions))

    # ──────────────────────────────────────────────
    # 초기화
    # ──────────────────────────────────────────────

    def _load_column_definitions(self):
        try:
            with open('column_definitions.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
            return {t['table_name'].lower(): t for t in data if 'table_name' in t}
        except Exception as e:
            logger.warning("컬럼 정의서 로드 실패: %s", e)
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
        response = self.llm.invoke(prompt_classify_intent(question))
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

        return self.llm.invoke(prompt_festival_info(info)).content.strip()

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

        # 인트로 멘트 구성
        region_str = search_region if search_region else "해당 지역"
        year_str = f"{search_year}년 " if search_year else ""

        # 가장 최근 축제 찾기
        try:
            latest_row = festivals_df.loc[festivals_df['event_bgnde'].astype(str).idxmax()]
            latest_name = latest_row.get('event_nm', '')
            latest_start = datetime.strptime(str(latest_row['event_bgnde']), '%Y%m%d').strftime('%Y년 %m월 %d일')
            latest_end   = datetime.strptime(str(latest_row['event_endde']), '%Y%m%d').strftime('%m월 %d일')
            latest_info = f" 가장 최근 축제는 **{latest_start}**에 시작한 **{latest_name}**입니다."
        except Exception:
            latest_info = ""

        # 연도 범위
        years = sorted(set(str(r['event_bgnde'])[:4] for _, r in festivals_df.iterrows()), reverse=True)
        if len(years) >= 2:
            year_range = f"{years[-1]}년부터 {years[0]}년까지 "
        elif len(years) == 1:
            year_range = f"{years[0]}년 "
        else:
            year_range = ""

        intro = (
            f"{region_str}에서 {year_range}진행된 {year_str}축제를 총 **{total_count}개** 찾았습니다."
            f"{latest_info}\n\n"
        )

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
        return intro + header + body + footer

    def _answer_festival_ranking(self, all_festivals_df, question: str,
                                  search_year: str = None,
                                  search_region: str = None) -> dict:
        """
        여러 축제를 JOIN 쿼리로 한 번에 비교하여 순위 답변 생성.
        {'answer': str, 'sql': str} 반환
        """
        from datetime import datetime

        # 메트릭 감지 (매출 vs 방문객)
        is_revenue = any(kw in question for kw in ['매출', '소비', '판매', '결제', '거래', '소비금액'])
        metric_key = 'revenue' if is_revenue else 'visitor'
        stat_table, join_col, stat_col, metric_label, unit = _RANKING_METRICS[metric_key]

        # 오름차순/내림차순 감지
        is_asc = any(kw in question for kw in ['가장 적은', '가장 낮은', '최저', '낮은 순', '적은 순'])
        order = 'ASC' if is_asc else 'DESC'

        reprt_ids = all_festivals_df['reprt_id'].astype(int).tolist()
        ids_str = ', '.join(str(i) for i in reprt_ids)

        sql = f"""
SELECT
    r.reprt_id,
    r.event_nm,
    r.event_bgnde,
    r.event_endde,
    SUM(v.{stat_col}) AS total_value
FROM "{DB_SCHEMA}"."tb_analysis_report" r
JOIN "{DB_SCHEMA}"."{stat_table}" v
    ON v.{join_col} = r.{join_col}
   AND v.stdr_ymd::text BETWEEN r.event_bgnde::text AND r.event_endde::text
WHERE r.reprt_id IN ({ids_str})
GROUP BY r.reprt_id, r.event_nm, r.event_bgnde, r.event_endde
HAVING SUM(v.{stat_col}) IS NOT NULL AND SUM(v.{stat_col}) > 0
ORDER BY total_value {order}
""".strip()

        logger.info("[랭킹 쿼리] %s", sql)
        result = self.execute_query(sql)

        if not result['success']:
            logger.error("랭킹 쿼리 실패: %s", result['error'])
            return {'answer': f"순위 데이터를 조회하는 중 오류가 발생했습니다: {result['error']}", 'sql': sql}

        if result['rows'] == 0:
            return {'answer': "조건에 맞는 통계 데이터가 없습니다.", 'sql': sql}

        df = result['data']

        # 인트로 멘트 구성
        region_str = search_region or "해당 지역"
        year_str = f"{search_year}년 " if search_year else ""
        top_row = df.iloc[0]
        top_name = top_row['event_nm']
        top_value = int(top_row['total_value'])
        direction = "가장 적은" if is_asc else "가장 많은"

        intro = (
            f"{region_str}에서 {year_str}진행된 축제 중 **{metric_label}** 기준으로 순위를 비교했습니다.\n"
            f"{direction} 축제는 **{top_name}**으로 총 **{top_value:,}{unit}**을 기록했습니다.\n\n"
        )

        # 마크다운 순위표
        rank_header = f"### {year_str}{region_str} 축제 {metric_label} 순위\n\n"
        table = f"| 순위 | 축제명 | 기간 | {metric_label}({unit}) |\n"
        table += f"|-----|-------|------|------|\n"
        for rank, (_, row) in enumerate(df.iterrows(), 1):
            value = int(row['total_value'])
            try:
                start = datetime.strptime(str(row['event_bgnde']), '%Y%m%d').strftime('%m/%d')
                end   = datetime.strptime(str(row['event_endde']), '%Y%m%d').strftime('%m/%d')
                period = f"{start}~{end}" if start != end else start
            except Exception:
                period = f"{row['event_bgnde']}~{row['event_endde']}"
            table += f"| {rank} | {row['event_nm']} | {period} | {value:,} |\n"

        answer = intro + rank_header + table
        return {'answer': answer, 'sql': sql}

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

        resp = self.llm.invoke(
            prompt_extract_festival_context(history_section, festival_hint, question, current_year)
        )
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

        logger.info("추출된 키워드 → 지역: %s, 연도: %s, 특정날짜: %s, 월: %s", region, year, specific_date, month)

        if not region and not year and not specific_date:
            if previous_festival_context:
                logger.info("추출 실패 → 이전 축제 컨텍스트 재사용: [%s]", previous_festival_context.get('event_nm'))
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
            logger.info("축제 관련 키워드 없음 → 일반 질문으로 판단")
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
            params = {}
            if search_region:
                parts = [p for p in search_region.split() if len(p) >= 2]
                if not parts:
                    parts = [search_region]
                clauses = []
                for i, p in enumerate(parts):
                    key = f"kw{i}"
                    params[key] = f"%{p}%"
                    clauses.append(
                        f"(event_nm LIKE :{key} OR reprt_nm LIKE :{key} "
                        f"OR event_plc LIKE :{key} OR cty_nm LIKE :{key} "
                        f"OR sido_nm LIKE :{key})"
                    )
                conds.append("(" + " OR ".join(clauses) + ")")
            if specific_date:
                conds.append("event_bgnde <= :sd AND event_endde >= :sd")
                params["sd"] = specific_date
            elif year:
                conds.append("event_bgnde LIKE :yr")
                params["yr"] = f"{year}%"
            if not conds:
                return None, {}
            sql = f"""
SELECT reprt_id, event_nm, region_cd, admi_cd,
       sido_nm, cty_nm, event_bgnde, event_endde, event_plc,
       event_site, evnet_dc, event_auspc
FROM "{DB_SCHEMA}"."tb_analysis_report"
WHERE {" AND ".join(conds)}
ORDER BY event_bgnde DESC, reprt_id DESC
"""
            return sql, params

        sql, params = _build_sql(region)
        if not sql:
            return None

        logger.debug("실행 SQL: %s", sql.strip())
        result = self.execute_query(sql, params)

        if not result['success']:
            logger.warning("SQL 오류: %s", result['error'])
            return None

        # 0행이고 region에 축제 접미어가 있으면 접미어 제거 후 재시도
        # 예: "수원축제" → "수원" 으로 재검색
        if result['rows'] == 0 and region:
            stripped = _strip_festival_suffix(region)
            if stripped:
                logger.info("0행 → '%s' 접미어 제거 후 '%s'로 재시도", region, stripped)
                sql2, params2 = _build_sql(stripped)
                if sql2:
                    result2 = self.execute_query(sql2, params2)
                    if result2['success'] and result2['rows'] > 0:
                        region = stripped
                        result = result2
                        sql = sql2
                        logger.debug("재시도 SQL: %s", sql2.strip())

        if result['rows'] == 0:
            logger.warning("조건에 맞는 축제가 없습니다.")
            # 연도 검색인데 결과 없으면 빈 결과를 담은 컨텍스트 반환 (fallback 방지)
            if year and not region and not specific_date:
                return {'_empty': True, 'year': year, 'all_festivals': result['data'], '_intent': intent}
            return None

        # 랭킹/비교 질문이고 통계 의도인 경우, 단일 축제 선택 없이 전체 비교 모드로 전환
        is_ranking = (intent == '통계_분석'
                      and result['rows'] > 1
                      and any(kw in question for kw in _RANKING_KEYWORDS))

        if is_ranking:
            logger.info("랭킹 질문 감지 (%d개 축제 비교 모드)", result['rows'])
            return {
                'all_festivals': result['data'],
                '_intent': intent,
                '_ranking': True,
                '_year': year,
                '_region': region,
            }

        if result['rows'] > 1:
            logger.info("%d개 축제 검색됨 (전체 목록):\n%s", result['rows'],
                        result['data'][['reprt_id', 'event_nm', 'event_bgnde', 'event_endde']].to_string(index=False))

            # "최근" 유형 질문은 날짜순 첫 번째 항목 사용
            if any(kw in question for kw in ['최근', '최신', '가장 최근', '마지막', '요즘']):
                row = result['data'].iloc[0]
                logger.info("최신 날짜 기준 선택: [%s] (event_bgnde=%s)", row['event_nm'], row['event_bgnde'])
            else:
                # 이전 축제 힌트를 LLM에게 제공해서 연속 질문 vs 새 질문을 스스로 판단
                row = self._pick_best_festival(question, result['data'], previous_festival_context)
                logger.info("LLM 선택: [%s] (reprt_id=%d)", row['event_nm'], int(row['reprt_id']))
        else:
            row = result['data'].iloc[0]

        ctx = {k: row.get(k) for k in [
            'reprt_id', 'event_nm', 'region_cd', 'admi_cd',
            'sido_nm', 'cty_nm', 'event_bgnde', 'event_endde', 'event_plc',
            'event_site', 'evnet_dc', 'event_auspc'
        ]}
        ctx['specific_date'] = specific_date
        ctx['all_festivals'] = result['data']
        ctx['_intent'] = intent
        ctx['_ranking'] = False
        ctx['_year'] = year
        ctx['_region'] = region

        date_info = f"특정 날짜: {specific_date}" if specific_date else f"기간: {ctx['event_bgnde']}~{ctx['event_endde']}"
        logger.info("축제: [%s] | %s | REGION_CD: %s | ADMI_CD: %s",
                    ctx['event_nm'], date_info, ctx['region_cd'], ctx['admi_cd'])
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

        response = self.llm.invoke(prompt_pick_best_festival(question, candidates, prev_hint))
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
        tables_summary = self._tables_summary_cache  # 캐시 사용 (매번 재계산 X)

        response = self.llm.invoke(
            prompt_decompose_question(question, len(self.schema_info['tables']), tables_summary)
        )
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
            logger.info("LLM이 선택한 테이블: %s", ', '.join(valid))
        else:
            logger.warning("LLM이 테이블 선택 실패 → 재시도")
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

        response = self.llm.invoke(prompt_generate_sql(
            table_schema=table_schema,
            festival_name=festival_ctx['event_nm'],
            filter_col=filter_col,
            filter_val=filter_val,
            date_desc=date_desc,
            date_condition=date_condition,
            group_by_hint=group_by_hint,
            question=question,
            db_schema=DB_SCHEMA,
            table=table,
        ))
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
                logger.error("보안: 위험한 키워드 '%s' 감지", keyword)
                return False

        # 2. WHERE 절 필수 검증 (region_cd 또는 admi_cd)
        if table in ADMI_CD_TABLES:
            required_key = "admi_cd"
            required_val = festival_ctx.get('admi_cd')
        else:
            required_key = "region_cd"
            required_val = festival_ctx.get('region_cd')

        if required_key not in sql.lower():
            logger.error("보안: WHERE %s 조건 누락", required_key)
            return False

        # 3. 테이블명 검증
        if f'"{DB_SCHEMA}"."{table}"' not in sql and f'{DB_SCHEMA}.{table}' not in sql:
            logger.error("보안: 잘못된 테이블 참조")
            return False

        return True

    def execute_query(self, sql: str, params: dict | None = None) -> dict:
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(sql), conn, params=params or {})
            return {'success': True, 'data': df, 'rows': len(df), 'columns': list(df.columns)}
        except Exception as e:
            return {'success': False, 'error': str(e), 'error_type': type(e).__name__}

    def _fix_sql(self, sql: str, error: str, table: str) -> str:
        """LLM이 오류난 SQL 자동 수정"""
        if table in ADMI_CD_TABLES:
            filter_hint = "admi_cd 조건 유지 (region_cd 아님)"
        else:
            filter_hint = "region_cd 조건 유지"

        response = self.llm.invoke(prompt_fix_sql(sql, error, filter_hint, DB_SCHEMA, table))
        return response.content.strip().replace('```sql', '').replace('```', '').strip()

    # ──────────────────────────────────────────────
    # STEP 3 병렬 처리용 헬퍼
    # ──────────────────────────────────────────────

    def _process_single_table(self, question: str, table: str,
                              festival_ctx: dict, show_sql: bool) -> dict | None:
        """테이블 1개에 대한 SQL 생성 → 검증 → 실행 (병렬 실행용)"""
        logger.info("%s (%s)", table, self._get_table_kr(table))
        sql = self._generate_sql_per_table(question, table, festival_ctx)

        if not self._validate_sql(sql, table, festival_ctx):
            logger.warning("보안 검증 실패 → 스킵 (%s)", table)
            return None

        if show_sql:
            logger.debug("SQL: %s", sql)

        result = self.execute_query(sql)
        if not result['success']:
            logger.warning("오류: %s → 자동 수정 시도... (%s)", result['error'], table)
            fixed = self._fix_sql(sql, result['error'], table)

            if not self._validate_sql(fixed, table, festival_ctx):
                logger.warning("수정된 SQL도 보안 검증 실패 → 스킵 (%s)", table)
                return None

            result = self.execute_query(fixed)
            sql = fixed

        if result['success']:
            logger.info("%d행 조회 | 컬럼: %s", result['rows'], ', '.join(result['columns']))
            if result['rows'] > 0 and logger.isEnabledFor(logging.DEBUG):
                logger.debug(result['data'].to_string())
            return {'table': table, 'sql': sql, 'data': result['data'], 'rows': result['rows']}
        else:
            logger.error("최종 실패: %s (%s)", result['error'], table)
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

        return prompt_combined_answer(
            question=question,
            festival_name=festival_ctx['event_nm'],
            date_info=date_info,
            results_text=results_text,
            change_instruction=change_instruction,
            prev_analysis_section=prev_analysis_section,
        )

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
            logger.warning("차트 데이터 생성 실패 (%s): %s", table, e)
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
    # Fallback: DB에 없는 질문은 Google Search Grounding으로 답변
    # ──────────────────────────────────────────────

    def _generate_fallback_answer(self, question: str) -> str:
        """Google Search Grounding을 사용한 웹 검색 기반 답변"""
        try:
            system_instruction = (
                "당신은 축제 데이터 분석을 도와주는 친절한 AI 챗봇입니다. "
                "축제 데이터베이스에서 답을 찾을 수 없는 질문에 대해 Google 검색을 통해 최신 정보를 제공합니다. "
                "친근하고 자연스럽게 대화하며 적절한 이모지를 사용하세요. "
                "축제 데이터 분석이 주 업무임을 자연스럽게 언급하세요."
            )
            response = self.search_client.models.generate_content(
                model="gemini-2.5-flash",
                contents=f"{system_instruction}\n\n질문: {question}",
                config=genai_types.GenerateContentConfig(
                    tools=[genai_types.Tool(google_search=genai_types.GoogleSearch())],
                )
            )
            answer = response.text.strip()
            disclaimer = """

---
💡 **참고 사항**
- 위 정보는 Google 검색을 통해 제공된 최신 정보입니다
- 전문적인 조언(의료/법률/재무)은 전문가와 상담하세요
- 저는 축제 데이터 분석에 특화된 챗봇입니다

📊 **제가 도울 수 있는 질문 예시:**
- "수원축제 2025년 시간대별 방문인구"
- "화성문화제 20대 매출 분석"
- "정조대왕 능행차 성별 소비금액"
"""
            return answer + disclaimer

        except Exception as e:
            logger.error("Google Search Grounding 실패, 기본 답변으로 전환: %s", e)
            return self._generate_legacy_answer(question)

    def _generate_legacy_answer(self, question: str) -> str:
        """Google Search 실패 시 Gemini 학습 지식 기반 답변 (fallback)"""
        answer = self.llm.invoke(prompt_legacy_answer(question)).content.strip()
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
        logger.info("질문: %s", question)

        # ── STEP 1+2 병렬: 축제 컨텍스트 탐색 & 테이블 선택 동시 실행 ──
        logger.info("[1+2단계 병렬] 축제 탐색 & 테이블 선택 동시 시작...")
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_ctx    = executor.submit(self._extract_festival_context, question, conversation_history, previous_festival_context)
            future_tables = executor.submit(self._decompose_question, question)
            festival_ctx  = future_ctx.result()
            tables_prefetch = future_tables.result()

        if not festival_ctx:
            logger.info("축제 컨텍스트 없음 → Gemini 일반 답변 모드")
            fallback_answer = self._generate_fallback_answer(question)
            logger.info("답변:\n%s", fallback_answer)
            return {
                'question': question,
                'intent': 'fallback',
                'answer': fallback_answer
            }

        # ── 빈 결과 처리 (해당 연도 데이터 없음) ──
        if festival_ctx.get('_empty'):
            year = festival_ctx['year']
            answer = f"{year}년에 등록된 축제 데이터가 없습니다.\n\n데이터가 있는 연도의 축제를 조회하시려면 연도를 지정해주세요.\n예: \"2025년 수원 축제 갯수를 알려줘\""
            logger.info("답변:\n%s", answer)
            return {
                'question': question,
                'intent': '축제_목록',
                'answer': answer
            }

        # ── 질문 의도 분류 (_extract_festival_context에서 통합 추출) ──
        intent = festival_ctx.get('_intent', '통계_분석')
        logger.info("질문 의도 (컨텍스트 추출): %s", intent)

        if intent == "축제_목록":
            logger.info("[축제 목록 제공 모드]")
            answer = self._answer_festival_list(
                festival_ctx['all_festivals'], question,
                search_year=festival_ctx.get('_year'),
                search_region=festival_ctx.get('_region'),
            )
            logger.info("답변:\n%s", answer)
            return {
                'question': question,
                'festival_count': len(festival_ctx['all_festivals']),
                'intent': '축제_목록',
                'answer': answer
            }

        if intent == "축제_정보":
            logger.info("[축제 정보 제공 모드]")
            answer = self._answer_festival_info(festival_ctx)
            logger.info("답변:\n%s", answer)
            return {
                'question': question,
                'festival_context': festival_ctx,
                'intent': '축제_정보',
                'answer': answer
            }

        # ── 랭킹/비교 모드 ──
        if festival_ctx.get('_ranking'):
            logger.info("[축제 랭킹 비교 모드]")
            ranking_result = self._answer_festival_ranking(
                festival_ctx['all_festivals'], question,
                search_year=festival_ctx.get('_year'),
                search_region=festival_ctx.get('_region'),
            )
            logger.info("답변:\n%s", ranking_result['answer'])
            return {
                'question': question,
                'intent': '축제_순위',
                'sql_list': [ranking_result['sql']],
                'answer': ranking_result['answer'],
            }

        # ── STEP 2: 테이블 목록 (병렬 prefetch 결과 사용) ──
        tables = tables_prefetch
        if not tables:
            logger.error("LLM이 테이블을 선택하지 못했습니다.")
            fallback_answer = self._generate_fallback_answer(question)
            logger.info("답변:\n%s", fallback_answer)
            return {
                'question': question,
                'intent': 'fallback',
                'answer': fallback_answer
            }
        logger.info("[테이블 선택 완료] %s", tables)

        # ── STEP 3: 테이블별 SQL 생성 & 실행 (병렬) ──
        logger.info("[3단계] 테이블별 SQL 생성 & 실행 (%d개) - 병렬 처리...", len(tables))
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
            logger.error("모든 쿼리 실패")
            fallback_answer = self._generate_fallback_answer(question)
            logger.info("답변:\n%s", fallback_answer)
            return {
                'question': question,
                'intent': 'fallback',
                'festival_context': festival_ctx,
                'answer': fallback_answer
            }

        # ── STEP 4: 통합 답변 생성 ──
        logger.info("[4단계] 통합 자연어 답변 생성...")
        answer = self._generate_combined_answer(question, query_results, festival_ctx)
        logger.info("답변:\n%s", answer)

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
        logger.info("[스트리밍] 질문: %s", question)

        # 축제 컨텍스트 탐색 & 테이블 선택 병렬 실행
        logger.info("[스트리밍] 1+2단계 병렬 시작...")
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_ctx      = executor.submit(self._extract_festival_context, question, conversation_history, previous_festival_context)
            future_tables   = executor.submit(self._decompose_question, question)
            festival_ctx    = future_ctx.result()
            tables_prefetch = future_tables.result()

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

        # ── 랭킹/비교 모드 ──
        if festival_ctx.get('_ranking'):
            logger.info("[스트리밍] 축제 랭킹 비교 모드")
            ranking_result = self._answer_festival_ranking(
                festival_ctx['all_festivals'], question,
                search_year=festival_ctx.get('_year'),
                search_region=festival_ctx.get('_region'),
            )
            meta = json.dumps({
                'type': 'meta',
                'intent': '축제_순위',
                'festival_context': None,
                'sql_list': [ranking_result['sql']],
            }, ensure_ascii=False)
            yield f"data: {meta}\n\n"
            yield f"data: {json.dumps({'type': 'chunk', 'text': ranking_result['answer']}, ensure_ascii=False)}\n\n"
            yield "data: [DONE]\n\n"
            return

        tables = tables_prefetch
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

        response = self.llm.invoke(prompt_query_without_festival(all_schemas, question, DB_SCHEMA))
        sql = response.content.strip().replace('```sql', '').replace('```', '').strip()

        if show_sql:
            logger.debug("SQL:\n%s", sql)

        result = self.execute_query(sql)
        if not result['success']:
            fixed = self._fix_sql(sql, result['error'], '')
            result = self.execute_query(fixed)
            sql = fixed

        if not result['success']:
            logger.error("실패: %s", result['error'])
            return None

        logger.info("%d행 조회", result['rows'])
        if result['rows'] > 0 and logger.isEnabledFor(logging.DEBUG):
            logger.debug(result['data'].head(10).to_string())

        answer = self.llm.invoke(
            prompt_simple_answer(question, sql, result['data'].to_string())
        ).content.strip()
        logger.info("답변:\n%s", answer)
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
