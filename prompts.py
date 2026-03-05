"""
txt2sql.py 에서 사용하는 LLM 프롬프트 모음.
동적 변수를 받아 완성된 프롬프트 문자열을 반환하는 함수로 구성.
"""


def prompt_classify_intent(question: str) -> str:
    """질문 의도를 축제_목록 / 축제_정보 / 통계_분석 으로 분류"""
    return f"""다음 질문의 의도를 분류하세요.

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


def prompt_festival_info(info: str) -> str:
    """축제 기본 정보를 자연어로 설명"""
    return f"""다음 축제 정보를 바탕으로 사용자에게 친절하고 자연스럽게 설명해주세요.

{info}

[작성 규칙]
1. 축제명을 굵게 표시하고 간단한 소개로 시작
2. 기간, 장소, 주최 등을 읽기 쉽게 정리
3. 축제 설명이 있으면 포함
4. 홈페이지가 있으면 링크 제공
5. 마지막에 "더 자세한 통계 정보가 필요하시면 물어보세요" 같은 안내 추가
6. 간결하고 친근하게 (3-5문단 정도)

답변:"""


def prompt_extract_festival_context(
    history_section: str,
    festival_hint: str,
    question: str,
    current_year: int,
) -> str:
    """이전 대화에서 축제/지역/날짜 컨텍스트 추출"""
    return f"""이전 대화 맥락을 참고하여 현재 질문에서 검색 키워드(지역명 또는 축제명)와 날짜 정보를 추출하세요.
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


def prompt_pick_best_festival(question: str, candidates: str, prev_hint: str) -> str:
    """여러 축제 후보 중 가장 적합한 것을 선택"""
    return f"""다음 축제 목록 중 아래 질문에 가장 적합한 축제의 reprt_id를 숫자만 반환하세요.

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


def prompt_decompose_question(question: str, table_count: int, tables_summary: str) -> str:
    """질문에 필요한 테이블 목록 선택"""
    return f"""당신은 데이터베이스 전문가입니다.

[질문]
{question}

[사용 가능한 테이블 목록 ({table_count}개)]
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


def prompt_generate_sql(
    table_schema: str,
    festival_name: str,
    filter_col: str,
    filter_val: str,
    date_desc: str,
    date_condition: str,
    group_by_hint: str,
    question: str,
    db_schema: str,
    table: str,
) -> str:
    """테이블 1개에 대한 SQL 생성"""
    return f"""당신은 PostgreSQL 전문가입니다.

[테이블 스키마]
{table_schema}

[축제 컨텍스트]
- 축제명: {festival_name}
- {filter_col.upper()}: {filter_val}  ← 이 값으로 관심지역을 필터링합니다
- {date_desc}

[사용자 질문]
{question}

[SQL 규칙]
1. 테이블 참조: "{db_schema}"."{table}"
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


def prompt_fix_sql(sql: str, error: str, filter_hint: str, db_schema: str, table: str) -> str:
    """오류난 SQL 자동 수정"""
    return f"""SQL 오류 수정:

원본 SQL:
{sql}

오류:
{error}

수정 규칙:
- 테이블 참조: "{db_schema}"."{table}"
- {filter_hint}
- stdr_ymd는 'YYYYMMDD' 형식
- HAVING 절 사용 금지

수정된 SQL만 반환."""


def prompt_combined_answer(
    question: str,
    festival_name: str,
    date_info: str,
    results_text: str,
    change_instruction: str,
    prev_analysis_section: str,
) -> str:
    """다중 테이블 조회 결과를 통합하여 자연어 답변 생성"""
    return f"""질문: {question}

축제: {festival_name} ({date_info})
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


def prompt_legacy_answer(question: str) -> str:
    """Google Search 실패 시 학습 지식 기반 fallback 답변"""
    return f"""당신은 축제 데이터 분석을 도와주는 친절한 AI 챗봇입니다.

[질문]
{question}

[상황]
데이터베이스에서 이 질문에 대한 답을 찾을 수 없습니다.
일반 지식 범위 내에서 답변하고, 실시간 정보가 필요한 경우 관련 서비스 확인을 안내하세요.

[톤 & 스타일]
- 친근하고 자연스럽게 대화
- 축제 데이터 분석이 주 업무임을 자연스럽게 언급
- 적절한 이모지 사용

답변:"""


def prompt_query_without_festival(all_schemas: str, question: str, db_schema: str) -> str:
    """축제 컨텍스트 없이 일반 통계 SQL 생성"""
    return f"""당신은 PostgreSQL 전문가입니다.

[사용 가능한 테이블 스키마]
{all_schemas}

[질문] {question}

[규칙]
1. 테이블 참조: "{db_schema}"."테이블명" 형식 필수
2. region_cd 기준 테이블: 매출·방문인구 계열
3. admi_cd 기준 테이블: 소비금액·생활인구·거주인구·직장인구·소득·사업체 계열
4. stdr_ymd는 'YYYYMMDD' 형식
5. 주석 없이 SQL만 반환

SQL:"""


def prompt_simple_answer(question: str, sql: str, result_str: str) -> str:
    """SQL 결과를 간단히 한국어로 답변"""
    return f"질문: {question}\nSQL: {sql}\n결과: {result_str}\n\n한국어로 답변:"
