# OpenMate ChatBot

LLM 기반 Text-to-SQL 챗봇 - 축제 데이터 분석 시스템

## 📁 프로젝트 구조

```
openmate_ChatBot/
├── main.py                 # FastAPI 서버 (API 진입점)
├── app.py                  # Streamlit 데모 페이지
├── txt2sql.py              # 핵심 챗봇 엔진
├── sync_schema_from_db.py  # DB COMMENT → column_definitions.json 동기화
├── column_definitions.json # 테이블/컬럼 한글 정의 (자동 생성)
├── pyproject.toml          # 의존성 설정
└── poetry.lock
```

---

## 🚀 빠른 시작

### 1. 환경 설정

```bash
# 의존성 설치
poetry install
```

`.env` 파일 작성:

```env
DB_USER=your_user
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=regionMonitor
GOOGLE_API_KEY=your_gemini_api_key
```

### 2. 서버 실행

**터미널 1 - FastAPI (API 서버)**
```bash
poetry run python main.py
```

**터미널 2 - Streamlit (데모 페이지)**
```bash
poetry run streamlit run app.py
```

**터미널 3 - 동시 실행**
```bash
poetry run python main.py & poetry run streamlit run app.py
```

### 3. 접속

| 서비스 | URL |
|--------|-----|
| Streamlit 데모 | http://localhost:8501 |
| FastAPI Swagger | http://localhost:8000/docs |
| 서버 상태 확인 | http://localhost:8000/health |

---

## 🔄 DB 스키마 동기화

DB에 테이블/컬럼이 추가되거나 변경되면 아래 순서로 동기화합니다.

### DB에서 COMMENT 작성 (DBA)

```sql
COMMENT ON TABLE tb_new_table IS '새 테이블 설명';
COMMENT ON COLUMN tb_new_table.col1 IS '컬럼 한글명';
```

### column_definitions.json 업데이트

```bash
# 미리보기 (파일 저장 안 함)
poetry run python sync_schema_from_db.py

# 실제 저장
poetry run python sync_schema_from_db.py --save
```

### 서버 재시작

```bash
poetry run python main.py
```

---

## 🎯 주요 기능

### 동작 흐름

```
사용자 질문 (자연어)
    │
    ▼
축제 컨텍스트 추출 (TB_ANALYSIS_REPORT 조회)
    │  축제 관련 키워드 없음 → fallback
    ▼
질문 의도 분류 (LLM)
    ├── 축제_목록  → 축제 목록 마크다운 표 출력
    ├── 축제_정보  → 축제 기본 정보 답변
    ├── 통계_분석  → SQL 병렬 생성 → DB 조회 → 스트리밍 답변
    └── fallback   → Gemini 일반 상식 답변
```

### SQL 병렬 처리

여러 테이블 조회가 필요한 경우 `ThreadPoolExecutor`로 동시 실행하여 응답 속도를 단축합니다.

### 스트리밍 응답

`/query/stream` 엔드포인트(SSE)를 통해 답변을 실시간으로 스트리밍합니다.

```
data: {"type":"meta","intent":"통계_분석","festival_context":{...},"sql_list":[...]}
data: {"type":"chunk","text":"## 분석 결과..."}
data: [DONE]
```

### LLM 기반 자동 테이블 선택

하드코딩 없이 LLM이 전체 테이블 목록에서 질문에 맞는 테이블을 자동 선택합니다.

```
질문: "시간대별 방문인구는?"
  → LLM: tb_tmzon_visit_popltn 선택
```

### SQL 보안 검증

LLM이 생성한 SQL을 실행 전 자동 검증합니다.

- DROP, DELETE 등 위험 키워드 차단
- WHERE region_cd / admi_cd 조건 필수 검증
- 테이블명 화이트리스트 검증

### Fallback 처리

축제 데이터와 무관한 질문(일반 상식, 지역 기본 정보 등)은 DB 조회 없이 Gemini가 직접 답변합니다.

```
질문: "서울시의 인구는?"  →  Gemini 일반 답변
질문: "수원화성문화제 방문인구는?"  →  DB 조회 후 답변
```

---

## 💡 기술 스택

| 항목 | 내용 |
|------|------|
| LLM | Google Gemini 2.5 Flash Lite |
| API Framework | FastAPI |
| Demo UI | Streamlit |
| Database | PostgreSQL (regionmonitor 스키마) |
| ORM | SQLAlchemy |
| LLM Framework | LangChain |
| Language | Python 3.11+ |

---

## 📝 API 명세

### `POST /query`

자연어 질문을 받아 DB를 조회하고 답변을 반환합니다.

**Request**
```json
{
  "question": "수원축제 2025년 연령대별 방문인구는?",
  "conversation_history": [],
  "previous_festival_context": null
}
```

**Response**
```json
{
  "question": "수원축제 2025년 연령대별 방문인구는?",
  "answer": "...",
  "intent": "통계_분석",
  "festival_context": {
    "event_nm": "수원시 3대 축제",
    "event_bgnde": "20250927",
    "event_endde": "20251012"
  },
  "sql_list": ["SELECT ..."]
}
```

`intent` 값:

| 값 | 설명 |
|----|------|
| `통계_분석` | DB 조회 후 데이터 분석 답변 |
| `축제_정보` | 축제 기본 정보 답변 |
| `축제_목록` | 축제 목록/갯수 답변 |
| `fallback` | DB 미해당 → Gemini 일반 답변 |

### `POST /query/stream`

동일한 질문을 SSE 형식으로 스트리밍 반환합니다.

**Request** - `/query`와 동일

**Response (SSE)**
```
data: {"type":"meta","intent":"통계_분석","festival_context":{...},"sql_list":[...]}
data: {"type":"chunk","text":"분석 결과 첫 번째 청크..."}
data: {"type":"chunk","text":"...이어지는 내용..."}
data: [DONE]
```

### `GET /health`

서버 및 챗봇 초기화 상태를 반환합니다.

```json
{
  "status": "ok",
  "chatbot_ready": true
}
```
