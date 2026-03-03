from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
from txt2sql import ImprovedTextToSQL

chatbot: ImprovedTextToSQL | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global chatbot
    chatbot = ImprovedTextToSQL()
    yield
    chatbot = None


app = FastAPI(
    title="RegionMonitor ChatBot API",
    description="자연어 질문을 SQL로 변환하여 지역 모니터링 데이터를 조회합니다.",
    version="0.1.0",
    lifespan=lifespan,
)


class QueryRequest(BaseModel):
    question: str
    conversation_history: list[dict] | None = None
    previous_festival_context: dict | None = None
    previous_analysis_context: list[dict] | None = None


class QueryResponse(BaseModel):
    question: str
    answer: str
    intent: str | None = None
    festival_context: dict | None = None
    sql_list: list[str] | None = None


@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest):
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="question은 비어있을 수 없습니다.")

    result = chatbot.query(req.question, show_sql=True,
                           conversation_history=req.conversation_history,
                           previous_festival_context=req.previous_festival_context)

    if result is None:
        raise HTTPException(status_code=500, detail="쿼리 실패: 결과를 생성하지 못했습니다.")

    # festival_context의 pandas NaN/DataFrame 등 직렬화
    festival_ctx = result.get("festival_context")
    if festival_ctx:
        import pandas as pd
        festival_ctx = {
            k: None
            if isinstance(v, pd.DataFrame) or (not isinstance(v, (str, int, float, bool, type(None))) and not isinstance(v, (list, dict)))
            else (None if isinstance(v, float) and v != v else v)  # NaN → None
            for k, v in festival_ctx.items()
        }

    # SQL 목록 추출
    sql_list = None
    if "query_results" in result:
        sql_list = [r["sql"] for r in result["query_results"]]
    elif "sql" in result:
        sql_list = [result["sql"]]

    return QueryResponse(
        question=result["question"],
        answer=result["answer"],
        intent=result.get("intent"),
        festival_context=festival_ctx,
        sql_list=sql_list,
    )


@app.post("/query/stream")
def query_stream(req: QueryRequest):
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="question은 비어있을 수 없습니다.")

    def generate():
        for chunk in chatbot.query_stream(req.question,
                                          conversation_history=req.conversation_history,
                                          previous_festival_context=req.previous_festival_context,
                                          previous_analysis_context=req.previous_analysis_context):
            yield chunk

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/health")
def health():
    return {"status": "ok", "chatbot_ready": chatbot is not None}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
