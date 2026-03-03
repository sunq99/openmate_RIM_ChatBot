"""
DB COMMENT를 읽어서 column_definitions.json 자동 생성

PostgreSQL COMMENT 기능 활용:
  COMMENT ON TABLE tb_tmzon_selng IS '시간대별 매출';
  COMMENT ON COLUMN tb_tmzon_selng.stdr_ymd IS '기준년월일';

사용법:
  python sync_schema_from_db.py           # 미리보기 (파일 저장 안 함)
  python sync_schema_from_db.py --save    # column_definitions.json 저장
"""

import os
import json
import argparse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_SCHEMA = "regionmonitor"
OUTPUT_FILE = "column_definitions.json"


def get_engine():
    db_uri = (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    engine = create_engine(
        db_uri,
        connect_args={"options": f"-csearch_path={DB_SCHEMA},public"}
    )
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✅ PostgreSQL 연결 성공!")
    return engine


def fetch_schema(engine) -> list[dict]:
    """DB에서 테이블/컬럼 정보 및 COMMENT를 읽어 column_definitions 형식으로 반환"""

    # 테이블 목록 + 테이블 COMMENT
    table_query = text("""
        SELECT
            c.relname                              AS table_name,
            obj_description(c.oid, 'pg_class')    AS table_comment
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = :schema
          AND c.relkind = 'r'
        ORDER BY c.relname
    """)

    # 컬럼 정보 + 컬럼 COMMENT
    column_query = text("""
        SELECT
            a.attnum                                           AS col_order,
            a.attname                                          AS column_name,
            col_description(a.attrelid, a.attnum)             AS column_comment,
            pg_catalog.format_type(a.atttypid, a.atttypmod)   AS data_type,
            CASE WHEN a.atttypmod > 0
                 THEN a.atttypmod - 4
                 ELSE NULL
            END                                                AS length,
            CASE WHEN a.attnotnull THEN 'NOT NULL' ELSE NULL END AS nullable,
            EXISTS (
                SELECT 1 FROM pg_constraint con
                WHERE con.conrelid = a.attrelid
                  AND con.contype = 'p'
                  AND a.attnum = ANY(con.conkey)
            )                                                  AS is_pk,
            (
                SELECT array_position(con.conkey, a.attnum)
                FROM pg_constraint con
                WHERE con.conrelid = a.attrelid
                  AND con.contype = 'p'
                LIMIT 1
            )                                                  AS pk_order
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = :schema
          AND c.relname = :table
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
    """)

    result = []

    with engine.connect() as conn:
        tables = conn.execute(table_query, {"schema": DB_SCHEMA}).fetchall()
        print(f"📊 {len(tables)}개 테이블 발견\n")

        for table_row in tables:
            table_name = table_row.table_name
            table_comment = table_row.table_comment or ""

            columns_raw = conn.execute(
                column_query, {"schema": DB_SCHEMA, "table": table_name}
            ).fetchall()

            columns = []
            for col in columns_raw:
                # data_type 정규화
                raw_type = col.data_type.upper()
                if "CHARACTER VARYING" in raw_type or "VARCHAR" in raw_type:
                    data_type = "VARCHAR"
                elif "CHARACTER" in raw_type or "CHAR" in raw_type:
                    data_type = "CHAR"
                elif "NUMERIC" in raw_type or "DECIMAL" in raw_type:
                    data_type = "NUMERIC"
                elif "INTEGER" in raw_type or "INT" in raw_type:
                    data_type = "INTEGER"
                elif "BIGINT" in raw_type:
                    data_type = "BIGINT"
                elif "TEXT" in raw_type:
                    data_type = "TEXT"
                elif "TIMESTAMP" in raw_type:
                    data_type = "TIMESTAMP"
                elif "DATE" in raw_type:
                    data_type = "DATE"
                elif "BOOLEAN" in raw_type:
                    data_type = "BOOLEAN"
                else:
                    data_type = raw_type

                columns.append({
                    "order": col.col_order,
                    "column_name": col.column_name.upper(),
                    "column_name_kr": col.column_comment or "",
                    "is_pk": col.pk_order if col.is_pk else None,
                    "nullable": col.nullable,
                    "data_type": data_type,
                    "length": col.length,
                    "scale": None,
                    "remark": None,
                })

            result.append({
                "table_name": table_name.upper(),
                "table_name_kr": table_comment,
                "subject": "",
                "remark": None,
                "columns": columns,
            })

            print(f"  ✅ {table_name} ({table_comment}) — {len(columns)}개 컬럼")

    return result


def main():
    parser = argparse.ArgumentParser(description="DB COMMENT → column_definitions.json 동기화")
    parser.add_argument("--save", action="store_true", help="결과를 파일로 저장")
    args = parser.parse_args()

    engine = get_engine()
    definitions = fetch_schema(engine)

    if args.save:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(definitions, f, ensure_ascii=False, indent=2)
        print(f"\n💾 저장 완료: {OUTPUT_FILE} ({len(definitions)}개 테이블)")
    else:
        print("\n--- 미리보기 (처음 2개 테이블) ---")
        print(json.dumps(definitions[:2], ensure_ascii=False, indent=2))
        print(f"\n총 {len(definitions)}개 테이블 (저장하려면 --save 옵션 사용)")


if __name__ == "__main__":
    main()
