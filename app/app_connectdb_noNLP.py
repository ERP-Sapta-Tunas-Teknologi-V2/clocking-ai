from typing import Dict, List, Optional, Union, Tuple
import streamlit as st
import requests
import json
import re
import mysql.connector
import pandas as pd
from fpdf import FPDF
import io
from decimal import Decimal

# ================================
# ✅ CONFIGURATION
# ================================

class Config:
    OLLAMA_URL = "http://localhost:11434/api/generate"
    DB_CONFIG = {
        "host": "localhost",
        "user": "root",
        "password": "",  # Fill if needed
        "database": "clocking_reports"
    }
    MODEL_LIST = ["qwen3:0.6b"]
    SQL_MAPPING = {
        "sql1": {
            "description": "Clocking Month Of Month selama 4 bulan",
            "query": """
                SELECT 
                  DATE_FORMAT(ca.start_date, '%Y-%m') AS month,
                  SUM(ca.duration_minutes) AS total_minutes
                FROM clocking_activities ca
                  JOIN daily_activities da ON ca.daily_activity_id = da.daily_activity_id
                  JOIN users u ON da.user_id = u.user_id
                WHERE u.full_name LIKE %s
                  AND ca.category_id = 400
                  AND ca.start_date >= DATE_SUB(CURDATE(), INTERVAL 4 MONTH)
                GROUP BY DATE_FORMAT(ca.start_date, '%Y-%m')
                ORDER BY month ASC;
            """
        },
        "sql2": {
            "description": "Analisa clocking bulan 1-3 dibanding target",
            "query": """
                SELECT 
                  DATE_FORMAT(ca.start_date, '%Y-%m') AS month,
                  SUM(ca.duration_minutes) AS total_minutes,
                  40 * 60 * 4 AS monthly_target_minutes,
                  SUM(ca.duration_minutes) - (40 * 60 * 4) AS difference_from_target
                FROM clocking_activities ca
                  JOIN daily_activities da ON ca.daily_activity_id = da.daily_activity_id
                  JOIN users u ON da.user_id = u.user_id
                WHERE u.full_name LIKE %s
                  AND MONTH(ca.start_date) BETWEEN 1 AND 3
                  AND YEAR(ca.start_date) = YEAR(CURDATE())
                GROUP BY DATE_FORMAT(ca.start_date, '%Y-%m')
                ORDER BY month ASC;
            """
        }
    }

# ================================
# ✅ DATABASE HANDLER
# ================================

class Database:
    @staticmethod
    def run_query(sql: str, params: tuple = None) -> Union[List[Dict], str]:
        try:
            with mysql.connector.connect(**Config.DB_CONFIG) as conn:
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute(sql, params)
                    return cursor.fetchall()
        except mysql.connector.Error as e:
            return f"Database error: {e}"

# ================================
# ✅ LLM HANDLER
# ================================

class LLM:
    # Define synonyms for key terms
    KEYWORD_SYNONYMS = {
        "analisa": ["analisa", "analisis", "evaluasi", "review", "tinjau"],
        "user": ["user", "pengguna"],
        "bulan 1-3": ["bulan 1-3", "januari-maret", "awal tahun", "kuartal pertama"],
        "clocking": ["clocking", "jam kerja", "waktu kerja", ""],  # Empty string allows optional "clocking"
        "4 bulan": ["4 bulan", "empat bulan", "4 months", "last 4 months"]
    }

    @staticmethod
    def extract_username(query: str) -> Optional[str]:
        # Match username after user-related keywords
        match = re.search(r"(?:user|pengguna|untuk user|analisa user)\s+(\w+)", query, re.IGNORECASE)
        return match.group(1) if match else None

    @staticmethod
    def normalize_query(query: str) -> str:
        # Normalize query: lowercase, remove extra spaces
        return ' '.join(query.lower().strip().split())

    @classmethod
    def detect_sql_query_type(cls, query: str) -> Optional[str]:
        normalized_query = cls.normalize_query(query)

        # Helper function to check if any synonym is present
        def has_synonym(keyword: str, text: str) -> bool:
            return any(syn in text for syn in cls.KEYWORD_SYNONYMS[keyword])

        # Check for sql1: 4-month clocking analysis
        if has_synonym("4 bulan", normalized_query):
            return "sql1"

        # Check for sql2: Jan-Mar analysis (with optional "clocking")
        if (has_synonym("analisa", normalized_query) and
            has_synonym("bulan 1-3", normalized_query)):
            return "sql2"

        return None

    @staticmethod
    def stream_response(model: str, prompt: str) -> Tuple[str, str]:
        response = requests.post(
            Config.OLLAMA_URL,
            json={"model": model, "prompt": prompt, "stream": True},
            stream=True
        )
        full_think, full_response, current_section = "", "", "response"
        
        for line in response.iter_lines():
            if not line:
                continue
            try:
                data = json.loads(line.decode('utf-8'))
                chunk = data.get('response', '')
                if "<think>" in chunk:
                    current_section = "think"
                    full_think += re.sub(r'^<think>', '', chunk)
                elif "</think>" in chunk:
                    current_section = "response"
                    full_think += re.sub(r'</think>$', '', chunk)
                elif current_section == "think":
                    full_think += chunk
                else:
                    full_response += chunk
            except json.JSONDecodeError:
                continue
        return full_response.strip() or "[No response from model]", full_think.strip() or ""

    @classmethod
    def summarize(cls, model: str, result: List[Dict], query: str) -> Tuple[str, str]:
        prompt = f"Berikut hasil query:\n{json.dumps(result, default=str)}\n\nTolong buatkan ringkasan berdasarkan query ini: {query}"
        return cls.stream_response(model, prompt)

# ================================
# ✅ OUTPUT GENERATOR
# ================================

class OutputGenerator:
    @staticmethod
    def to_excel(result: List[Dict], query: str, think: str, response: str) -> bytes:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            pd.DataFrame(result).to_excel(writer, index=False, sheet_name="SQL Result")
            pd.DataFrame({
                "Item": ["Query", "LLM Analysis Think", "LLM Analysis Response"],
                "Content": [query, think, response]
            }).to_excel(writer, index=False, sheet_name="Analysis Summary")
        return output.getvalue()

    @staticmethod
    def to_pdf(query: str, result: List[Dict], analysis: str) -> bytes:
        pdf = FPDF(orientation="P", unit="mm", format="A4")
        pdf.add_page()
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 10, "LLM Analysis Report", ln=True)

        # Query
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 10, "Query:", ln=True)
        pdf.set_font("Arial", "", 12)
        pdf.multi_cell(0, 10, query)

        # SQL Result
        df = pd.DataFrame(result) if result else pd.DataFrame()
        if not df.empty:
            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 10, "SQL Result Table:", ln=True)
            col_width = pdf.w / (len(df.columns) + 1)
            pdf.set_font("Arial", "B", 10)
            for col in df.columns:
                pdf.cell(col_width, 10, str(col)[:15], border=1)
            pdf.ln()
            pdf.set_font("Arial", "", 10)
            for _, row in df.iterrows():
                for item in row:
                    pdf.cell(col_width, 10, str(item)[:15], border=1)
                pdf.ln()
        else:
            pdf.set_font("Arial", "", 12)
            pdf.cell(0, 10, "No SQL result data available.", ln=True)

        # Analysis
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 10, "LLM Analysis:", ln=True)
        pdf.set_font("Arial", "", 12)
        pdf.multi_cell(0, 10, analysis)

        return pdf.output(dest='S').encode('latin1')

# ================================
# ✅ STREAMLIT UI
# ================================

def main():
    st.title("📊 LLM + MySQL Clocking Analysis")
    st.sidebar.title("📁 Query History")
    
    if 'history' not in st.session_state:
        st.session_state.history = []

    selected_model = st.selectbox("Select Model", Config.MODEL_LIST, index=0)
    query = st.text_area("Enter your query:", placeholder="e.g. clocking Month Of Month selama 4 bulan untuk user juanrico.")
    submit_button = st.button("Submit")

    if submit_button and query:
        sql_id = LLM.detect_sql_query_type(query)
        if sql_id:
            username = LLM.extract_username(query)
            if not username:
                st.error("❌ Username not found in query.")
                return

            result = Database.run_query(Config.SQL_MAPPING[sql_id]["query"], (f"%{username}%",))
            if isinstance(result, str):
                st.error(result)
                return

            st.success("✅ SQL Executed Successfully")
            st.write("📊 Result:")
            st.json(result)

            st.info("🤖 Sending to LLM for analysis...")
            response, think = LLM.summarize(selected_model, result, query)
            
            # Display results
            st.markdown(f"**Summary:** {response}")
            if think:
                with st.expander("🧠 Thinking Process", expanded=False):
                    st.markdown(think)

            # Generate downloads
            if result:
                st.download_button(
                    label="📥 Download Excel (with Analysis)",
                    data=OutputGenerator.to_excel(result, query, think, response),
                    file_name="analysis_output.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                st.download_button(
                    label="📄 Download PDF Report",
                    data=OutputGenerator.to_pdf(query, result, f"[THINKING]\n{think}\n\n[RESPONSE]\n{response}"),
                    file_name="analysis_report.pdf",
                    mime="application/pdf"
                )

        else:
            st.info("🧠 No SQL matched. Sending directly to LLM...")
            response_container = st.empty()
            think_container = st.empty()
            try:
                response, think = LLM.stream_response(selected_model, query)
                if think:
                    with think_container.expander("🧠 Thinking Process", expanded=False):
                        st.markdown(think)
                if response:
                    response_container.markdown(f"**Response:** {response}")
                    st.session_state.history.append({"query": query, "response": response})
            except Exception as e:
                st.error(f"❌ Error: {e}")

    # Display history
    for i, entry in enumerate(st.session_state.history):
        with st.sidebar.expander(f"Q{i+1}: {entry['query'][:30]}..."):
            st.write(f"**Question:** {entry['query']}")
            st.write(f"**Answer:** {entry['response']}")

if __name__ == "__main__":
    main()