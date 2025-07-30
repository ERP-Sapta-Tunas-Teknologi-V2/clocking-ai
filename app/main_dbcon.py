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

OLLAMA_URL = "http://localhost:11434/api/generate"
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",  # Fill if needed
    "database": "clocking_reports"
}

MODEL_LIST = ["qwen3:0.6b"]

# ================================
# ✅ SQL MAPPING (TOOLS)
# ================================

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
              AND MONTH(ca.start_date) BETWEEN %s AND %s
              AND YEAR(ca.start_date) = YEAR(CURDATE())
            GROUP BY DATE_FORMAT(ca.start_date, '%Y-%m')
            ORDER BY month ASC;
        """
    }
}

# ================================
# ✅ UTILITY FUNCTIONS
# ================================

def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def extract_username(user_query: str):
    match = re.search(r"(?:user|untuk user|analisa user)\s+(\w+)", user_query, re.IGNORECASE)
    return match.group(1) if match else None

def extract_time_range(user_query: str):
    match = re.search(r"(?:dari|selama)\s+(?:bulan\s+)?(\d+(?:-\d+)?)", user_query.lower())
    if match:
        time_range = match.group(1)
        if '-' in time_range:
            start, end = map(int, time_range.split('-'))
            return start, end
        return int(time_range), int(time_range)
    return 1, 3  # Default to 1-3 months if not found

def select_sql_tool(model, query):
    """Let LLM select the most appropriate SQL tool based on the query."""
    prompt = f"""Given the following SQL tools and their descriptions, select the most appropriate one for the user query. Return only the tool name (e.g., 'sql1' or 'sql2').\n\nTools:\n{json.dumps(SQL_MAPPING, indent=2)}\n\nQuery: {query}"""
    response = requests.post(OLLAMA_URL, json={"model": model, "prompt": prompt, "stream": False}).json()
    selected_tool = response.get("response", "").strip()
    return selected_tool if selected_tool in SQL_MAPPING else None

def run_query(sql, params=None):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, params)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results
    except Exception as e:
        return f"Database error: {e}"

def save_json_to_excel(json_data, filename="output.xlsx"):
    try:
        if isinstance(json_data, dict):
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                for key, value in json_data.items():
                    pd.DataFrame(value).to_excel(writer, sheet_name=key, index=False)
        elif isinstance(json_data, list):
            pd.DataFrame(json_data).to_excel(filename, index=False)
        else:
            raise ValueError("Unsupported JSON format")
        print(f"✅ Excel file saved as: {filename}")
    except Exception as e:
        print(f"❌ Error converting to Excel: {e}")

def generate_pdf(query: str, result_data, analysis: str) -> bytes:
    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)

    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, "LLM Analysis Report", ln=True)

    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, "Query:", ln=True)
    pdf.set_font("Arial", "", 12)
    pdf.multi_cell(0, 10, query)

    try:
        if isinstance(result_data, (list, tuple)):
            df = pd.DataFrame(result_data)
        elif isinstance(result_data, dict):
            df = pd.DataFrame([result_data])
        else:
            df = pd.DataFrame()
    except Exception:
        df = pd.DataFrame()

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

    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 10, "LLM Analysis:", ln=True)
    pdf.set_font("Arial", "", 12)
    pdf.multi_cell(0, 10, analysis)

    pdf_bytes = pdf.output(dest='S').encode('latin1')
    return pdf_bytes

def summarize_with_llm(model, result, query):
    url = "http://localhost:11434/api/generate"
    payload = {
        "model": model,
        "prompt": f"Berikut hasil query:\n{result}\n\nTolong buatkan ringkasan berdasarkan query ini: {query}",
        "stream": True
    }

    try:
        response = requests.post(url, json=payload, stream=True)
        response.raise_for_status()
    except requests.RequestException as e:
        return f"[Request error: {e}]", ""

    summary_container = st.empty()
    think_container = st.empty()
    summary = ""
    think_content = ""
    current_section = "response"

    for line in response.iter_lines():
        if not line:
            continue
        try:
            data = json.loads(line)
            chunk = data.get("response", "")
            if "<think>" in chunk:
                current_section = "think"
                think_content += re.sub(r'^<think>', '', chunk)
            elif "</think>" in chunk:
                current_section = "response"
                think_content += re.sub(r'</think>$', '', chunk)
            elif current_section == "think":
                think_content += chunk
            else:
                summary += chunk
                summary_container.markdown(f"**Summary:** {summary}")
        
            if think_content:
                with think_container.expander("Thinking Process", expanded=False):
                    st.markdown(think_content)
        except json.JSONDecodeError:
            print(f"[JSONDecodeError] Skipping invalid line: {line.decode(errors='ignore')}")

    return summary.strip() or "[No response from model]", think_content.strip() or ""

def stream_response(model, prompt):
    response = requests.post(
        OLLAMA_URL,
        json={"model": model, "prompt": prompt, "stream": True},
        stream=True
    )
    full_think = ""
    full_response = ""
    current_section = "response"
    for line in response.iter_lines():
        if line:
            try:
                data = json.loads(line.decode('utf-8'))
                if 'response' in data:
                    chunk = data['response']
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
                    yield {"think": full_think.strip(), "response": full_response.strip()}
            except json.JSONDecodeError:
                continue

# ================================
# ✅ STREAMLIT UI
# ================================

st.title("📊 LLM + MySQL Clocking Analysis")

st.sidebar.title("📁 Query History")
if 'history' not in st.session_state:
    st.session_state.history = []

selected_model = st.selectbox("Select Model", MODEL_LIST, index=0)

query = st.text_area("Enter your query:", placeholder="e.g. clocking Month Of Month selama 4 bulan untuk user juanrico.")
submit_button = st.button("Submit")

# ================================
# ✅ MAIN QUERY HANDLER
# ================================

if submit_button and query:
    sql_id = select_sql_tool(selected_model, query)
    if sql_id:
        username = extract_username(query)
        if not username:
            st.error("❌ Username not found in your query.")
        else:
            start_month, end_month = extract_time_range(query)
            sql_template = SQL_MAPPING[sql_id]["query"]
            params = (f"%{username}%",) if sql_id == "sql1" else (f"%{username}%", start_month, end_month)
            result = run_query(sql_template, params)
            if isinstance(result, str):  # Error
                st.error(result)
            else:
                st.success("✅ SQL Executed Successfully")
                st.write("📊 Result:")
                st.json(result)

                st.info("🤖 Sending to LLM for analysis...")
                final_analysis = summarize_with_llm(selected_model, result, query)

                final_response, final_think = final_analysis

                if isinstance(result, list) and result:
                    df_result = pd.DataFrame(result)

                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df_result.to_excel(writer, index=False, sheet_name="SQL Result")
                        df_summary = pd.DataFrame({
                            "Item": ["Query", "LLM Analysis Think", "LLM Analysis Response"],
                            "Content": [query, final_think, final_response]
                        })
                        df_summary.to_excel(writer, index=False, sheet_name="Analysis Summary")

                    st.download_button(
                        label="📥 Download Excel (with Analysis)",
                        data=output.getvalue(),
                        file_name="analysis_output.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

                st.download_button(
                    label="📄 Download PDF Report",
                    data=generate_pdf(query, result, f"[THINKING]\n{final_think}\n\n[RESPONSE]\n{final_response}"),
                    file_name="analysis_report.pdf",
                    mime="application/pdf"
                )
    else:
        st.info("🧠 No SQL matched. Sending directly to LLM...")
        response_container = st.empty()
        think_container = st.empty()
        try:
            full_think = ""
            full_response = ""
            for data in stream_response(selected_model, query):
                full_think = data["think"]
                full_response = data["response"]
                if full_think:
                    with think_container.expander("🧠 Thinking Process", expanded=False):
                        st.markdown(full_think)
                if full_response:
                    response_container.markdown(f"**Response:** {full_response}")
            if full_response:
                st.session_state.history.append({
                    "query": query,
                    "response": full_response
                })
        except Exception as e:
            st.error(f"❌ Error: {e}")

for i, entry in enumerate(st.session_state.history):
    with st.sidebar.expander(f"Q{i+1}: {entry['query'][:30]}..."):
        st.write(f"**Question:** {entry['query']}")
        st.write(f"**Answer:** {entry['response']}")