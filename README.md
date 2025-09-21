# LLM + MySQL Clocking Analysis App

[![Streamlit](https://img.shields.io/badge/Streamlit-FF6B35?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org/)
[![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)](https://mysql.com/)
[![Plotly](https://img.shields.io/badge/Plotly-239120?style=for-the-badge&logo=plotly&logoColor=white)](https://plotly.com/)
![Ollama](https://img.shields.io/badge/Ollama-EE7F00?style=for-the-badge&logo=ollama&logoColor=white)

A Streamlit-powered web application that integrates Large Language Models (LLM) with MySQL for advanced clocking time analysis. This app allows users to query clocking data using natural language, execute SQL queries, generate LLM summaries, visualize data with interactive charts (using Plotly), and export reports to Excel and PDF. It supports user-specific analysis, over/under clocking detection, team reports, and historical query management.

The app is designed for HR, project managers, or teams tracking time clocking data, providing insights into productivity, efficiency, and trends.

## Table of Contents
- [Features](#features)
- [Supported Queries](#supported-queries)
- [Database Schema](#database-schema)
- [prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgments](#acknowledgments)

## Features

- **Natural Language Query Processing**: Detect and map user queries to predefined SQL templates using keyword synonyms and regex patterns.
- **SQL Query Execution**: Securely execute MySQL queries with parameterized inputs to prevent SQL injection.
- **LLM Integration**: Use Ollama (local LLM) to generate summaries and analysis of query results.
- **Interactive Visualizations**: Generate Plotly line charts for month-over-month clocking trends (sql5 queries).
- **Report Generation**:
  - Excel export with embedded charts and analysis.
  - PDF reports using ReportLab, including data tables and images.
- **Query History Management**: Sidebar with truncated history entries; "View Report" buttons open modal popups for full details.
- **Session Persistence**: Results and history persist across interactions using Streamlit session state.
- **Flexible Query Types**:
  - User clocking totals by category (sql1).
  - Top 5 over/under clocking users (sql2).
  - Single-month efficiency analysis (sql3).
  - Multi-month clocking vs. target comparison (sql4).
  - 4-month trend visualization (sql5).
  - Team PM clocking report (sql6).

## Supported Queries

The app maps natural language queries to SQL using synonyms and patterns. Examples:

- **sql1**: "jumlah clocking untuk user juan dengan detail category" → Totals by category.
- **sql2**: "Top 5 over clocking & under clocking" → Top users by clocking status.
- **sql3**: "analisa user juan maret" → Efficiency for March.
- **sql4**: "analisa user juan bulan 2-9" → Multi-month vs. target (supports any range, e.g., 1-3, 2-9).
- **sql5**: "grafik clocking Month Of Month selama 4 bulan untuk user juan" → 4-month trend chart (category 400).
- **sql6**: "Report clocking untuk Tim PM" → PM team report.

Fallback: Unmatched queries are sent directly to LLM for free-form responses.

## Database Schema

This schema will be further visualized in  [mvp folder](mvp).The app assumes a MySQL database named `clocking_reports` with the following tables:

### `users`
- `user_id` (INT, PK)
- `full_name` (VARCHAR)
- `email` (VARCHAR)
- `position` (TINYINT)
- `created_at` (TIMESTAMP)
- `updated_at` (TIMESTAMP)

### `daily_activities`
- `daily_activity_id` (INT, PK)
- `project_code` (VARCHAR)
- `activity_date` (DATETIME)
- `priority` (ENUM)
- `start_time` / `end_time` (TIMESTAMP)
- `created_by` (VARCHAR)
- `created_at` / `updated_at` (TIMESTAMP)
- `activity_type` (ENUM)
- `description` (TEXT)
- `activity_duration_minutes` (INT)
- `user_id` (INT, FK to `users.user_id`)

### `clocking_activities`
- `activity_id` (INT, PK)
- `daily_activity_id` (INT, FK to `daily_activities.daily_activity_id`)
- `task_id` (INT)
- `activity_description` (TEXT)
- `duration_minutes` (INT)
- `start_date` / `end_date` (DATE)
- `start_time` / `end_time` (TIME)
- `category_id` (INT, FK to `category_clocking.category_id`)

### `category_clocking`
- `category_id` (INT, PK)
- `category_description` (VARCHAR)
- `is_productive` / `is_billable` / `is_used` / `is_direct` (TINYINT)

### `project_users` (for sql6)
- `project_code` (VARCHAR)
- `user_id` (INT)

### `projects` (for sql6)
- `project_code` (VARCHAR, PK)
- `project_name` (VARCHAR)
- `customer_name` (VARCHAR)
- `project_manager_id` (INT)
- `created_by` (VARCHAR)
- `created_at` / `last_update` (TIMESTAMP)
- `status` (ENUM)

**Notes**:
- Queries use `LIKE %username%` for user matching.
- Overclocking: >40 hours/week; Underclocking: <40 hours/week.
- Target: 40 hours/week * 4 weeks/month.

## Prerequisites

- **Python 3.8+**
- **MySQL Database**: With tables `users`, `category_clocking`, `daily_activities`, `clocking_activities`, `project_users`, `projects`.
- **Ollama**: Running locally on `http://localhost:11434` with model `qwen3:0.6b`.
- **Hardware**: Sufficient RAM for LLM inference (e.g., 4GB+ for small models).

## Installation

1. **Clone the Repository**:
   ```
   git clone <repo-url>
   cd clocking-ai/app
   ```


2. **Install Dependencies**:
   ```
   pip install streamlit plotly pandas mysql-connector-python openpyxl reportlab kaleido streamlit-modal fpdf2
   ```

3. **Configure Database**:
   - Update `DB_CONFIG` in `app_grok.py` with your MySQL credentials:
     ```python
     DB_CONFIG = {
         "host": "localhost",
         "user": "root",
         "password": "your_password",
         "database": "clocking_reports"
     }
     ```
   - Create the database and tables using the schema above.
   - Insert dummy data for testing (see "Testing" section).

4. **Configure Ollama (LLM)**:
   - Install Ollama: Download from [ollama.com](https://ollama.com).
   - Pull a model (e.g., `ollama pull qwen3:0.6b`).
   - Update `MODEL_LIST` in `app_grok.py` if needed.
   - Ensure Ollama runs on `http://localhost:11434`.

5. **Run the App**:
   ```
   streamlit run app_grok.py
   ```
   - Open `http://localhost:8501` in your browser.

## Usage

1. **Select Model**: Choose an LLM model from the dropdown (e.g., `qwen3:0.6b`).
2. **Enter Query**: Type a natural language query in the text area (e.g., "analisa user juan bulan 1-3").
3. **Submit**: Click "Submit" to execute SQL (if matched) and LLM analysis.
4. **View Results**: See SQL results (JSON), chart (for sql5), and LLM summary on the main panel.
5. **History**: Check the sidebar for past queries; click "View Report" for a popup with full details and downloads.
6. **Downloads**: Export to Excel (with chart for sql5) or PDF.

**Example Queries**:
- "jumlah clocking untuk user juan" → Category totals.
- "Top 5 over clocking" → Over/under clocking leaders.
- "grafik clocking 4 bulan user juan" → Trend chart.

## Configuration

- **Ollama URL**: Edit `OLLAMA_URL = "http://localhost:11434/api/generate"` for remote LLM.
- **SQL Mappings**: Customize `SQL_MAPPING` in `Config` class for new queries.
- **Synonyms**: Update `KEYWORD_SYNONYMS` in `LLM` class for query detection.
- **Targets**: Adjust 40-hour weekly target in SQL queries if needed.

## Troubleshooting

- **No SQL Matched**: Query doesn't match patterns; refine synonyms or add to `detect_sql_query_type`.
- **Database Error**: Check `DB_CONFIG`; ensure tables exist and data is inserted.
- **LLM Error**: Verify Ollama is running; check model name in `MODEL_LIST`.
- **Chart Not Showing**: Ensure dummy data for category 400; install `kaleido` for image export.
- **Deprecation Warning**: Upgrade Streamlit and `streamlit-modal`; suppress with `warnings.filterwarnings` if needed.
- **Modal Not Opening**: Ensure `streamlit-modal` is installed (`pip install streamlit-modal`); rerun app.

## Contributing

1. Fork the repo.
2. Create a branch: `git checkout -b feature-branch`.
3. Commit changes: `git commit -m "Add feature"`.
4. Push: `git push origin feature-branch`.
5. Open a PR.

## License

MIT License. See [LICENSE](LICENSE) for details.

## Acknowledgments

- [Streamlit](https://streamlit.io/) for the UI framework.
- [Ollama](https://ollama.com/) for local LLM.
- [Plotly](https://plotly.com/) for visualizations.
- [streamlit-modal](https://github.com/jrieke/streamlit-modal) for popups.
- MySQL for the database backend.

For issues or questions, open a GitHub issue.