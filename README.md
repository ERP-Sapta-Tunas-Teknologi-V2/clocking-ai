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

### Detailed Migration Mapping (from MVP)

The following mapping details are merged from the MVP documentation to clarify how source tables/fields map into the `clocking_reports` schema.

#### Table: `users` (from `ss_user`)

| Field        | Description                                                      |
|--------------|------------------------------------------------------------------|
| `user_id`    | Unique identifier for each user (auto-generated)                 |
| `user_key`   | User key (mapped from `id_key` in the JSON)                      |
| `full_name`  | Full name of the user (mapped from `name` in `ss_user`)          |
| `email`      | User's email address (mapped from `email` in `ss_user`)          |
| `position`   | Position or role of the user (mapped from `jabatan`)             |
| `created_at` | Account creation date (mapped from `created_at` in `ss_user`)    |
| `updated_at` | Last update timestamp (mapped from `updated_at` in `ss_user`)    |

#### Table: `projects` (from `ss_project_management`)

| New Table Field       | Old Table Field (`ss_project_management`) | Description            |
|-----------------------|-------------------------------------------|------------------------|
| `project_code`        | `pr_project_code`                         | Project code           |
| `project_name`        | `pr_project_name`                         | Project name           |
| `customer_name`       | `pr_customer_name`                        | Customer name          |
| `project_manager_id`  | `pr_pic_project`                          | Project manager ID     |
| `created_by`          | `pr_created_by`                           | Creator of the project |
| `created_at`          | `pr_created_date`                         | Project creation date  |
| `last_update`         | `pr_last_update`                          | Last update timestamp  |
| `status`              | `pr_status`                               | Project status         |

Status Mapping:

```json
{
  "p": "progress",
  "f": "finished",
  "i": "initial",
  "c": "cancelled"
}
```

#### Table: `project_users` (from JSON `pr_members`)

| New Table Field | Old Table Field (`pr_members`) | Description        |
|-----------------|--------------------------------|--------------------|
| `project_code`  | `pr_project_code`              | Project code       |
| `user_id`       | `user_id`                      | User ID            |
| `role`          | `role`                         | Role in the project|

Note: In the current implementation, `project_users` contains `project_code` and `user_id`. The `role` field can be added later if needed.

#### Table: `daily_activities` (from JSON `da_data`)

| New Table Field           | Old Table Field (`da_data`) | Description                         |
|---------------------------|------------------------------|-------------------------------------|
| `daily_activity_id`       | `da_id`                      | Unique ID for daily activity        |
| `project_code`            | `da_project_code`            | Project code                        |
| `activity_date`           | `da_date`                    | Date of activity                    |
| `priority`                | `da_priority`                | Activity priority                   |
| `start_time`              | `da_start_tm`                | Start time of activity              |
| `end_time`                | `da_end_tm`                  | End time of activity                |

#### Table: `category_clocking` (from `ss_category_clocking`)

| `ss_category_clocking` Field | `category_clocking` Field | Description              |
|------------------------------|---------------------------|--------------------------|
| `cc_id`                      | `category_id`             | Category ID              |
| `cc_definition`              | `category_description`    | Category description     |
| `cc_productive`              | `is_productive`           | Indicates if productive  |
| `cc_billable`                | `is_billable`             | Indicates if billable    |
| `cc_direct`                  | (Not mapped, skipped)     | -                        |
| `cc_used`                    | `is_used`                 | Indicates if used        |

#### Table: `daily_activity` (from `da_activity`)

| `daily_activity` Field        | `da_activity` Field  | Description                              |
|-------------------------------|----------------------|------------------------------------------|
| `daily_activity_id`           | `da_id`              | Unique ID for daily activity             |
| `project_code`                | `da_project_code`    | Project code                             |
| `activity_date`               | `da_date`            | Date of activity                         |
| `priority`                    | `da_priority`        | Activity priority                        |
| `start_time`                  | `da_start_tm`        | Start time of activity                   |
| `end_time`                    | `da_end_tm`          | End time of activity                     |
| `created_by`                  | `da_created_by`      | Creator of the activity                  |
| `created_at`                  | `da_created_date`    | Creation date of activity                |
| `updated_at`                  | `da_updated_date`    | Last update timestamp                    |
| `activity_type`               | `da_activity`        | Type of activity                         |
| `description`                 | `da_keterangan`      | Activity description                     |
| `activity_duration_minutes`   | `da_duration`        | Duration of activity in minutes          |
| (Mapped to `user_id`)         | `da_user_id`         | User ID (assumed to map to `created_by` or similar) |

## Migration

This project includes migration scripts to import and normalize data from the legacy database `system-smartpro` into the target database `clocking_reports`.

### Overview
- Location: `migration/`
- Source DB: `system-smartpro`
- Target DB: `clocking_reports`
- Idempotency: Scripts use duplicate checks where applicable; prefer running in the order below.

### Run Order
- `migration_user.py` → imports base user records.
- `migration_project.py` → imports projects and status mapping.
- `migration_project_user.py` → links projects to users (creates users if missing).
- `migration_category_clocking.py` → imports category definitions.
- `migration_clocking_activities.py` → imports daily activities (auto-create parents) and clocking activities, then backfills fields.

### How to Run
- Open terminal in `c:\laragon\www\clocking-ai\migration`.
- Execute scripts one by one:
  - `python migration_user.py`
  - `python migration_project.py`
  - `python migration_project_user.py`
  - `python migration_category_clocking.py`
  - `python migration_clocking_activities.py`

### Script Details
- `migration_project_user.py`
  - Reads `ss_project_management.pr_members` JSON with fields like `email`, `id_key`, `jabatan`, `nickname`.
  - Looks up users by `email`; if not found or email missing, creates a placeholder user with `id_key@placeholder.local`.
  - Generates `user_id` using `MAX(user_id)+1` to handle tables without `AUTO_INCREMENT`.
  - Prevents duplicate `(project_code, user_id)` inserts in `project_users`.
  - Typical outcome: previously skipped rows due to missing emails are inserted; existing pairs are ignored.

- `migration_clocking_activities.py`
  - Computes `duration_minutes` from `start_date`/`start_time` and `end_date`/`end_time` when missing; falls back to `0` if still `NULL`.
  - Maps `task_id` based on `activity_description`: `remote|wfh` → `1`, `onsite|wfo` → `7`, otherwise `NULL`.
  - Converts any existing `task_id=0` to `NULL`.
  - Auto-creates missing parent `daily_activities` when absent (maps `priority` and optional `user_id` from `id_key`).
  - Runs a backfill step automatically after insertion and prints verification counts for `task_id` and `duration_minutes`.

- `migration_user.py`
  - Imports base users from `ss_user` into `users`.
  - Maps common fields (`full_name`, `email`, `position`, timestamps). Optional keys like `user_key` can be added if present.

- `migration_project.py`
  - Imports `projects` from `ss_project_management` with status mapping:
    - `p` → `progress`, `f` → `finished`, `i` → `initial`, `c` → `cancelled`.

- `migration_category_clocking.py`
  - Imports category definitions from `ss_category_clocking` into `category_clocking`.

### Verification SQL
- `SELECT COUNT(*) FROM clocking_activities WHERE task_id = 0;` → should be `0`.
- `SELECT COUNT(*) FROM clocking_activities WHERE task_id IS NULL;` → expected for unknown tasks.
- `SELECT COUNT(*) FROM clocking_activities WHERE duration_minutes IS NULL;` → should be `0` after backfill.
- `SELECT COUNT(*) FROM project_users pu LEFT JOIN users u ON pu.user_id = u.user_id WHERE u.user_id IS NULL;` → should be `0`.
- `SELECT COUNT(*) FROM users WHERE email LIKE '%@placeholder.local';` → count of placeholder users created.

### Notes & Troubleshooting
- Re-running scripts: `project_users` is protected from duplicates; for other tables, avoid double-inserting unless scripts include explicit duplicate checks.
- Error `Field 'user_id' doesn't have a default value`: ensure the migration creates `user_id` using `MAX(user_id)+1` or set `AUTO_INCREMENT` on the table.
- Legacy `user_key` lookups were replaced by email-based lookups in `migration_project_user.py`; ensure the script version used reflects this change.
- Customize placeholder domain by editing `id_key@placeholder.local` in `migration_project_user.py` if needed.

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