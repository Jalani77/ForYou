The Eco-Stream project demonstrates a full-stack analytics pipeline designed for the Snowflake Data Cloud. It automates the ingestion of synthetic SaaS usage data, processes it via Snowpark, and visualizes consumption-based revenue metrics in a Streamlit dashboard.

The core features include data ingestion via Python scripts that generate synthetic usage, billing, and customer data for upload to Snowflake internal stages. It also utilizes analytics engineering with SQL models to calculate Credit Consumption Month-over-Month and Churn Risk scores.

The tech stack comprises the Snowflake Data Warehouse using Virtual Warehouses and RBAC, as well as SQL and Python. Key libraries used are snowflake-snowpark-python, pandas, and streamlit, all managed within VS Code or GitHub Codespaces.

The project structure is organized into a scripts folder for data generation, a SQL folder for schemas and business logic, and an app folder for the Streamlit dashboard. This tool enables SaaS companies to transition to a consumption-based model by identifying low-usage customers at risk of churn and high-growth accounts ready for upsell.
