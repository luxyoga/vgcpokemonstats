VGC Usage Stats Pipeline

📊 Project Overview

This project builds a data pipeline that ingests competitive Pokémon VGC usage data, cleans and structures it, loads it into a DuckDB database, and produces insights & visualizations.

It demonstrates practical data engineering skills:
	•	Data ingestion (parsing raw .txt stats from Smogon/Showdown)
	•	ETL pipeline (extract → transform → load)
	•	SQL analytics (aggregations, joins, window functions)
	•	Visualization (Python, matplotlib/seaborn)
	•	Optional dashboard layer (Streamlit)

⸻

🚀 Motivation

Competitive Pokémon is full of stats on which Pokémon, items, and abilities dominate the meta. These files are public but messy and unstructured.

This project shows how to:
	1.	Turn unstructured raw text into clean, queryable tables.
	2.	Use SQL to answer real meta questions (e.g., “What are the top 50 Pokémon and their signature items?”).
	3.	Build a reusable pipeline that updates as new monthly stats drop.

⸻

🛠 Tech Stack
	•	Python (data parsing, ETL scripts)
	•	Pandas (intermediate cleaning)
	•	DuckDB (lightweight analytics warehouse)
	•	SQL (joins, window functions, aggregations)
	•	matplotlib / seaborn (visualization)
	•	Streamlit (optional, for dashboard)

⸻

📂 Project Structure

<img width="933" height="292" alt="Project Structure" src="https://github.com/user-attachments/assets/83f3fb5a-cc51-400b-bdae-e0d28ef0969c" />
