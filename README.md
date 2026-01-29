What this does:
This take the CSV data in the data folder, changes it, and puts it into a SQLite DB (Extract, Transform, and Load)
This is only designed with these two CSVs in mind

Steps to run
1. Create an .venv (optional but recomended): "python -m venv .venv" & ".venv\Scripts\activate"
2. run: "pip install pandas sqlalchemy ipykernel"
3. Run "scripts/etl_process.py"
