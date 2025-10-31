PYTHON := python
UVICORN := uvicorn

.PHONY: install run refresh lint format

install:
	$(PYTHON) -m pip install -r backend/requirements.txt

run:
	$(UVICORN) backend.app.main:app --reload

refresh:
	$(PYTHON) backend/scripts/update_data.py

lint:
	$(PYTHON) -m ruff check backend

format:
	$(PYTHON) -m ruff format backend
