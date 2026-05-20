VENV := .venv
PY := $(VENV)/bin/python3
PIP := $(VENV)/bin/pip

.PHONY: install venv run-api clean

install: venv
	$(PIP) install -U pip
	$(PIP) install -r requirements.txt

venv:
	python3 -m venv $(VENV)

run-api:
	$(VENV)/bin/uvicorn main:app --host 0.0.0.0 --reload

clean:
	rm -rf $(VENV) __pycache__ .pytest_cache
