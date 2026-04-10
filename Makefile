.PHONY: setup ingest-bronze transform-silver transform-gold test clean

setup:
	python -m venv .venv
	.venv/Scripts/pip install -r requirements.txt

ingest-bronze:
	python -m src.extractors.eersa_generacion_extractor

transform-silver:
	python -m src.transformations.silver_generacion

transform-gold:
	python -m src.transformations.gold_generacion

test:
	python -m pytest tests/ -v

clean:
	rm -rf data/bronze/* data/silver/* data/gold/*
	@echo "Datos procesados eliminados"
