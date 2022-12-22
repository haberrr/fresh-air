SHELL := /bin/bash
VENV = $(shell poetry env info --path)

.PHONY: install-dev add-kernel

install-dev:
	@echo "Installing development dependencies..."
	poetry install --with dev

add-kernel:
	@echo "Adding IPython kernel..."
	source $(VENV)/bin/activate; python -m ipykernel install --user --name "turbiner"
