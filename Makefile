# Makefile for Running Races Insights project

# Variables
PYTHON = python # Or specific path to your python3 if needed, e.g., python3
SCRIPT_DIR = scripts
ELT_DIR = elt
INTELLIGENT_UPDATER_SCRIPT = $(SCRIPT_DIR)/intelligent_race_updater.py
EXTRACT_SCRIPT = $(ELT_DIR)/extract.py
TRANSFORM_SCRIPT = $(ELT_DIR)/transform.py

# Default target: runs when you just type `make`
.PHONY: all
all: update

# Target to run the full update process (ELT + Intelligent Updater)
# The intelligent_race_updater.py script calls the ELT scripts internally.
.PHONY: update
update:
	@echo "Starting full update process (Intelligent Updater will call ELT scripts)..."
	$(PYTHON) $(INTELLIGENT_UPDATER_SCRIPT)
	@echo "Full update process finished."

# Target to only run the ELT (extract and transform) process separately
.PHONY: elt
elt:
	@echo "Running ELT process (extract and transform)..."
	$(PYTHON) $(EXTRACT_SCRIPT)
	$(PYTHON) $(TRANSFORM_SCRIPT)
	@echo "ELT process finished."

# Target to set up the environment (e.g., install dependencies)
# Assumes requirements.txt is in the elt directory as per previous context
.PHONY: setup
setup:
	@echo "Installing dependencies from $(ELT_DIR)/requirements.txt..."
	pip install -r $(ELT_DIR)/requirements.txt
	@echo "Dependencies installed."

# Target for cleaning temporary files (optional)
.PHONY: clean
clean:
	@echo "Cleaning temporary Python files..."
	find . -type f -name '*.py[co]' -delete
	find . -type d -name '__pycache__' -exec rm -rf {} + 
	@echo "Clean up complete." 