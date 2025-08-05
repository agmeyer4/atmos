# GRA2PES Workflow

This module contains scripts to download and regrid the GRA2PES emissions inventory dataset. 

## Contents

- `gra2pes_base_creator.py` — Downloads raw GRA2PES emissions data and organizes it into a structured directory hierarchy.
- `gra2pes_regrid.py` — Regrids the downloaded GRA2PES files to a specified spatial resolution and grid.

## Requirements

- A valid GRA2PES FTP or HTTPS credentials file.
- Configuration of parameters such as data paths, years, and sectors through configuration files:
  - `configs/gra2pes/gra2pes_config.yaml` — user-editable YAML configuration file.
  - `configs/gra2pes/gra2pes_config.py` — Python configuration class that wraps the YAML with additional logic.

## Configuration

Adjust parameters and settings in the YAML config file (`configs/gra2pes/gra2pes_config.yaml`). The Python config class reads from this file and provides convenient access to configuration values and derived paths.

## Usage

Run these scripts from the root of the `atmos` repository using Python’s module execution syntax. For example:

```bash
python -m workflows.inventories.gra2pes.gra2pes_base_creator
python -m workflows.inventories.gra2pes.gra2pes_regrid
```

This ensures that imports work correctly with the package structure.


