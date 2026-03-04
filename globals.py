# -*- coding: utf-8 -*-
"""
-------------------------------------------------------------------------------------------------------------------
    Written by      : Rob Lewis

    Date            : 07SEP2025

    Purpose         : Global variables for environment setup

    Dependencies    :

    Program name    : globals

    Modifications
    -------------
    07SEP2025   RLEWIS  Initial Version 
    27OCT2025   RLEWIS  Updated to work in Windows
    01NOV2025   RLEWIS  Added project root logic that takes airflow process into account
-------------------------------------------------------------------------------------------------------------------
"""

# ---------------
# --- Imports ---
# ---------------

import sys, os
from pathlib import Path

        # ---------------
   
# ------------------------
# --- Global variables ---
# ------------------------

# Set a fallback/default value
project_root_env_var = os.environ.get("PROJECT_ROOT")

if project_root_env_var:

    # Use the path defined in the Docker Compose environment
    project_root = project_root_env_var

else:

    # obtain current file information
    current_file = Path(__file__).resolve()

    # Assume project root name
    project_root_name = "qa-library-fresh"

    # Look through all parent folders
    project_root = next((p for p in current_file.parents if p.name == project_root_name), None)

    if project_root is None:
        raise RuntimeError(f"Could not find {project_root_name} in path hierarchy")

# add the common directory to sys.path
sys.path.append(str(project_root)+'/src')

# thus enabling import of autoexec file
#from src.common.autoexec import *