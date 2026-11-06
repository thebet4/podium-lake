from pathlib import Path

# src/podium_lake/paths.py -> project root is three levels up
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
