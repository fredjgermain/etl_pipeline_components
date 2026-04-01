
from pathlib import Path 

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data" 
ARTIFACTS = ROOT / 'artifacts' 
OUTPUT = ROOT / 'output' 

DATA_RAW = DATA / '01_raw' 
DATA_REF = DATA / '10_reference' 
DATA_PROCESS = DATA / '02_process' 
DATA_FINAL = DATA / '03_final' 


