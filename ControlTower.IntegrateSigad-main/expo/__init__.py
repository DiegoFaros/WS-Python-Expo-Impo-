from .database_expo import run_database_expo
from .soap_expo import run_soap_expo

def run_expo():
    run_database_expo()
    run_soap_expo()
