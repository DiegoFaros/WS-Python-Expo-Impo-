from .database_impo import run_database_impo
from .soap_impo import run_soap_impo

def run_impo():
    run_database_impo()
    run_soap_impo()
