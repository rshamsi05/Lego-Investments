'''
Pytest configuration file - adds project root to Python path
'''
import sys
from pathlib import Path

# Add the project root to the Python path so tests can import modules
# This allows tests to import 'storage', 'ingestion', 'config', etc.
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
