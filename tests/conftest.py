import sys
import os

# Agrega backend/ al path para que los tests importen servicios directamente
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
