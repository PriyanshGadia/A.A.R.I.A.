"""
A.A.R.I.A System Launcher
This script ensures proper module imports and starts the system.
"""
import os
import sys
from pathlib import Path

def setup_python_path():
    """Add the project root to Python path."""
    project_root = Path(__file__).parent.absolute()
    sys.path.insert(0, str(project_root))
    os.environ["PYTHONPATH"] = str(project_root)

def main():
    """Main entry point."""
    setup_python_path()
    
    # Import after path setup
    from backend.main import main as aaria_main
    import asyncio
    
    # Run the system
    try:
        exit_code = asyncio.run(aaria_main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nA.A.R.I.A system terminated by user.")
        sys.exit(0)
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()