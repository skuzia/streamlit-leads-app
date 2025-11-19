# run_app.py
import os
import sys
from pathlib import Path

# מנקים כל משתנה סביבה שקשור ל STREAMLIT
for k in list(os.environ.keys()):
    if k.upper().startswith("STREAMLIT_"):
        os.environ.pop(k, None)

import streamlit.web.cli as stcli


def resource_path(rel_path: str) -> str:
    """
    מחזיר נתיב לקובץ גם מתוך EXE (אחרי PyInstaller)
    """
    if hasattr(sys, "_MEIPASS"):
        base_path = Path(sys._MEIPASS)
    else:
        base_path = Path(__file__).resolve().parent
    return str(base_path / rel_path)


if __name__ == "__main__":
    app_path = resource_path("app.py")  # אם הקובץ שלך בשם אחר - תעדכן כאן
    print("Using app path:", app_path)

    # מריץ כאילו כתבת: streamlit run app.py
    sys.argv = ["streamlit", "run", app_path]
    stcli.main()
