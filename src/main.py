import sys
from pathlib import Path

# Ensure src/ is on the path when running as a script or frozen exe
sys.path.insert(0, str(Path(__file__).parent))

from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt

from app import App

APP_NAME = "Mesh Command Post"
APP_VERSION = "1.0.0"


def main() -> int:
    # High-DPI support
    QApplication.setHighDpiScaleFactorRoundingPolicy(
        Qt.HighDpiScaleFactorRoundingPolicy.PassThrough
    )

    qt_app = QApplication(sys.argv)
    qt_app.setApplicationName(APP_NAME)
    qt_app.setApplicationVersion(APP_VERSION)
    qt_app.setOrganizationName("MeshCommandPost")

    # Use Fusion style for a consistent cross-platform look
    qt_app.setStyle("Fusion")

    app = App()
    app.show()

    return qt_app.exec()


if __name__ == "__main__":
    sys.exit(main())
