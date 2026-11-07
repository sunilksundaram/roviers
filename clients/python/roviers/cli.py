"""
Roviers Python CLI Entrypoints
"""

import sys
from .installer import install_roviers, run_server, get_binary_path


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "install":
        version = sys.argv[2] if len(sys.argv) > 2 else "latest"
        install_roviers(version=version)
    elif len(sys.argv) > 1 and sys.argv[1] == "server":
        run_server(sys.argv[2:])
    elif len(sys.argv) > 1 and sys.argv[1] == "which":
        path = get_binary_path("roviers-server")
        print(f"roviers-server binary: {path} (exists: {path.is_file()})")
    else:
        print("Roviers CLI & Server Manager (Python)")
        print("\nCommands:")
        print("  roviers install [version]    Download & install native binaries")
        print("  roviers server [args...]     Start native roviers-server")
        print("  roviers which                Display installed binary paths")


if __name__ == "__main__":
    main()
