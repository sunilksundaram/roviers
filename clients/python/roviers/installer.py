"""
Roviers Native Binary Installer & Runner
Enables `pip install roviers` to download, manage, and launch native `roviers-server` and `roviers-cli` binaries.
"""

from __future__ import annotations
import os
import platform
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path

GITHUB_REPO = "sunilksundaram/roviers"
DEFAULT_INSTALL_DIR = Path.home() / ".roviers" / "bin"


def get_platform_asset_name() -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()

    if "windows" in system:
        return "roviers-windows-x86_64.zip"
    elif "darwin" in system:
        if "arm" in machine or "aarch64" in machine:
            return "roviers-macos-aarch64.tar.gz"
        return "roviers-macos-x86_64.tar.gz"
    elif "linux" in system:
        if "arm" in machine or "aarch64" in machine:
            return "roviers-linux-aarch64.tar.gz"
        return "roviers-linux-x86_64.tar.gz"
    else:
        raise RuntimeError(f"Unsupported operating system: {system} {machine}")


def get_binary_path(name: str = "roviers-server") -> Path:
    ext = ".exe" if platform.system().lower() == "windows" else ""
    binary_name = f"{name}{ext}"

    # 1. Check system PATH
    which_path = shutil.which(binary_name)
    if which_path:
        return Path(which_path)

    # 2. Check ~/.roviers/bin
    local_path = DEFAULT_INSTALL_DIR / binary_name
    if local_path.is_file() and os.access(local_path, os.X_OK):
        return local_path

    # 3. Check workspace target/release (if local repo build)
    target_path = Path.cwd() / "target" / "release" / binary_name
    if target_path.is_file():
        return target_path

    return local_path


def install_roviers(version: str = "latest", dest_dir: Path = DEFAULT_INSTALL_DIR) -> Path:
    """
    Installs the native roviers-server and roviers-cli binaries into ~/.roviers/bin
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    asset_name = get_platform_asset_name()

    if version == "latest":
        url = f"https://github.com/{GITHUB_REPO}/releases/latest/download/{asset_name}"
    else:
        url = f"https://github.com/{GITHUB_REPO}/releases/download/{version}/{asset_name}"

    archive_path = dest_dir / asset_name
    print(f"📦 Downloading Roviers native binaries from {url}...")
    try:
        urllib.request.urlretrieve(url, archive_path)
    except Exception as e:
        print(f"⚠️ Could not download pre-built binary: {e}")
        # Try cargo install as fallback
        if shutil.which("cargo"):
            print("🔨 Found cargo! Building and installing via cargo...")
            subprocess.check_call(["cargo", "install", "--git", f"https://github.com/{GITHUB_REPO}.git", "roviers-server", "roviers-cli"])
            print("✅ Successfully built and installed via cargo!")
            return get_binary_path("roviers-server")
        raise RuntimeError(f"Failed to install native binaries: {e}")

    # Extract archive
    print(f"📂 Extracting {archive_path.name} to {dest_dir}...")
    shutil.unpack_archive(archive_path, dest_dir)
    archive_path.unlink(missing_ok=True)

    # Ensure executable permissions on Unix
    for binary_name in ["roviers-server", "roviers-cli"]:
        p = dest_dir / binary_name
        if p.exists():
            p.chmod(0o755)

    print(f"✅ Roviers successfully installed to {dest_dir}")
    print(f"💡 Add {dest_dir} to your PATH to run `roviers-server` directly.")
    return get_binary_path("roviers-server")


def run_server(args: list[str] | None = None) -> None:
    """
    Launches the native `roviers-server` binary, installing it first if missing.
    """
    binary = get_binary_path("roviers-server")
    if not binary.is_file():
        print("🔍 roviers-server binary not found locally.")
        binary = install_roviers()

    cmd = [str(binary)] + (args or sys.argv[1:])
    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        pass
