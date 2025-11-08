#!/usr/bin/env python3

import os
import subprocess
import invoke
import psutil
import colorama
import shutil

PROJECT_NAME = os.path.basename(os.getcwd())
BINARIES = ["monodb", "mdb"]
GUI_DIR = os.path.join(os.getcwd(), "gui")
TAURI_DIR = os.path.join(GUI_DIR, "src-tauri")
MANDIR = "/usr/share/man/man1"  # Only on Unix


def get_cores():
    cpu_count = psutil.cpu_count(logical=True) or 4
    available_gb = psutil.virtual_memory().available / (1024**3)

    max_by_cpu = min(cpu_count * 2, 32)
    max_by_memory = min(max(int(available_gb) - 2, 0), 32)

    optimal = max(1, min(max_by_cpu, max_by_memory))

    if optimal > 1 and optimal % 2 == 1:
        optimal -= 1

    return optimal


CORES = get_cores()


def run_cmd(c, command, **kwargs):
    """Run command with colored output preserved."""
    cwd = kwargs.get("cwd", os.getcwd())
    print(
        f"{colorama.Fore.BLUE}▸{colorama.Style.RESET_ALL} {colorama.Style.DIM}{command}{colorama.Style.RESET_ALL}"
    )

    env = os.environ.copy()
    env.update(
        {
            "CARGO_TERM_COLOR": "always",
            "FORCE_COLOR": "1",
            "CLICOLOR_FORCE": "1",
            "NPM_CONFIG_COLOR": "always",
        }
    )

    if "env" in kwargs:
        env.update(kwargs["env"])

    try:
        result = subprocess.run(command, shell=True, env=env, cwd=cwd, check=False)
        return result
    except Exception as e:
        print(
            f"{colorama.Fore.RED}Error running command: {e}{colorama.Style.RESET_ALL}"
        )
        raise


@invoke.task
def help(c):
    """Show available tasks and environment configuration."""
    colorama.init(autoreset=True)

    tasks = [
        ("help", "Show this help message"),
        ("build", "Build the project"),
        ("clean", "Clean build artifacts"),
        ("test", "Run tests"),
        ("lint", "Run linters"),
        ("format", "Format the code"),
        ("docs", "Generate documentation"),
        ("package", "Package the application"),
        ("install", "Install the application"),
        ("uninstall", "Uninstall the application"),
    ]

    print(f"{colorama.Style.BRIGHT}Usage:{colorama.Style.RESET_ALL} inv <task>\n")
    print(f"{colorama.Style.BRIGHT}Available Tasks:{colorama.Style.RESET_ALL}")
    for task, description in tasks:
        print(
            f"  {colorama.Fore.CYAN}{task:<13}{colorama.Style.RESET_ALL} {description}"
        )

    print(
        f"\n{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Using {colorama.Style.BRIGHT}{CORES}{colorama.Style.RESET_ALL} cores for building."
    )


@invoke.task
def deps(c):
    """Install necessary dependencies."""
    print(
        f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Installing dependencies..."
    )
    run_cmd(c, "cargo install cargo-audit cargo-tarpaulin")
    run_cmd(c, "npm install", cwd=TAURI_DIR)
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Dependencies installed.")


def install_gui_deps(c):
    """Install GUI dependencies."""
    print(
        f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Installing GUI dependencies..."
    )
    run_cmd(c, "npm install", cwd=TAURI_DIR)
    print(
        f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} GUI dependencies installed."
    )


def build_gui(c):
    """Build the GUI using Tauri."""
    install_gui_deps(c)
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Building GUI...")
    run_cmd(c, "npm run tauri build", cwd=TAURI_DIR)
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} GUI build complete.")


@invoke.task
def build(c, release=False):
    """Build the project."""
    print(
        f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Building project with {CORES} cores..."
    )
    run_cmd(c, f"cargo build {'--release' if release else ''} -j {CORES}")
    # build_gui(c)
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Build complete.")


@invoke.task
def clean(c):
    """Clean build artifacts."""
    print(
        f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Cleaning build artifacts..."
    )
    run_cmd(c, "cargo clean")
    run_cmd(c, "cargo clean", cwd=TAURI_DIR)
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Clean complete.")


@invoke.task
def test(c):
    """Run tests."""
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Running tests...")
    run_cmd(c, "cargo test --all")
    run_cmd(c, "cargo tarpaulin --out Html")
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Tests complete.")


@invoke.task
def lint(c):
    """Run linters."""
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Running linters...")
    run_cmd(c, "cargo fmt -- --check")
    run_cmd(c, "cargo clippy -- -D warnings")
    run_cmd(c, "npm run lint", cwd=TAURI_DIR)
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Linting complete.")


@invoke.task
def format(c):
    """Format the code."""
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Formatting code...")
    run_cmd(c, "cargo fmt")
    run_cmd(c, "npm run format", cwd=TAURI_DIR)
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Formatting complete.")


@invoke.task
def docs(c):
    """Generate documentation."""
    print(
        f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Generating documentation..."
    )
    run_cmd(c, "cargo doc --no-deps --document-private-items")
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Documentation generated.")


@invoke.task
def package(c):
    """Package the application."""
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Packaging application...")
    build(c, release=True)
    # Create dist/
    dist_dir = "./dist"
    if not os.path.exists(dist_dir):
        os.makedirs(dist_dir)

    # List target and find all executables
    target_dir = "./target/release"
    for binary in BINARIES:
        # Add .exe extension on Windows
        binary_name = f"{binary}.exe" if os.name == "nt" else binary
        src_path = os.path.join(target_dir, binary_name)
        if os.path.isfile(src_path):
            dest_path = os.path.join(dist_dir, binary_name)
            shutil.copy2(src_path, dest_path)
            print(
                f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Packaged {binary_name}."
            )
        else:
            print(
                f"{colorama.Fore.YELLOW}Warning: {binary_name} not found in {target_dir}, skipping.{colorama.Style.RESET_ALL}"
            )

    # Copy tauri binary
    tauri_binary_name = "mdb-admin.exe" if os.name == "nt" else "mdb-admin"
    tauri_binary_path = os.path.join(TAURI_DIR, "target", "release", tauri_binary_name)
    if os.path.isfile(tauri_binary_path):
        dest_path = os.path.join(dist_dir, tauri_binary_name)
        shutil.copy2(tauri_binary_path, dest_path)
        print(
            f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Packaged {tauri_binary_name}."
        )
    else:
        print(
            f"{colorama.Fore.YELLOW}Warning: Tauri binary not found at {tauri_binary_path}, skipping.{colorama.Style.RESET_ALL}"
        )

    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Packaging complete.")
