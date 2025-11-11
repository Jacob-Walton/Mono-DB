#!/usr/bin/env python3

import os
import shutil
import subprocess

import colorama
import invoke
import psutil

PROJECT_NAME = os.path.basename(os.getcwd())
BINARIES = ["monod", "mdb", "monodb-admin"]
GUI_TARGET = "monodb-admin"
MANDIR = "/usr/share/man/man1"  # Only on Unix


def get_cores():
    cpu_count = psutil.cpu_count(logical=True) or 4
    total_gb = psutil.virtual_memory().total / (1024**3)

    # Rust builds are CPU-bound; assume around 1GB per job
    memory_limited_cores = int(total_gb / 1.0)

    # Favour CPU, only constrain if memory clearly insufficient
    optimal = min(cpu_count, max(memory_limited_cores, cpu_count // 2))
    optimal = max(1, min(optimal, 32))

    # Prefer even numbers for better scheduling
    if optimal > 4 and optimal % 2 != 0:
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
        ("build-gui", "Build the GUI."),
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
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Dependencies installed.")


@invoke.task
def build_gui(c, release=False):
    """Build the GUI."""
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Building GUI...")
    run_cmd(
        c,
        f"cargo build {'--release' if release else ''} -p {GUI_TARGET} -j {get_cores()}",
    )
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} GUI build complete.")


@invoke.task
def build(c, release=False):
    """Build the project."""
    print(
        f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Building project with {CORES} cores..."
    )
    run_cmd(c, f"cargo build {'--release' if release else ''} -j {CORES}")
    build_gui(c, release)
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Build complete.")


@invoke.task
def clean(c):
    """Clean build artifacts."""
    print(
        f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Cleaning build artifacts..."
    )
    run_cmd(c, "cargo clean")
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
    run_cmd(c, "cargo clippy --all-targets -- -D warnings")
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Linting complete.")


@invoke.task
def format(c):
    """Format the code."""
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Formatting code...")
    run_cmd(c, "cargo fmt")
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

    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Packaging complete.")
