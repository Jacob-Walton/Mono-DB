#!/usr/bin/env python3

import os
import shutil
import subprocess

import colorama
import invoke
import psutil

PROJECT_NAME = os.path.basename(os.getcwd())
BINARIES = ["monod", "mdb"]
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
GUI_DIR = "./monodb-admin"
TAURI_DIR = os.path.join(GUI_DIR, "src-tauri")


def run_cmd(c, command, **kwargs):
    """Run command with colored output preserved."""
    cwd = kwargs.get("cwd", os.getcwd())
    check = kwargs.get("check", True)
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
        result = subprocess.run(command, shell=True, env=env, cwd=cwd, check=check)
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
        ("ci", "Run CI checks (lint, test, build)"),
        ("build", "Build the project"),
        ("build-gui", "Build the GUI (Next.js + Tauri)"),
        ("build-frontend", "Build Next.js frontend only"),
        ("build-tauri", "Build Tauri backend only"),
        ("dev-gui", "Run GUI in development mode"),
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
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Installing GUI dependencies...")
    run_cmd(c, "yarn install", cwd=GUI_DIR)
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Dependencies installed.")


@invoke.task
def build_frontend(c):
    """Build the Next.js frontend."""
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Building Next.js frontend...")
    run_cmd(c, "yarn build", cwd=GUI_DIR)
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Frontend build complete.")


@invoke.task
def build_tauri(c, release=False):
    """Build the Tauri backend."""
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Building Tauri backend...")
    run_cmd(
        c,
        f"cargo build {'--release' if release else ''} -j {get_cores()}",
        cwd=TAURI_DIR,
    )
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Tauri backend build complete.")


@invoke.task
def build_gui(c, release=False):
    """Build the GUI (Next.js + Tauri)."""
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Building GUI...")
    # Use Tauri's build command which properly links frontend and backend
    if release:
        run_cmd(c, "yarn tauri build", cwd=GUI_DIR)
    else:
        run_cmd(c, "yarn tauri build --debug", cwd=GUI_DIR)
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} GUI build complete.")


@invoke.task
def dev_gui(c):
    """Run GUI in development mode."""
    deps(c)
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Starting GUI in dev mode...")
    run_cmd(c, "yarn tauri dev", cwd=GUI_DIR)


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
    run_cmd(c, "cargo clean", cwd=TAURI_DIR)
    # Clean Next.js build artifacts
    nextjs_dirs = [".next", "out"]
    for dir_name in nextjs_dirs:
        dir_path = os.path.join(GUI_DIR, dir_name)
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
            print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Removed {dir_path}")
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Clean complete.")


@invoke.task
def test(c):
    """Run tests."""
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Running tests...")
    run_cmd(c, "cargo test")
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
def ci(c):
    """Run CI checks (lint, test, build)."""
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Running CI checks...")
    deps(c)
    lint(c)
    test(c)
    build(c)
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} CI checks complete.")


@invoke.task
def package(c):
    """Package the application."""
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Packaging application...")
    
    # Build CLI binaries first
    run_cmd(c, f"cargo build --release -j {CORES}")
    
    # Build and bundle GUI with Tauri's bundler
    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Building and bundling GUI...")
    result = run_cmd(c, "yarn tauri build", cwd=GUI_DIR, check=False)
    
    # Check if bundling failed (e.g., AppImage issues)
    if result.returncode != 0:
        print(
            f"{colorama.Fore.YELLOW}Warning: Some bundles may have failed to build. Continuing with available packages...{colorama.Style.RESET_ALL}"
        )
    
    # Create dist/
    dist_dir = "./dist"
    if not os.path.exists(dist_dir):
        os.makedirs(dist_dir)

    # Package CLI binaries
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

    # Package Tauri bundles (deb, AppImage, dmg, msi, etc.)
    # Only package actual distribution files, skip intermediate directories
    tauri_bundle_dir = os.path.join(TAURI_DIR, "target", "release", "bundle")
    if os.path.exists(tauri_bundle_dir):
        # Define valid distribution file extensions and directories to skip
        valid_extensions = {'.deb', '.rpm', '.AppImage', '.dmg', '.msi', '.exe', '.app'}
        skip_dirs = {'data', 'macos', 'appimage'}  # Intermediate build directories
        
        packaged_count = 0
        for bundle_type in os.listdir(tauri_bundle_dir):
            bundle_path = os.path.join(tauri_bundle_dir, bundle_type)
            if os.path.isdir(bundle_path):
                # For directories like 'deb', 'rpm', 'appimage', 'dmg', 'msi', 'nsis'
                for artifact in os.listdir(bundle_path):
                    # Skip intermediate directories (e.g., .AppDir, data/)
                    if artifact.lower() in skip_dirs or artifact.endswith('.AppDir'):
                        continue
                    
                    src = os.path.join(bundle_path, artifact)
                    
                    # Only copy files with valid extensions or .app bundles
                    if os.path.isfile(src):
                        _, ext = os.path.splitext(artifact)
                        if ext in valid_extensions:
                            dst = os.path.join(dist_dir, artifact)
                            shutil.copy2(src, dst)
                            print(
                                f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Packaged {artifact}"
                            )
                            packaged_count += 1
                    elif os.path.isdir(src) and artifact.endswith('.app'):
                        # macOS .app bundles are directories
                        dst = os.path.join(dist_dir, artifact)
                        if os.path.exists(dst):
                            shutil.rmtree(dst)
                        shutil.copytree(src, dst)
                        print(
                            f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Packaged {artifact}/"
                        )
                        packaged_count += 1
        
        if packaged_count == 0:
            print(
                f"{colorama.Fore.YELLOW}Warning: No Tauri bundles were packaged. Check build output for errors.{colorama.Style.RESET_ALL}"
            )
    else:
        print(
            f"{colorama.Fore.YELLOW}Warning: Tauri bundles not found at {tauri_bundle_dir}{colorama.Style.RESET_ALL}"
        )

    print(f"{colorama.Fore.GREEN}▸{colorama.Style.RESET_ALL} Packaging complete.")
