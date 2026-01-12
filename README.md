# MonoDB

A modern, multi-paradigm database powered by Rust.

## Repo Info

**Canonical repository:** <https://git.konpeki.co.uk/jacob/prototype_3>

**GitHub mirror:** <https://github.com/Jacob-Walton/Mono-DB>

> [!NOTE]
> These repositories may not always be in sync. The canonical repo should always be considered the source of truth.

## Development Setup

To set this project up for development, you will first need to install some prerequisites:

### Install Rust and Cargo

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
rustup update
rustup component add rustfmt clippy
```

### Install Node.js and npm

You'll need Node.js version 22.0.0 or higher. Download and run the installer from
<https://nodejs.org/> or use a version manager like nvm.

### Install project dependencies

```bash
# Install yarn package manager over npm
npm install -g yarn

# Install node modules for the GUI
cd gui && yarn
cd ..

# Install Rust dependencies
cargo fetch
```

### (Optional) Install Python tools for task management

#### Install pipx for Python package management

```bash
python3 -m pip install --user pipx
python3 -m pipx ensurepath
source ~/.bashrc  # or source ~/.zshrc, depending on your shell
pipx upgrade pipx
```

#### Install invoke for task management

```bash
pipx install invoke
pipx inject invoke colorama
```

Or alternatively:

```bash
pip3 install invoke colorama
```

##### Test invoke

```bash
inv help
```

## Known Issues (Windows)

On Windows, building `aws-lc-sys` may required using the **GNU Nightly Toolchain**, selecting Ninja as the CMake generator,
and explicitly specifying the C and C++ compilers

Other CMake generators (such as `MinGW Makefiles`) *may* work if their respective toolchains are installed correctly, but
they are generally more fragile and may fail depending on the local environment. The default Visual Studio generator is
**not supported** due to issues with AWS-LC's build scripts.

```powershell
$env:CC="gcc"
$env:CXX="g++"
$env:CMAKE_GENERATOR="Ninja"
```

When using an IDE, ensure that environment variables are passed to the language server (for example, via `rust-analyzer.cargo.extraEnv`)
so that build scripts run correctly.
