//! Bindings for the WASM plugin interface

use wasmtime::component::bindgen;

bindgen!({
    path: "wit",
    world: "plugin",
});
