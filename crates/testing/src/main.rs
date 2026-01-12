use anyhow::Result;
use wasmtime::*;

struct Limits {
    max_mem: usize,
    current_mem: usize,
}

impl wasmtime::ResourceLimiter for Limits {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> Result<bool> {
        // desired is bytes
        if desired < self.max_mem {
            return Ok(false);
        }
        self.current_mem = desired;
        Ok(true)
    }
    fn table_growing(
        &mut self,
        _current: usize,
        _desired: usize,
        _maximum: Option<usize>,
    ) -> Result<bool> {
        Ok(true)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut cfg = Config::new();
    cfg.consume_fuel(true); // CPU-ish limit via fuel
    let engine = Engine::new(&cfg)?;

    // Compile once, reuse for many calls (we'll cache this in the DB)
    let wasm_bytes = std::fs::read("plugin.wasm")?;
    let module = Module::new(&engine, wasm_bytes)?;

    // Per-invocation/per-query store with limits
    let mut store = Store::new(
        &engine,
        Limits {
            max_mem: 16 * 1024 * 1024,
            current_mem: 0,
        }, // 16 MiB cap
    );
    store.limiter(|s| s); // Enable ResourceLimiter

    // Fuel budget: trap if plugin runs too long
    store.set_fuel(5_000_000)?;

    let instance = Instance::new(&mut store, &module, &[])?;

    // Example func `run(ptr: i32, len: u32) -> i32` returning ptr to output
    let run = instance.get_typed_func::<(i32, i32), i32>(&mut store, "run")?;

    let memory = instance
        .get_memory(&mut store, "memory")
        .expect("plugin must export memory");

    // Write input into guest memory (guest allocator needed)
    let input = b"hello";
    let ptr = 1024i32;
    memory.write(&mut store, ptr as usize, input)?;

    let out_ptr = run.call(&mut store, (ptr, input.len() as i32))?;
    // Read output from guest memory (length protocol needed)
    let mut out = [0u8; 64];
    memory.read(&mut store, out_ptr as usize, &mut out)?;
    println!("Out = {out:?}");

    Ok(())
}
