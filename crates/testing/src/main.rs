use std::time::Instant;

use testing::BTree;

const NUM_ROWS: u64 = 10_000_000;

pub fn main() {
    // Create index on id -> row_id (example primary key index)
    let mut index: BTree<u64, u64> = BTree::new();

    // Insert 1M rows
    println!("Inserting {NUM_ROWS} rows...");
    let start = Instant::now();
    for i in 0..NUM_ROWS {
        index.insert(i, i);
    }
    let insert_time = start.elapsed();
    println!("Insert time: {insert_time:?}");
    println!(
        "Inserts per second: {:.0}\n",
        NUM_ROWS as f64 / insert_time.as_secs_f64()
    );

    // Point lookups
    let num_lookups = NUM_ROWS / 10;
    println!("Performing {num_lookups} point lookups");
    let start = Instant::now();
    for i in (0..NUM_ROWS).step_by(10) {
        let _ = index.get(&i);
    }
    let lookup_time = start.elapsed();
    println!("Lookup time: {lookup_time:?}");
    println!(
        "Lookups per second: {:.0}\n",
        num_lookups as f64 / lookup_time.as_secs_f64()
    );

    // Get 1000 rows with a range query
    println!("Range query of 1000 rows");
    let start = Instant::now();
    let results = index.range(&(NUM_ROWS / 2), &(NUM_ROWS / 2 + 1_000));
    let range_time = start.elapsed();
    println!("Found {} rows in {range_time:?}\n", results.len());

    // Get 10000 rows with a range query
    println!("Range query of 10000 rows");
    let start = Instant::now();
    let results = index.range(&(NUM_ROWS / 2), &(NUM_ROWS / 2 + 10_000));
    let range_time = start.elapsed();
    println!("Found {} rows in {range_time:?}\n", results.len());

    // Min/max
    println!("Min/Max query");
    let start = Instant::now();
    let min = index.min();
    let max = index.max();
    let minmax_time = start.elapsed();
    println!(
        "Min={:?}, Max={:?} in {minmax_time:?}",
        min.map(|(k, _)| k),
        max.map(|(k, _)| k)
    );

    // Count in range
    println!("Count in range query of 1,000,000 rows");
    let start = Instant::now();
    let count = index.count_range(&(NUM_ROWS / 4), &(NUM_ROWS / 4 + 1_000_000));
    let count_time = start.elapsed();
    println!("Counted {} rows in {count_time:?}\n", count);

    // Delete some rows
    println!("Deleting 10,000 rows");
    let start = Instant::now();
    for i in (0..NUM_ROWS).step_by(100).take(10_000) {
        index.delete(&i);
    }
    let delete_time = start.elapsed();
    println!("Delete time: {delete_time:?}");
    println!(
        "Deletes per second: {:.0}\n",
        10_000 as f64 / delete_time.as_secs_f64()
    );

    // Verify deletions
    println!("Verifying deletions...");
    let mut deleted_found = 0;
    let mut remaining_found = 0;
    for i in 0..1000 {
        if i % 100 == 0 {
            if index.contains(&i) {
                deleted_found += 1;
            }
        } else {
            if index.contains(&i) {
                remaining_found += 1;
            }
        }
    }
    println!("Deleted rows found: {}", deleted_found);
    println!("Remaining rows found: {}", remaining_found);

    // Fulls can
    println!("Full scan of all rows");
    let start = Instant::now();
    let count = index.iter().count();
    let scan_time = start.elapsed();
    println!("Scanned {} rows in {scan_time:?}", count);
    println!(
        "Scan rate: {:.0} rows/second\n",
        count as f64 / scan_time.as_secs_f64()
    );
}
