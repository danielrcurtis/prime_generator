extern crate rayon;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;
extern crate num_bigint as bigint;
extern crate num_traits;
use bigint::{BigInt, ToBigInt};
use num_traits::Zero;
use std::fs::OpenOptions;
use std::io::Result;
use std::sync::{Arc, Mutex};
use serde::Serialize;
use csv::Writer;
extern crate clap;
use clap::{App, Arg};
extern crate csv;
use std::time::Instant;

#[derive(Serialize)]
struct PrimeRecord {
    prime: u128,
    squared: String,
    cubed: String,
    to_fourth_power: String,
}

// Define a threshold for record count or memory usage
const FLUSH_THRESHOLD: usize = 10000;

/// The entry point for the Prime Factorization program.
///
/// This function sets up a command-line interface (CLI) for the program,
/// processes user input to determine the range of numbers to analyze for primality,
/// performs the prime factorization within the given range, and then writes the results
/// to a CSV file.
///
/// # Arguments
///
/// * `start` - A CLI argument that specifies the start of the range for prime factorization.
///             It is provided by the user with the `-s` or `--start` flag.
///
/// * `end` - A CLI argument that specifies the end of the range for prime factorization.
///           It is provided by the user with the `-e` or `--end` flag.
///
/// # Panics
///
/// * The function will panic if the `start` or `end` values are not provided in the expected
///   format (unsigned 64-bit integers).
/// * It will also panic if the `write_to_csv` function fails to write the data to a CSV file.
///
/// # Examples
///
/// ```sh
/// prime_generator -s 2 -e 1000000
/// ```
///
/// This will generate prime numbers and their factors between 2 and 1,000,000.
fn main() {
    // Get the number of CPUs and subtract 1
    let num_cpus = num_cpus::get();
    let thread_count = if num_cpus > 1 { num_cpus - 2 } else { 1 };

    // Build a new thread pool with the specified number of threads
    ThreadPoolBuilder::new().num_threads(thread_count).build_global().unwrap();

    // Setup CLI using `clap` crate.
    let matches = App::new("Prime Factorization")
        // Specifies the version, author, and about text for the help output.
        .version("1.0")
        .author("Daniel R Curtis")
        .about("Generates prime numbers and their factors within a given range")
        // Define `start` argument.
        .arg(
            Arg::with_name("start")
                .short('s')
                .long("start")
                .takes_value(true)
                .help("Start of the range"),
        )
        // Define `end` argument.
        .arg(
            Arg::with_name("end")
                .short('e')
                .long("end")
                .takes_value(true)
                .help("End of the range"),
        )
        // Parse arguments from the command line.
        .get_matches();

    let start = matches
    .value_of("start")
    .unwrap_or("2")
    .parse::<u128>()
    .expect("Invalid start value");

    let end = matches
        .value_of("end")
        .unwrap_or("1000000")
        .parse::<u128>()
        .expect("Invalid end value");

    let primes_and_powers = Arc::new(Mutex::new(HashMap::new()));

    // Clone `primes_and_powers` before moving it into the closure
    let primes_and_powers_clone = primes_and_powers.clone();
    let temp_storage: Arc<Mutex<Vec<(u128, Vec<BigInt>)>>> = Arc::new(Mutex::new(Vec::new()));

    let start_time = Instant::now();

    // Parallel iteration
    let temp_storage_clone = temp_storage.clone();
    (start..=end)
        .into_par_iter()
        .filter(|&n| n % 2 == 1 || n == 2)
        .for_each(move |n| {
            if is_prime(n) {
                if let Some((squared, cubed, to_fourth_power)) = calculate_powers(n) {
                    let mut storage = temp_storage_clone.lock().unwrap();
                    storage.push((n, vec![squared, cubed, to_fourth_power]));

                    // Check if it's time to flush
                    if storage.len() >= FLUSH_THRESHOLD {
                        flush_to_csv(&mut *storage).expect("Failed to flush to CSV");
                    }
                } else {
                    println!("Overflow error for {}", n);
                }
            }
        });
    
    // Flush any remaining data
    {
        let mut storage = temp_storage.lock().unwrap();
        if !storage.is_empty() {
            flush_to_csv(&mut *storage).expect("Failed to flush to CSV");
        }
    }

    let elapsed_duration = start_time.elapsed();
    println!("Time taken: {:?}", elapsed_duration);
    
    // Write final data to CSV
    let data = primes_and_powers_clone.lock().unwrap();
    write_to_csv(&*data).expect("Failed to write to CSV");
}

// Function to calculate the powers of a number
fn calculate_powers(n: u128) -> Option<(BigInt, BigInt, BigInt)> {
    let big_n = n.to_bigint()?;
    let squared = &big_n * &big_n;
    let cubed = &squared * &big_n;
    let to_fourth_power = &squared * &squared;
    Some((squared, cubed, to_fourth_power))
}

// Function to check if a number is prime
fn is_prime(n: u128) -> bool {
    if n <= 1 {
        return false;
    }
    let big_n = n.to_bigint().unwrap();
    let two = 2.to_bigint().unwrap();
    let mut i = two.clone();

    while &i * &i <= big_n {
        if &big_n % &i == Zero::zero() {
            return false;
        }
        i = i + BigInt::from(1);
    }
    true
}

// Function to flush data to CSV and clear the temporary storage
fn flush_to_csv(temp_storage: &mut Vec<(u128, Vec<BigInt>)>) -> Result<()> {
    let mut wtr = Writer::from_writer(OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open("primes_and_powers.csv")?);

    for (prime, powers) in temp_storage.iter() {
        let record = PrimeRecord {
            prime: *prime,
            squared: powers[0].to_str_radix(10),
            cubed: powers[1].to_str_radix(10),
            to_fourth_power: powers[2].to_str_radix(10),
        };
        wtr.serialize(record)?;
    }

    wtr.flush()?;
    temp_storage.clear(); // Clear the temporary storage after flushing
    Ok(())
}

fn write_to_csv(data: &HashMap<u128, Vec<BigInt>>) -> Result<()> {
    let path = "primes_and_powers.csv";
    let file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(path)?;

    let mut wtr = Writer::from_writer(file);

    for (prime, powers) in data {
        let record = PrimeRecord {
            prime: *prime,
            squared: powers[0].to_str_radix(10),
            cubed: powers[1].to_str_radix(10),
            to_fourth_power: powers[2].to_str_radix(10),
        };
        wtr.serialize(record)?;
    }

    wtr.flush()?;
    Ok(())
}