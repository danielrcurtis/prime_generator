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
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::convert::From;
use serde::{Serialize, Deserialize};
use csv::Writer;
extern crate clap;
use clap::{App, Arg};
extern crate csv;
use std::time::Instant;
use reqwest;
use tokio::runtime::Runtime;
use num_traits::ToPrimitive;
#[derive(Serialize, Deserialize)]
struct PrimeRecord {
    prime: u128,
    squared: String,
    cubed: String,
    to_fourth_power: String,
}

#[derive(Deserialize)]
struct Range {
    start: u128,
    end: u128,
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
    // Create a new Tokio runtime
    let rt = Runtime::new().unwrap();
    // Use the runtime to block on the asynchronous function
    let (default_start, default_end) = rt.block_on(fetch_default_range()).expect("Failed to fetch default range");

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
        // Define `cpus` argument.
        .arg(
            Arg::with_name("cpus")
                .short('c')
                .long("cpus")
                .takes_value(true)
                .help("Number of CPUs to use"),
        )
        .get_matches();

    // Retrieve the number of CPUs from arguments, or use default
    let num_cpus = matches.value_of("cpus")
        .map(|c| c.parse::<usize>().expect("Invalid number of CPUs"))
        .unwrap_or_else(|| num_cpus::get() - 1);

    // Ensure at least 1 CPU is used
    let thread_count = if num_cpus > 1 { num_cpus - 1 } else { 1 };

    // Build a new thread pool with the specified number of threads
    ThreadPoolBuilder::new().num_threads(thread_count).build_global().unwrap();

    let start = matches
    .value_of("start")
    .map(|s| s.parse::<u128>().expect("Invalid start value"))
    .unwrap_or(default_start);

    let end = matches
        .value_of("end")
        .map(|e| e.parse::<u128>().expect("Invalid end value"))
        .unwrap_or(default_end);

    let primes_and_powers = Arc::new(Mutex::new(HashMap::new()));

    // Clone `primes_and_powers` before moving it into the closure
    let primes_and_powers_clone = primes_and_powers.clone();
    let temp_storage: Arc<Mutex<Vec<(u128, Vec<BigInt>)>>> = Arc::new(Mutex::new(Vec::new()));

    let start_time = Instant::now();

    // Parallel iteration
    let temp_storage_clone = temp_storage.clone();
    (start..=end)
        .into_par_iter()
        .filter_map(|n| {
            let big_n = BigInt::from(n);
            if big_n.clone() % 2.to_bigint().unwrap() == 1.to_bigint().unwrap() || big_n == 2.to_bigint().unwrap() {
                Some(big_n)
            } else {
                None
            }
        })
    .for_each(move |big_n| {
            if is_prime(big_n.clone()) {
                let n = big_n.to_u128().expect("Number should fit in u128");
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

    // Post results to API
    rt.block_on(post_results("primes_and_powers.csv"))
        .expect("Failed to post results");
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
fn is_prime(big_n: BigInt) -> bool {
    if let Some(n) = big_n.to_u128() {
        // Handle numbers that fit into u128
        match n {
            0 | 1 => false,
            2 | 3 => true,
            _ if n % 2 == 0 || n % 3 == 0 => false,
            _ => {
                let limit = (n as f64).sqrt() as u128 + 1;
                (5..=limit).step_by(6).all(|i| n % i != 0 && n % (i + 2) != 0)
            }
        }
    } else {
        // Use BigInt for very large numbers
        if big_n <= 1.to_bigint().unwrap() || big_n == 2.to_bigint().unwrap() || big_n == 3.to_bigint().unwrap() {
            return big_n > 1.to_bigint().unwrap();
        }
        if &big_n % 2.to_bigint().unwrap() == Zero::zero() || &big_n % 3.to_bigint().unwrap() == Zero::zero() {
            return false;
        }

        let mut i = BigInt::from(5);
        while &i * &i <= big_n {
            if &big_n % &i == Zero::zero() || &big_n % (&i + 2) == Zero::zero() {
                return false;
            }
            i = i + 6;
        }
        true
    }
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

async fn fetch_default_range() -> (u128, u128) {
    let api_url = "http://primegen.io/api/default_range";
    let client = reqwest::Client::new();

    let response = client.get(api_url)
        .send()
        .await
        .expect("Failed to fetch the range");

    let Range { start, end } = response.json::<Range>()
        .await
        .expect("Failed to parse the range");

    (start, end)
}

// Function to read data from CSV file
fn read_csv_data<P: AsRef<Path>>(path: P) -> Result<Vec<PrimeRecord>> {
    let file = OpenOptions::new().read(true).open(path)?;
    let mut rdr = csv::Reader::from_reader(file);
    let mut records = Vec::new();

    for result in rdr.deserialize() {
        let record: PrimeRecord = result?;
        records.push(record);
    }

    Ok(records)
}

// Function to post results to an API
async fn post_results(file_path: &str) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let records = read_csv_data(file_path)?;
    let client = reqwest::Client::new();
    let api_url = "http://primegen.io/api/post_results"; // Replace with your actual POST API URL

    client.post(api_url)
        .json(&records)
        .send()
        .await?
        .error_for_status()?;

    Ok(())
}