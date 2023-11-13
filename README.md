The purpose of this program is to create a database of prime numbers, and verified non-prime numbers.
This is being used to create a dataset of primes and excluded primes so that we can train machine learning
models to predict potential primes.

Build the application from source:

cargo update

cargo build --release

You can search a specific space by running prime_generator.exe -s start_number -e end_number -c num_cpus

Example:

prime_generator.exe -s 3000000000 -e 3100000000 -c 6