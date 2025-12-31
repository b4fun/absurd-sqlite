# absurd-sqlite-extension

This folder contains the core SQLite extension logic for Absurd-SQLite, implemented in Rust.

## Development

This is a Rust based project. To build the extension, ensure you have Rust and Cargo installed, then run:

```bash
$ cargo build
```

### Code Linting and Formatting

Please make sure to lint / format code. Use the following commands to validate:

```
$ cargo fmt -p absurd-sqlite-extension -- --check
$ cargo clippy -p absurd-sqlite-extension -- -D warnings
```

### Code Testing

To run the tests for the SQLite extension, use:

```bash
$ cargo test -p absurd-sqlite-extension
```

Please make sure all tests pass after making changes.
