# Database Cloner

## Build

```bash
cargo build --release
```

## Usage

```bash
cp env.example .env

./target/release/db-cloner
```

⚠️Warning: If the table in the target database already exists, it will be deleted.
