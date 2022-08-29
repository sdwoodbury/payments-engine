# payments-engine

## usage
- `cargo run -- test_files/f1.csv > output.csv`
- `payments_engine <input file> > output.csv`

## directory
```
├── bin
│   └── payments_engine.rs <-- the executable. contains unit tests
├── db.rs <-- sql database used for disputes
├── errors.rs<-- error reporting utilities
├── lib.rs <-- allows for integration testing, if desired
└── model.rs <-- contains structs for the database and customer account representation
```

# assumptions
- all transactions submitted are valid
    + transaction ids are globally unique
    + customer ids are globally unique
    + transactions for a customer aren't submitted out of order
    + transaction amounts are non negative
    + disputes refer to transactions with a matching customer ID, where that transaction is either a deposit or a withdrawal
    + resolves and chargebacks always refer to a valid dispute, and there is at most one of these per dispute
- once an account is locked, subsequent transactions are rejected
- disputes should hold funds when a deposit is disputed (as those funds are accessible) but not when a withdrawal is disputed
- if a disputed withdrawal is charged back, the account is credited the withdrawn amount (available increases)
- if a disputed deposit is charged back, the account is debited the deposited amount 
