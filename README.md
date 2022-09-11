# payments-engine

## usage
- `cargo run -- test_files/f1.csv > output.csv`
- `payments_engine <input file> > output.csv`
- to view errors, prepend `RUST_LOG=error` to the program. ex: `RUST_LOG=error payments_engine <input file> > output.csv`

## directory
```
├── bin
│   └── payments_engine.rs      <-- the executable.
├── db.rs                       <-- sql database
├── errors.rs                   <-- error reporting utilities
├── lib.rs                      <-- allows for integration testing, if desired
├── model.rs                    <-- contains structs for the database and customer account representation
└── transaction_processor.rs    <-- validates and processes transactions
```

# assumptions
- once an account is locked, subsequent transactions are rejected
- invalid inputs are ignored 
- deposits and withdrawals must specify a positive amount
- a dispute involves the entire amount of the deposit or withdrawal
- a deposit or withdrawal may only be disputed once
- if the program experiences an error, `log::error` will be used to output the error. assuming it is OK to not show the errors unless explicitly requested, via environment variables. 

# assumptions regarding disputes, withdrawals, and chargebacks 
- if a withdrawal is disputed, total funds will increase and the withdrawn amount will be held. the available funds will remain unchanged
- if a disputed withdrawal is charged back, the account is credited the withdrawn amount (available increases)
- if a deposit is charged back, total funds will remain unchanged, the available amount will decrease by the disputed amount (could become negative if the dispute occurs after the deposited funds are withdrawn), and the held amount increases
- if a disputed deposit is charged back, the account is debited the deposited amount 

# data integrity constraints 
- a SQLite database is used to store the various types of transactions. The relational model enforces certain constraints. 
- deposits and withdrawals are considered "balance transfers". Balance transfers are stored in their own table. The primary key consists of a customer id and transaction id
    + transaction ids have a UNIQUE constraint
    + customer id is a foreign key in a "Customers" table --> the customer id is also required to be unique
- disputes go a "Disputes" table. 
    +  primary key and foreign key of (customer_id, transaction_id), referencing the BalanceTransfers table, ensures a balance transfer may only be disputed once and that ony existing balance_transfers may be disputed
- "resolve" and "chargeback" go in a "Resolutions" table. 
    +  primary key and foreign key of (customer_id, transaction_id), referencing the Disputes table, ensures a dispute may only be resolved once and that a resolution may only be applied to an existing dispute
- the client account information (the state) is stored in a "Customers" table. the `transaction_processor` will obtain the state for a client, insert the balance_transfer, dispute, or resolution, update the state, and save it. if desired, rusqlite allows for transactions; these are not currently used. 
