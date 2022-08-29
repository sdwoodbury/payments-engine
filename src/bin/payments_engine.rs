use csv::{self, ReaderBuilder};
use error_stack::{IntoReport, Result, ResultExt};
use payments_engine::{db::TxnDb, errors::print_report, errors::*, fmt_error, model::*};
use random_string::generate;
use std::{collections::HashMap, fs, io::BufReader, path::Path, process::ExitCode};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("error: no input file specified");
        return ExitCode::FAILURE;
    }

    let input_file = &args[1];

    // ensure the item exists
    let path = Path::new(input_file);
    if !path.exists() {
        eprintln!("error: \"{}\" does not exist", input_file);
        return ExitCode::FAILURE;
    }

    // ensure the item is a file
    if !path.is_file() {
        eprintln!("error: {} is not a file", input_file);
        return ExitCode::FAILURE;
    }

    // attempt to open the file
    let open_res = fs::OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(input_file);

    if open_res.is_err() {
        eprintln!("failed to open file: {}", open_res.unwrap_err());
        return ExitCode::FAILURE;
    }

    if let Err(e) = process(open_res.unwrap()) {
        print_report(e);
    }

    ExitCode::SUCCESS
}

fn process(input_file: fs::File) -> Result<(), MyError> {
    let mut processor = PaymentProcessor::init()?;

    // process the input file
    let reader = BufReader::new(input_file);
    let mut csv_reader = ReaderBuilder::new().from_reader(reader);
    for string_record in csv_reader.records() {
        // get a line from the CSV
        let mut record = string_record
            .report()
            .attach_printable_lazy(|| fmt_error!(""))
            .change_context(MyError::FileReader)?;
        record.trim();

        // deserialize it
        let txn: Txn = record
            .deserialize(None)
            .report()
            .attach_printable_lazy(|| fmt_error!(""))
            .change_context(MyError::FileReader)?;

        processor.process(txn)?;
    }

    processor.display();

    Ok(())
}

pub struct PaymentProcessor {
    db: TxnDb,
    clients: HashMap<ClientId, ClientState>,
}

impl PaymentProcessor {
    pub fn init() -> Result<Self, MyError> {
        // having the same for the db name every time messes up the unit tests.
        let charset = "abcdefghijklmnopqrstuvwxyz";
        Ok(PaymentProcessor {
            db: TxnDb::new(&format!("{}.db", generate(6, charset)))
                .attach_printable_lazy(|| fmt_error!("database failure"))?,
            clients: HashMap::new(),
        })
    }

    #[cfg(test)]
    pub fn get_clients(&self) -> HashMap<ClientId, ClientState> {
        return self.clients.clone();
    }

    pub fn display(&self) {
        // display the result
        println!("client,available,held,total,locked");
        for (k, v) in &self.clients {
            println!("{},{}", k, v);
        }
    }

    pub fn process(&mut self, txn: Txn) -> Result<(), MyError> {
        // store it in the DB. disputes, resolutions, and chargebacks will be ignored by the DB
        self.db
            .insert_txn(&txn)
            .attach_printable_lazy(|| fmt_error!(""))?;

        // obtain the customer state - create new if needed
        let mut state = match self.clients.get_key_value(&txn.client_id) {
            Some((_, state)) => state.clone(),
            None => ClientState::init(),
        };

        // ignore transactions once the account is locked/frozen
        if state.locked {
            return Ok(());
        }

        // update the customer state
        match txn.txn_type {
            TxnType::Invalid => panic!("should never happen"),
            TxnType::Chargeback => match self.db.get_txn(txn.txn_id)? {
                None => {} // txn was invalid and was ignored
                Some(disputed) => {
                    match disputed.txn_type {
                        TxnType::Withdrawal => {
                            // should not have withdrawn. reverse the transaction
                            state.held -= disputed.amount.unwrap();
                            state.available += disputed.amount.unwrap()
                        }
                        TxnType::Deposit => {
                            // should not have deposited. reverse the transaction
                            state.held -= disputed.amount.unwrap();
                            // state.available was already deducted. don't need to deduct it here.
                        }
                        _ => panic!("should never happen"),
                    }
                    state.locked = true;
                }
            },
            TxnType::Deposit => {
                state.available += txn.amount.unwrap();
            }
            TxnType::Withdrawal => {
                if txn.amount.unwrap() > state.available {
                    // withdrawal cannot exceed balance
                } else {
                    state.available -= txn.amount.unwrap();
                }
            }
            TxnType::Dispute => match self.db.get_txn(txn.txn_id)? {
                None => {} // txn was invalid and was ignored
                Some(disputed) => match disputed.txn_type {
                    TxnType::Withdrawal => {
                        // consider undoing the withdrawal
                        state.held += disputed.amount.unwrap();
                    }
                    TxnType::Deposit => {
                        // consider undoing the deposit - hold the deposited funds
                        state.held += disputed.amount.unwrap();
                        state.available -= disputed.amount.unwrap();
                    }
                    _ => panic!("should never happen"),
                },
            },
            TxnType::Resolve => match self.db.get_txn(txn.txn_id)? {
                None => {} // txn was invalid and was ignored
                Some(disputed) => match disputed.txn_type {
                    TxnType::Withdrawal => {
                        // no funds held in this case
                        state.held -= disputed.amount.unwrap();
                    }
                    TxnType::Deposit => {
                        // release the held funds
                        state.held -= disputed.amount.unwrap();
                        state.available += disputed.amount.unwrap();
                    }
                    _ => panic!("should never happen"),
                },
            },
        }

        // update the state
        state.total = state.available + state.held;
        self.clients.insert(txn.client_id, state);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const TXN1: Txn = Txn {
        txn_type: TxnType::Deposit,
        txn_id: 1,
        client_id: 1,
        amount: Some(10.0),
    };

    const TXN2: Txn = Txn {
        txn_type: TxnType::Withdrawal,
        txn_id: 2,
        client_id: 1,
        amount: Some(5.0),
    };

    const TXN3: Txn = Txn {
        txn_type: TxnType::Dispute,
        txn_id: 1,
        client_id: 1,
        amount: None,
    };

    const TXN4: Txn = Txn {
        txn_type: TxnType::Dispute,
        txn_id: 2,
        client_id: 1,
        amount: None,
    };

    const TXN5: Txn = Txn {
        txn_type: TxnType::Chargeback,
        txn_id: 1,
        client_id: 1,
        amount: None,
    };

    const TXN6: Txn = Txn {
        txn_type: TxnType::Chargeback,
        txn_id: 2,
        client_id: 1,
        amount: None,
    };

    const TXN7: Txn = Txn {
        txn_type: TxnType::Resolve,
        txn_id: 1,
        client_id: 1,
        amount: None,
    };

    const TXN8: Txn = Txn {
        txn_type: TxnType::Resolve,
        txn_id: 2,
        client_id: 1,
        amount: None,
    };

    const TXN9: Txn = Txn {
        txn_type: TxnType::Deposit,
        txn_id: 5,
        client_id: 1,
        amount: Some(10.0),
    };

    #[test]
    fn test_deposit() {
        let mut processor = PaymentProcessor::init().unwrap();
        processor.process(TXN1).unwrap();
        assert_eq!(processor.get_clients().get(&1).unwrap().available, 10.0);
    }

    #[test]
    fn test_withdrawal() {
        let mut processor = PaymentProcessor::init().unwrap();
        processor.process(TXN1).unwrap();
        processor.process(TXN2).unwrap();
        assert_eq!(processor.get_clients().get(&1).unwrap().available, 5.0);
    }

    #[test]
    fn test_dispute_deposit() {
        let mut processor = PaymentProcessor::init().unwrap();
        // deposit 10 then dispute it
        processor.process(TXN1).unwrap();
        processor.process(TXN3).unwrap();
        assert_eq!(processor.get_clients().get(&1).unwrap().available, 0.0);
        assert_eq!(processor.get_clients().get(&1).unwrap().held, 10.0);
    }

    #[test]
    fn test_dispute_deposit2() {
        let mut processor = PaymentProcessor::init().unwrap();
        // deposit 10, withdraw 5, dispute the deposit
        processor.process(TXN1).unwrap();
        processor.process(TXN2).unwrap();
        processor.process(TXN3).unwrap();
        assert_eq!(processor.get_clients().get(&1).unwrap().available, -5.0);
        assert_eq!(processor.get_clients().get(&1).unwrap().held, 10.0);
    }

    #[test]
    fn test_dispute_withdrawal() {
        let mut processor = PaymentProcessor::init().unwrap();
        // deposit 10, withdraw 5, dispute the withdrawal
        processor.process(TXN1).unwrap();
        processor.process(TXN2).unwrap();
        processor.process(TXN4).unwrap();
        assert_eq!(processor.get_clients().get(&1).unwrap().available, 5.0);
        assert_eq!(processor.get_clients().get(&1).unwrap().held, 5.0);
    }

    #[test]
    fn test_dispute_deposit_chargeback() {
        let mut processor = PaymentProcessor::init().unwrap();
        // deposit 10, withdraw 5, dispute the deposit, then chargeback
        processor.process(TXN1).unwrap();
        processor.process(TXN2).unwrap();
        processor.process(TXN3).unwrap();
        processor.process(TXN5).unwrap();
        assert_eq!(processor.get_clients().get(&1).unwrap().available, -5.0);
        assert_eq!(processor.get_clients().get(&1).unwrap().held, 0.0);
        assert_eq!(processor.get_clients().get(&1).unwrap().locked, true);
    }

    #[test]
    fn test_dispute_deposit_chargeback2() {
        let mut processor = PaymentProcessor::init().unwrap();
        // deposit 10, withdraw 5, dispute the deposit, then chargeback, then try to deposit
        processor.process(TXN1).unwrap();
        processor.process(TXN2).unwrap();
        processor.process(TXN3).unwrap();
        processor.process(TXN5).unwrap();

        // verify this has no effect
        processor.process(TXN9).unwrap();

        assert_eq!(processor.get_clients().get(&1).unwrap().available, -5.0);
        assert_eq!(processor.get_clients().get(&1).unwrap().held, 0.0);
        assert_eq!(processor.get_clients().get(&1).unwrap().locked, true);
    }

    #[test]
    fn test_dispute_deposit_resolve() {
        let mut processor = PaymentProcessor::init().unwrap();
        // deposit 10, withdraw 5, dispute the deposit, then resolve
        processor.process(TXN1).unwrap();
        processor.process(TXN2).unwrap();
        processor.process(TXN3).unwrap();
        processor.process(TXN7).unwrap();
        assert_eq!(processor.get_clients().get(&1).unwrap().available, 5.0);
        assert_eq!(processor.get_clients().get(&1).unwrap().held, 0.0);
    }

    #[test]
    fn test_dispute_withdrawal_chargeback() {
        let mut processor = PaymentProcessor::init().unwrap();
        // deposit 10, withdraw 5, dispute the withdrawal, then chargeback
        processor.process(TXN1).unwrap();
        processor.process(TXN2).unwrap();
        processor.process(TXN4).unwrap();
        processor.process(TXN6).unwrap();
        assert_eq!(processor.get_clients().get(&1).unwrap().available, 10.0);
        assert_eq!(processor.get_clients().get(&1).unwrap().held, 0.0);
        assert_eq!(processor.get_clients().get(&1).unwrap().locked, true);
    }

    #[test]
    fn test_dispute_withdrawal_resolve() {
        let mut processor = PaymentProcessor::init().unwrap();
        // deposit 10, withdraw 5, dispute the withdrawal, then resolve
        processor.process(TXN1).unwrap();
        processor.process(TXN2).unwrap();
        processor.process(TXN4).unwrap();
        processor.process(TXN8).unwrap();
        assert_eq!(processor.get_clients().get(&1).unwrap().available, 5.0);
        assert_eq!(processor.get_clients().get(&1).unwrap().held, 0.0);
        assert_eq!(processor.get_clients().get(&1).unwrap().locked, false);
    }
}
