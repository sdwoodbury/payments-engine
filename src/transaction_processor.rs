use crate::{db::TxnDb, errors::*, fmt_error, model::*};
use error_stack::{bail, Result, ResultExt};
use random_string::generate;

pub struct TransactionProcessor {
    db: TxnDb,
}

impl TransactionProcessor {
    pub fn new() -> Result<Self, MyError> {
        // having the same for the db name every time messes up the unit tests.
        let charset = "abcdefghijklmnopqrstuvwxyz";
        Ok(TransactionProcessor {
            db: TxnDb::new(&format!("{}.db", generate(6, charset)))
                .attach_printable_lazy(|| fmt_error!("database failure"))?,
        })
    }

    pub fn display(&self) -> Result<(), MyError> {
        // display the result
        println!("client,available,held,total,locked");
        self.db
            .process_all_clients(|client| println!("{}", client))?;

        Ok(())
    }

    pub fn process(&mut self, raw_input: RawTxnInput) -> Result<(), MyError> {
        // ignore invalid transactions
        let txn = match self.validate_raw_input(&raw_input) {
            Some(r) => r,
            None => return Ok(()),
        };

        // obtain the customer state - create new if needed
        let mut state = match self.db.get_client_state(raw_input.client_id)? {
            Some(s) => s,
            None => self.db.create_client_state(raw_input.client_id)?,
        };

        // ignore transactions once the account is locked/frozen
        if state.is_locked() {
            return Ok(());
        }

        match txn {
            Txn::BalanceTransfer(transfer) => {
                // ignore withdrawals that exceed account balance
                // in the event of a dispute, available funds may be negative. allow deposits in this case.
                if transfer.amount < 0.0 && state.available + transfer.amount < 0.0 {
                    return Ok(());
                }

                // verify transaction_id is unique
                if self.db.try_insert_balance_transfer(transfer)? {
                    // update client state
                    state.available += transfer.amount;
                }
            }
            Txn::Dispute { client_id, txn_id } => {
                // validate txn_id and client_id using the database relations
                if self.db.try_insert_dispute(client_id, txn_id)? {
                    let balance_transfer = self
                        .db
                        .get_balance_transfer(client_id, txn_id)
                        .attach_printable_lazy(|| fmt_error!("process dispute failed"))?;
                    if balance_transfer.is_none() {
                        bail!(MyError::GenericFmt(fmt_error!(
                            "inserted dispute but get_balance_transfer returned None"
                        )));
                    }
                    let balance_transfer = balance_transfer.unwrap();

                    // if it was a withdrawal, increase held by the amount but to not increase available funds
                    if balance_transfer.amount < 0.0 {
                        // because here balance_transfer is negative, this operation increases state.held
                        state.held -= balance_transfer.amount;
                    } else {
                        // if it was a deposit, hold the funds and don't let them be spent -> decrease available funds
                        state.held += balance_transfer.amount;
                        state.available -= balance_transfer.amount;
                    }
                }
            }
            Txn::Resolve { client_id, txn_id } => {
                // validate txn_id and client_id using the database relations
                if self.db.try_resolve_dispute(client_id, txn_id)? {
                    let balance_transfer = self
                        .db
                        .get_balance_transfer(client_id, txn_id)
                        .attach_printable_lazy(|| fmt_error!("resolved dispute failed"))?;
                    if balance_transfer.is_none() {
                        bail!(MyError::GenericFmt(fmt_error!(
                            "resolved dispute but get_balance_transfer returned None"
                        )));
                    }
                    let balance_transfer = balance_transfer.unwrap();

                    // the withdrawal was cleared
                    if balance_transfer.amount < 0.0 {
                        // because here balance_transfer is negative, this operation decreases state.held
                        state.held += balance_transfer.amount;
                    } else {
                        // the deposit was cleared
                        state.held -= balance_transfer.amount;
                        state.available += balance_transfer.amount;
                    }
                }
            }
            Txn::Chargeback { client_id, txn_id } => {
                // validate txn_id and client_id using the database relations
                if self.db.try_chargeback_dispute(client_id, txn_id)? {
                    let balance_transfer = self
                        .db
                        .get_balance_transfer(client_id, txn_id)
                        .attach_printable_lazy(|| fmt_error!("charged back dispute failed"))?;
                    if balance_transfer.is_none() {
                        bail!(MyError::GenericFmt(fmt_error!(
                            "charged back dispute but get_balance_transfer returned None"
                        )));
                    }
                    let balance_transfer = balance_transfer.unwrap();

                    // the withdrawal was charged back. decrease state.held and increase state.available
                    if balance_transfer.amount < 0.0 {
                        // because here balance_transfer is negative, this operation decreases state.held
                        state.held += balance_transfer.amount;
                        state.available -= balance_transfer.amount;
                    } else {
                        // a deposit was charged back. decrease state.held but not state.available
                        state.held -= balance_transfer.amount;
                        // state.available was already deducted at the time of the dispute. don't need to deduct it here.
                    }
                    state.locked = LockedState::Locked;
                }
            }
        }

        state.total = state.available + state.held;
        self.db.update_client_state(&state)?;

        Ok(())
    }

    pub fn validate_raw_input(&self, txn: &RawTxnInput) -> Option<Txn> {
        match txn.txn_type {
            TxnType::Invalid => None,
            TxnType::Deposit => {
                let amount = txn.amount.unwrap_or(-1.0);
                if amount <= 0.0 {
                    return None;
                }
                Some(Txn::BalanceTransfer(BalanceTransfer {
                    client_id: txn.client_id,
                    txn_id: txn.txn_id,
                    amount,
                }))
            }
            TxnType::Withdrawal => {
                let amount = txn.amount.unwrap_or(-1.0);
                if amount <= 0.0 {
                    return None;
                }
                Some(Txn::BalanceTransfer(BalanceTransfer {
                    client_id: txn.client_id,
                    txn_id: txn.txn_id,
                    amount: -amount,
                }))
            }
            TxnType::Dispute => {
                if txn.amount.is_some() {
                    return None;
                }
                Some(Txn::Dispute {
                    client_id: txn.client_id,
                    txn_id: txn.txn_id,
                })
            }
            TxnType::Resolve => {
                if txn.amount.is_some() {
                    return None;
                }
                Some(Txn::Resolve {
                    client_id: txn.client_id,
                    txn_id: txn.txn_id,
                })
            }
            TxnType::Chargeback => {
                if txn.amount.is_some() {
                    return None;
                }
                Some(Txn::Chargeback {
                    client_id: txn.client_id,
                    txn_id: txn.txn_id,
                })
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn init() -> TransactionProcessor {
        let _ = env_logger::builder().is_test(true).try_init();
        TransactionProcessor::new().unwrap()
    }

    fn apply_transactions(csv: &str, processor: &mut TransactionProcessor) {
        let mut csv_reader = csv::Reader::from_reader(csv.as_bytes());
        for mut string_record in csv_reader.records().flatten() {
            string_record.trim();
            // deserialize it, skip invalid formats
            if let Ok(txn) = string_record.deserialize(None) {
                processor.process(txn).unwrap();
            }
        }
    }

    #[test]
    fn test_deposit_withdraw() {
        let mut tp = init();
        let csv = "type,client,tx,amount
                        deposit,1,1,1.0
                        deposit,2,2,2.0
                        deposit,1,3,100
                        withdrawal,1,4,50
                        withdrawal,2,5,3";
        apply_transactions(csv, &mut tp);
        let client1 = tp.db.get_client_state(1).unwrap().unwrap();
        assert_eq!(client1.available, 51.0);
        assert_eq!(client1.total, 51.0);
        assert_eq!(client1.held, 0.0);
        assert!(!client1.is_locked());

        let client2 = tp.db.get_client_state(2).unwrap().unwrap();
        assert_eq!(client2.available, 2.0);
        assert_eq!(client2.total, 2.0);
        assert_eq!(client2.held, 0.0);
        assert!(!client2.is_locked());
    }

    #[test]
    fn test_many_accounts() {
        let mut tp = init();
        let csv = "type,client,tx,amount
                        deposit,1,1,1.0
                        deposit,2,2,2.0
                        deposit,3,3,3.0
                        deposit,4,4,4.0
                        deposit,5,5,5.0
                        deposit,6,6,6.0
                        deposit,7,7,7.0
                        deposit,8,8,8.0";
        apply_transactions(csv, &mut tp);

        for i in 1..9 {
            let client = tp.db.get_client_state(i).unwrap().unwrap();
            assert_eq!(client.available, i as f64);
            assert_eq!(client.total, i as f64);
            assert_eq!(client.held, 0.0);
            assert!(!client.is_locked());
        }
    }

    #[test]
    fn test_dispute_deposit() {
        let mut tp = init();
        let csv = "type,client,tx,amount
                        deposit,1,10,1.0
                        dispute,1,10,";
        apply_transactions(csv, &mut tp);
        let client1 = tp.db.get_client_state(1).unwrap().unwrap();
        assert_eq!(client1.available, 0.0);
        assert_eq!(client1.total, 1.0);
        assert_eq!(client1.held, 1.0);
        assert!(!client1.is_locked());
    }

    #[test]
    fn test_dispute_deposit2() {
        let mut tp = init();
        let csv = "type,client,tx,amount
                        deposit,1,10,1.0
                        withdrawal,1,11,1.0
                        dispute,1,10,";
        apply_transactions(csv, &mut tp);
        let client1 = tp.db.get_client_state(1).unwrap().unwrap();
        assert_eq!(client1.available, -1.0);
        assert_eq!(client1.total, 0.0);
        assert_eq!(client1.held, 1.0);
        assert!(!client1.is_locked());
    }

    #[test]
    fn test_chargeback_deposit() {
        let mut tp = init();
        let csv = "type,client,tx,amount
                        deposit,1,10,1.0
                        dispute,1,10,
                        chargeback,1,10,";
        apply_transactions(csv, &mut tp);
        let client1 = tp.db.get_client_state(1).unwrap().unwrap();
        assert_eq!(client1.available, 0.0);
        assert_eq!(client1.total, 0.0);
        assert_eq!(client1.held, 0.0);
        assert!(client1.is_locked());
    }

    #[test]
    fn test_chargeback_deposit2() {
        let mut tp = init();
        let csv = "type,client,tx,amount
                        deposit,1,10,1.0
                        withdrawal,1,11,1.0
                        dispute,1,10,
                        chargeback,1,10,";
        apply_transactions(csv, &mut tp);
        let client1 = tp.db.get_client_state(1).unwrap().unwrap();
        assert_eq!(client1.available, -1.0);
        assert_eq!(client1.total, -1.0);
        assert_eq!(client1.held, 0.0);
        assert!(client1.is_locked());
    }

    #[test]
    fn test_dispute_withdrawal() {
        let mut tp = init();
        let csv = "type,client,tx,amount
                        deposit,1,10,1.0
                        withdrawal,1,11,1.0
                        dispute,1,11,";
        apply_transactions(csv, &mut tp);
        let client1 = tp.db.get_client_state(1).unwrap().unwrap();
        assert_eq!(client1.available, 0.0);
        assert_eq!(client1.total, 1.0);
        assert_eq!(client1.held, 1.0);
        assert!(!client1.is_locked());
    }

    #[test]
    fn test_resolve_withdrawal() {
        let mut tp = init();
        let csv = "type,client,tx,amount
                        deposit,1,10,1.0
                        withdrawal,1,11,1.0
                        dispute,1,11,
                        resolve,1,11,";
        apply_transactions(csv, &mut tp);
        let client1 = tp.db.get_client_state(1).unwrap().unwrap();
        assert_eq!(client1.available, 0.0);
        assert_eq!(client1.total, 0.0);
        assert_eq!(client1.held, 0.0);
        assert!(!client1.is_locked());
    }

    #[test]
    fn test_chargeback_withdrawal() {
        let mut tp = init();
        let csv = "type,client,tx,amount
                        deposit,1,10,1.0
                        withdrawal,1,11,1.0
                        dispute,1,11,
                        chargeback,1,11,";
        apply_transactions(csv, &mut tp);
        let client1 = tp.db.get_client_state(1).unwrap().unwrap();
        assert_eq!(client1.available, 1.0);
        assert_eq!(client1.total, 1.0);
        assert_eq!(client1.held, 0.0);
        assert!(client1.is_locked());
    }
}

/*

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
self.clients.insert(txn.client_id, state); */
