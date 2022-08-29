use crate::errors::*;
use serde::Deserialize;
use std::{fmt, str::FromStr};

pub type ClientId = u16;
pub type TransactionId = u32;

/// Represents a Client's account when all transactions up to `last_txn_processed` have been processed
#[derive(Clone)]
pub struct ClientState {
    /// liquid funds
    pub available: f32,
    /// disputed funds
    pub held: f32,
    /// avail + held
    pub total: f32,
    /// set to true if the account is frozen. happens in the event of a chargeback
    pub locked: bool,
}

impl ClientState {
    pub fn init() -> Self {
        ClientState {
            available: 0.0,
            held: 0.0,
            total: 0.0,
            locked: false,
        }
    }
}

// used for printing the output per coding challenge instructions
impl fmt::Display for ClientState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{},{},{},{}",
            self.available, self.held, self.total, self.locked
        )
    }
}

/// all possible transaction types
#[derive(Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TxnType {
    Invalid,
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl TxnType {
    pub fn to_i64(&self) -> i64 {
        match self {
            TxnType::Invalid => 0,
            TxnType::Deposit => 1,
            TxnType::Withdrawal => 2,
            TxnType::Dispute => 3,
            TxnType::Resolve => 4,
            TxnType::Chargeback => 5,
        }
    }
}

impl std::convert::From<i64> for TxnType {
    fn from(val: i64) -> TxnType {
        match val {
            1 => TxnType::Deposit,
            2 => TxnType::Withdrawal,
            3 => TxnType::Dispute,
            4 => TxnType::Resolve,
            5 => TxnType::Chargeback,
            _ => TxnType::Invalid,
        }
    }
}

impl FromStr for TxnType {
    type Err = MyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let txn = match s {
            "deposit" => TxnType::Deposit,
            "withdrawal" => TxnType::Withdrawal,
            "dispute" => TxnType::Dispute,
            "resolve" => TxnType::Resolve,
            "chargeback" => TxnType::Chargeback,
            _ => return Err(MyError::Conversion(s.to_string())),
        };
        Ok(txn)
    }
}

/// a deserialized transaction
#[derive(Deserialize, Debug, Clone)]
pub struct Txn {
    #[serde(rename = "type")]
    pub txn_type: TxnType,
    /// a globally unique client ID
    #[serde(rename = "client")]
    pub client_id: ClientId,
    /// a globally unique transaction ID
    #[serde(rename = "tx")]
    pub txn_id: TransactionId,
    pub amount: Option<f32>,
}

// don't want to mess with the option stuff when using the database.
#[derive(Deserialize, Debug, Clone)]
pub struct DbTxn {
    pub txn_type: TxnType,
    /// a globally unique client ID
    pub client_id: ClientId,
    /// a globally unique transaction ID
    pub txn_id: TransactionId,
    pub amount: f32,
}

impl Txn {
    pub fn from_row(row: &rusqlite::Row<'_>) -> std::result::Result<Self, rusqlite::Error> {
        let t: i64 = row.get(0)?;
        Ok(Txn {
            txn_type: TxnType::from(t),
            client_id: row.get(1)?,
            txn_id: row.get(2)?,
            amount: row.get(3)?,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_csv_test1() {
        let csv = "type,      client,      tx, amount
              deposit,     1,    1,     1.0
              deposit,   2,    2,  2.0";

        let mut reader = csv::Reader::from_reader(csv.as_bytes());

        for item in reader.records() {
            let mut record = item.unwrap();
            record.trim();
            let txn: Result<Txn, _> = record.deserialize(None);
            println!("{:?}", txn);
            assert!(txn.is_ok());
        }
    }

    #[test]
    fn parse_csv_test2() {
        let csv = "type,client,tx,amount
              deposit,1,1,1.0
              deposit,2,2,2.0";

        let mut reader = csv::Reader::from_reader(csv.as_bytes());

        for item in reader.records() {
            let mut record = item.unwrap();
            record.trim();
            let txn: Result<Txn, _> = record.deserialize(None);
            println!("{:?}", txn);
            assert!(txn.is_ok());
        }
    }

    #[test]
    fn print_client_state() -> Result<(), Box<dyn std::error::Error>> {
        let state = ClientState {
            available: 2.0,
            held: 1.7,
            total: 3.7,
            locked: false,
        };

        let s = format!("{}", state);
        assert_eq!("2,1.7,3.7,false", s.as_str());

        Ok(())
    }
}
