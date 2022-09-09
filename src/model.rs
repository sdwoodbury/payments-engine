use crate::errors::*;
use serde::Deserialize;
use std::{fmt, str::FromStr};

pub type ClientId = u16;
pub type TransactionId = u32;

#[derive(Clone)]
pub enum LockedState {
    Invalid,
    Locked,
    Unlocked,
}
impl LockedState {
    pub fn to_u8(&self) -> u8 {
        match self {
            LockedState::Invalid => 0,
            LockedState::Locked => 1,
            LockedState::Unlocked => 2,
        }
    }
}

impl std::convert::From<u8> for LockedState {
    fn from(val: u8) -> LockedState {
        match val {
            1 => LockedState::Locked,
            2 => LockedState::Unlocked,
            _ => LockedState::Invalid,
        }
    }
}

impl fmt::Display for LockedState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            LockedState::Invalid => "invalid",
            LockedState::Locked => "true",
            LockedState::Unlocked => "false",
        };
        write!(f, "{}", s)
    }
}

/// Represents a Client's account when all transactions up to `last_txn_processed` have been processed
#[derive(Clone)]
pub struct ClientState {
    pub client_id: ClientId,
    /// liquid funds
    pub available: f64,
    /// disputed funds
    pub held: f64,
    /// avail + held
    pub total: f64,
    /// set to true if the account is frozen. happens in the event of a chargeback
    pub locked: LockedState,
}

impl ClientState {
    pub fn init(client_id: ClientId) -> Self {
        ClientState {
            client_id,
            available: 0.0,
            held: 0.0,
            total: 0.0,
            locked: LockedState::Unlocked,
        }
    }
    pub fn from_row(row: &rusqlite::Row<'_>) -> std::result::Result<Self, rusqlite::Error> {
        let locked: u8 = row.get(4)?;
        Ok(ClientState {
            client_id: row.get(0)?,
            available: row.get(1)?,
            held: row.get(2)?,
            total: row.get(3)?,
            locked: locked.into(),
        })
    }

    pub fn is_locked(&self) -> bool {
        match self.locked {
            LockedState::Locked | LockedState::Invalid => true,
            _ => false,
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
    pub fn to_u8(&self) -> u8 {
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

impl std::convert::From<u8> for TxnType {
    fn from(val: u8) -> TxnType {
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

/// a deserialized input
#[derive(Deserialize, Debug, Clone)]
pub struct RawTxnInput {
    #[serde(rename = "type")]
    pub txn_type: TxnType,
    /// a globally unique client ID
    #[serde(rename = "client")]
    pub client_id: ClientId,
    /// a globally unique transaction ID
    #[serde(rename = "tx")]
    pub txn_id: TransactionId,
    pub amount: Option<f64>,
}

/// either a deposit or withdrawal
/// for deposits, amount is positive. for withdrawal, amount is negative
#[derive(Clone, Copy)]
pub struct BalanceTransfer {
    pub client_id: ClientId,
    pub txn_id: TransactionId,
    pub amount: f64,
}

impl BalanceTransfer {
    pub fn from_row(row: &rusqlite::Row<'_>) -> std::result::Result<Self, rusqlite::Error> {
        Ok(BalanceTransfer {
            client_id: row.get(0)?,
            txn_id: row.get(1)?,
            amount: row.get(2)?,
        })
    }
}

/// RawTxnInput gets processed into this
pub enum Txn {
    BalanceTransfer(BalanceTransfer),
    Dispute {
        client_id: ClientId,
        txn_id: TransactionId,
    },
    Resolve {
        client_id: ClientId,
        txn_id: TransactionId,
    },
    Chargeback {
        client_id: ClientId,
        txn_id: TransactionId,
    },
}

#[derive(PartialEq, Eq)]
pub enum DisputeStatus {
    Invalid,
    Open,
    Resolved,
    Chargeback,
}

impl DisputeStatus {
    pub fn to_u8(&self) -> u8 {
        match self {
            DisputeStatus::Invalid => 0,
            DisputeStatus::Open => 1,
            DisputeStatus::Resolved => 2,
            DisputeStatus::Chargeback => 3,
        }
    }
}

impl std::convert::From<u8> for DisputeStatus {
    fn from(val: u8) -> DisputeStatus {
        match val {
            1 => DisputeStatus::Open,
            2 => DisputeStatus::Resolved,
            3 => DisputeStatus::Chargeback,
            _ => DisputeStatus::Invalid,
        }
    }
}

pub struct Dispute {
    pub client_id: ClientId,
    pub txn_id: TransactionId,
    pub status: DisputeStatus,
}

impl Dispute {
    pub fn from_row(row: &rusqlite::Row<'_>) -> std::result::Result<Self, rusqlite::Error> {
        let status: u8 = row.get(2)?;
        Ok(Dispute {
            client_id: row.get(0)?,
            txn_id: row.get(1)?,
            status: status.into(),
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
            let txn: Result<RawTxnInput, _> = record.deserialize(None);
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
            let txn: Result<RawTxnInput, _> = record.deserialize(None);
            println!("{:?}", txn);
            assert!(txn.is_ok());
        }
    }

    #[test]
    fn print_client_state() -> Result<(), Box<dyn std::error::Error>> {
        let state = ClientState {
            client_id: 1,
            available: 2.0,
            held: 1.7,
            total: 3.7,
            locked: LockedState::Unlocked,
        };

        let s = format!("{}", state);
        assert_eq!("2,1.7,3.7,false", s.as_str());

        Ok(())
    }
}
