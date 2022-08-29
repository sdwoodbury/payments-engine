use crate::{errors::*, fmt_error, model::*};
use error_stack::{IntoReport, Result, ResultExt};
use rusqlite::{params, Connection};
use std::{fs, path::Path};

// todo: take the file name and delete the file on drop.
pub struct TxnDb {
    file_name: String,
    conn: Connection,
}

// clean up the file system. don't want successive runs to interfere with each other.
impl std::ops::Drop for TxnDb {
    fn drop(&mut self) {
        let path = Path::new(&self.file_name);
        if fs::remove_file(path).is_err() {
            // todo: error
        }
    }
}

impl TxnDb {
    pub fn new(file_name: &str) -> Result<Self, MyError> {
        let path = Path::new(file_name);
        let should_drop = path.exists();
        let conn = Connection::open(path)
            .report()
            .attach_printable_lazy(|| fmt_error!("failed to open txn db"))
            .change_context(MyError::Db)?;

        if should_drop {
            conn.execute("DROP TABLE IF EXISTS txns", [])
                .report()
                .attach_printable_lazy(|| fmt_error!("failed to drop table"))
                .change_context(MyError::Db)?;
        }

        // create pending_transactions table
        conn.execute(
            "CREATE TABLE txns (
                        txn_type INTEGER NOT NULL,
                        client_id INTEGER NOT NULL,
                        txn_id INTEGER NOT NULL,
                        amount NUMERIC NOT NULL,
                        PRIMARY KEY (txn_id)
                    )",
            [],
        )
        .report()
        .attach_printable_lazy(|| fmt_error!("failed to create table"))
        .change_context(MyError::Db)?;

        Ok(Self {
            file_name: file_name.into(),
            conn,
        })
    }

    pub fn insert_txn(&mut self, txn: &Txn) -> Result<(), MyError> {
        if !matches!(txn.txn_type, TxnType::Withdrawal | TxnType::Deposit) {
            return Ok(());
        }

        if txn.amount.is_none() {
            return Ok(());
        }

        self.conn
            .execute(
                "INSERT INTO txns VALUES (?1, ?2, ?3, ?4)",
                params![
                    &txn.txn_type.to_i64(),
                    txn.client_id,
                    txn.txn_id,
                    txn.amount.unwrap()
                ],
            )
            .report()
            .attach_printable_lazy(|| fmt_error!("failed to insert txn"))
            .change_context(MyError::Db)?;

        Ok(())
    }

    pub fn get_txn(&self, id: TransactionId) -> Result<Option<Txn>, MyError> {
        let mut stmt = self
            .conn
            .prepare("SELECT * from txns where txn_id = (?1)")
            .report()
            .attach_printable_lazy(|| fmt_error!("failed to prepare statement"))
            .change_context(MyError::Db)?;

        let mut txn_iter = stmt
            .query_map(params![id], Txn::from_row)
            .report()
            .attach_printable_lazy(|| fmt_error!("failed to execute statement"))
            .change_context(MyError::Db)?;

        let txn = match txn_iter.next() {
            Some(r) => r
                .report()
                .attach_printable_lazy(|| fmt_error!("somehow failed"))
                .change_context(MyError::Db)?,
            None => return Ok(None),
        };
        Ok(Some(txn))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs;

    #[test]
    fn create_db() -> Result<(), MyError> {
        let amount = Some(1.0);
        let _ = fs::remove_file("test_db.db");
        let mut db = TxnDb::new("test_db.db")?;
        let txn1 = Txn {
            txn_type: TxnType::Deposit,
            client_id: 1,
            txn_id: 2,
            amount,
        };

        let res = db.get_txn(txn1.txn_id)?;
        assert!(res.is_none());

        db.insert_txn(&txn1)?;
        let res = db.get_txn(txn1.txn_id)?;
        assert!(res.is_some());
        Ok(())
    }
}
