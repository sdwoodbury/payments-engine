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
            // deletes will cascade
            conn.execute("DROP TABLE IF EXISTS Clients", [])
                .report()
                .attach_printable_lazy(|| fmt_error!("failed to drop Clients"))
                .change_context(MyError::Db)?;
        }

        conn.execute(
            "CREATE TABLE Clients (
                        client_id INTEGER NOT NULL,
                        available INTEGER NOT NULL,
                        held REAL NOT NULL,
                        total REAL NOT NULL,
                        locked INTEGER NOT NULL,
                        PRIMARY KEY (client_id)
                    )",
            [],
        )
        .report()
        .attach_printable_lazy(|| fmt_error!("failed to create Clients table"))
        .change_context(MyError::Db)?;

        conn.execute(
            "CREATE TABLE BalanceTransfers (
                        client_id INTEGER NOT NULL,
                        txn_id INTEGER NOT NULL UNIQUE,
                        amount REAL NOT NULL,
                        PRIMARY KEY (client_id, txn_id),
                        FOREIGN KEY (client_id) REFERENCES Clients(client_id) ON DELETE CASCADE
                    )",
            [],
        )
        .report()
        .attach_printable_lazy(|| fmt_error!("failed to create BalanceTransfers table"))
        .change_context(MyError::Db)?;

        conn.execute(
            "CREATE TABLE Disputes (
                        client_id INTEGER NOT NULL,
                        txn_id INTEGER NOT NULL,
                        PRIMARY KEY (client_id, txn_id),
                        FOREIGN KEY (client_id, txn_id) REFERENCES BalanceTransfers(client_id, txn_id) ON DELETE CASCADE
                    )",
            [],
        )
        .report()
        .attach_printable_lazy(|| fmt_error!("failed to create Disputes table"))
        .change_context(MyError::Db)?;

        conn.execute(
            "CREATE TABLE Resolutions (
                        client_id INTEGER NOT NULL,
                        txn_id INTEGER NOT NULL,
                        status INTEGER NOT NULL,
                        PRIMARY KEY (client_id, txn_id),
                        FOREIGN KEY (client_id, txn_id) REFERENCES Disputes(client_id, txn_id) ON DELETE CASCADE
                    )",
            [],
        )
        .report()
        .attach_printable_lazy(|| fmt_error!("failed to create Resolutions table"))
        .change_context(MyError::Db)?;

        Ok(Self {
            file_name: file_name.into(),
            conn,
        })
    }

    // call this if get_client_state returns None
    pub fn create_client_state(&mut self, client_id: ClientId) -> Result<ClientState, MyError> {
        let client_state = ClientState::new(client_id);
        let locked = client_state.locked.to_u8();
        self.conn
            .execute(
                "INSERT INTO Clients VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    &client_state.client_id,
                    &client_state.available,
                    &client_state.held,
                    &client_state.total,
                    &locked,
                ],
            )
            .report()
            .attach_printable_lazy(|| fmt_error!("failed to create new Client"))
            .change_context(MyError::Db)?;
        Ok(client_state)
    }

    // search for a client state (an account) by client ID
    // return None if not found
    pub fn get_client_state(
        &mut self,
        client_id: ClientId,
    ) -> Result<Option<ClientState>, MyError> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM Clients WHERE client_id=(?1)")
            .report()
            .attach_printable_lazy(|| fmt_error!("failed to prepare statement"))
            .change_context(MyError::Db)?;

        let mut iter = stmt
            .query_map(params![&client_id], ClientState::from_row)
            .report()
            .attach_printable_lazy(|| fmt_error!("failed to get query iterator"))
            .change_context(MyError::Db)?;

        if let Some(r) = iter.next() {
            let state = r
                .report()
                .attach_printable_lazy(|| fmt_error!("failed to get row from Clients"))
                .change_context(MyError::Db)?;
            Ok(Some(state))
        } else {
            Ok(None)
        }
    }

    // used to display client account information
    // it's difficult to return an iterator to a query because the query only lives as long as the Statement. that's why this function accepts a closure
    pub fn process_all_clients<F>(&self, f: F) -> Result<(), MyError>
    where
        F: Fn(ClientState),
    {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM Clients")
            .report()
            .attach_printable_lazy(|| fmt_error!("failed to prepare statement"))
            .change_context(MyError::Db)?;

        let iter = stmt
            .query_map(params![], ClientState::from_row)
            .report()
            .attach_printable_lazy(|| fmt_error!("failed to get query iterator"))
            .change_context(MyError::Db)?;

        for state in iter.flatten() {
            f(state);
        }

        Ok(())
    }

    pub fn update_client_state(&mut self, client_state: &ClientState) -> Result<(), MyError> {
        let locked = client_state.locked.to_u8();
        self.conn.execute(
            "UPDATE Clients SET available=(?1), held=(?2), total=(?3), locked=(?4) WHERE client_id=(?5)",
            params![&client_state.available, &client_state.held, &client_state.total, &locked, &client_state.client_id,],
        ).report()
        .attach_printable_lazy(|| fmt_error!("failed to update Clients"))
        .change_context(MyError::Db)?;
        Ok(())
    }

    // returns true if the operation succeeded
    // return false if the operation violated a SQL constraint
    // otherwise return an error
    pub fn try_insert_balance_transfer(&mut self, txn: BalanceTransfer) -> Result<bool, MyError> {
        let res = self.conn.execute(
            "INSERT INTO BalanceTransfers VALUES (?1, ?2, ?3)",
            params![&txn.client_id, txn.txn_id, txn.amount,],
        );

        match res {
            Ok(_) => Ok(true),
            Err(e) => {
                filter_sql_errors(e)
                    .report()
                    .attach_printable_lazy(|| fmt_error!("failed to apply balance transfer"))
                    .change_context(MyError::Db)?;
                Ok(false)
            }
        }
    }

    // returns true if the operation succeeded
    // return false if the operation violated a SQL constraint
    // otherwise return an error
    pub fn try_insert_dispute(
        &mut self,
        client_id: ClientId,
        txn_id: TransactionId,
    ) -> Result<bool, MyError> {
        let res = self.conn.execute(
            "INSERT INTO Disputes VALUES (?1, ?2)",
            params![&client_id, &txn_id,],
        );
        match res {
            Ok(_) => Ok(true),
            Err(e) => {
                filter_sql_errors(e)
                    .report()
                    .attach_printable_lazy(|| fmt_error!("failed to add dispute"))
                    .change_context(MyError::Db)?;
                Ok(false)
            }
        }
    }

    // returns true if the operation succeeded
    // return false if the operation violated a SQL constraint
    // otherwise return an error
    pub fn try_resolve_dispute(
        &mut self,
        client_id: ClientId,
        txn_id: TransactionId,
    ) -> Result<bool, MyError> {
        let status = DisputeStatus::Resolved.to_u8();
        let res = self.conn.execute(
            "INSERT INTO Resolutions VALUES (?1, ?2, ?3)",
            params![&client_id, &txn_id, &status,],
        );
        match res {
            Ok(_) => Ok(true),
            Err(e) => {
                filter_sql_errors(e)
                    .report()
                    .attach_printable_lazy(|| fmt_error!("failed to apply resolution"))
                    .change_context(MyError::Db)?;
                Ok(false)
            }
        }
    }

    // returns true if the operation succeeded
    // return false if the operation violated a SQL constraint
    // otherwise return an error
    pub fn try_chargeback_dispute(
        &mut self,
        client_id: ClientId,
        txn_id: TransactionId,
    ) -> Result<bool, MyError> {
        let status = DisputeStatus::Chargeback.to_u8();
        let res = self.conn.execute(
            "INSERT INTO Resolutions VALUES (?1, ?2, ?3)",
            params![&client_id, &txn_id, &status,],
        );
        match res {
            Ok(_) => Ok(true),
            Err(e) => {
                filter_sql_errors(e)
                    .report()
                    .attach_printable_lazy(|| fmt_error!("failed to apply chargeback"))
                    .change_context(MyError::Db)?;
                Ok(false)
            }
        }
    }

    // return the balance transfer is it exists in the database
    // return None if not found
    // return an error on database failure
    pub fn get_balance_transfer(
        &self,
        client_id: ClientId,
        txn_id: TransactionId,
    ) -> Result<Option<BalanceTransfer>, MyError> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM BalanceTransfers WHERE client_id = (?1) AND txn_id = (?2)")
            .report()
            .attach_printable_lazy(|| fmt_error!("failed to prepare statement"))
            .change_context(MyError::Db)?;

        let mut txn_iter = stmt
            .query_map(params![client_id, txn_id], BalanceTransfer::from_row)
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

// certain operations are expected to fail due to constraint violations. filter these errors out
fn filter_sql_errors(e: rusqlite::Error) -> rusqlite::Result<(), rusqlite::Error> {
    if let rusqlite::Error::SqliteFailure(ffi, _) = e {
        if ffi.code == rusqlite::ffi::ErrorCode::ConstraintViolation {
            return Ok(());
        }
    }

    Err(e)
}

#[cfg(test)]
mod test {
    use super::*;
    use random_string::generate;

    fn init() -> TxnDb {
        let _ = env_logger::builder().is_test(true).try_init();
        let charset = "abcdefghijklmnopqrstuvwxyz";
        TxnDb::new(&format!("{}.db", generate(6, charset)))
            .attach_printable_lazy(|| fmt_error!("database failure"))
            .unwrap()
    }

    #[test]
    fn test_create_client() {
        let mut db = init();
        let client = match db.create_client_state(123) {
            Ok(c) => c,
            Err(e) => {
                print_report(e);
                assert!(false);
                // to make the compiler happy
                ClientState::new(123)
            }
        };

        let retrieved = match db.get_client_state(client.client_id) {
            Ok(c) => c,
            Err(e) => {
                print_report(e);
                assert!(false);
                // to make the compiler happy
                None
            }
        };

        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.client_id, client.client_id);
    }

    #[test]
    fn test_update_client() {
        let mut db = init();
        let mut client = match db.create_client_state(123) {
            Ok(c) => c,
            Err(e) => {
                print_report(e);
                assert!(false);
                // to make the compiler happy
                ClientState::new(123)
            }
        };
        assert_eq!(client.available, 0.0);

        client.available = 1.0;
        if let Err(e) = db.update_client_state(&client) {
            print_report(e);
            assert!(false);
        };

        let retrieved = match db.get_client_state(client.client_id) {
            Ok(c) => c,
            Err(e) => {
                print_report(e);
                assert!(false);
                // to make the compiler happy
                None
            }
        };

        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.available, 1.0);
    }

    #[test]
    fn test_get_client_negative() {
        let mut db = init();
        let retrieved = match db.get_client_state(123) {
            Ok(c) => c,
            Err(e) => {
                print_report(e);
                assert!(false);
                // to make the compiler happy
                None
            }
        };
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_balance_transfer_without_client() {
        let mut db = init();
        let xfer = BalanceTransfer {
            client_id: 123,
            txn_id: 1,
            amount: 1.0,
        };

        let res = db.try_insert_balance_transfer(xfer).unwrap();
        assert!(!res);
    }

    #[test]
    fn test_duplicate_balance_transfer() {
        let mut db = init();
        let _ = db.create_client_state(123);
        let xfer = BalanceTransfer {
            client_id: 123,
            txn_id: 1,
            amount: 1.0,
        };

        let mut res = db.try_insert_balance_transfer(xfer).unwrap();
        assert!(res);

        res = db.try_insert_balance_transfer(xfer).unwrap();
        assert!(!res);
    }

    #[test]
    fn test_get_balance_transfer() {
        let mut db = init();
        let _ = db.create_client_state(123);
        let xfer = BalanceTransfer {
            client_id: 123,
            txn_id: 1,
            amount: 1.0,
        };

        let res = db.try_insert_balance_transfer(xfer).unwrap();
        assert!(res);

        let res = db
            .get_balance_transfer(xfer.client_id, xfer.txn_id)
            .unwrap();
        assert!(res.is_some());
        let res = res.unwrap();
        assert_eq!(res.amount, 1.0);
    }

    #[test]
    fn test_dispute() {
        let mut db = init();
        let _ = db.create_client_state(123);
        let xfer = BalanceTransfer {
            client_id: 123,
            txn_id: 1,
            amount: 1.0,
        };

        let mut res = db.try_insert_balance_transfer(xfer).unwrap();
        assert!(res);

        res = db.try_insert_dispute(xfer.client_id, xfer.txn_id).unwrap();
        assert!(res);

        res = db.try_insert_dispute(xfer.client_id, xfer.txn_id).unwrap();
        assert!(!res);
    }

    #[test]
    fn test_dispute_without_client() {
        let mut db = init();
        let xfer = BalanceTransfer {
            client_id: 123,
            txn_id: 1,
            amount: 1.0,
        };

        let res = db.try_insert_dispute(xfer.client_id, xfer.txn_id).unwrap();
        assert!(!res);
    }

    #[test]
    fn test_chargeback_dispute() {
        let mut db = init();
        let _ = db.create_client_state(123);
        let xfer = BalanceTransfer {
            client_id: 123,
            txn_id: 1,
            amount: 1.0,
        };

        let mut res = db.try_insert_balance_transfer(xfer).unwrap();
        assert!(res);

        res = db.try_insert_dispute(xfer.client_id, xfer.txn_id).unwrap();
        assert!(res);

        res = db
            .try_chargeback_dispute(xfer.client_id, xfer.txn_id)
            .unwrap();
        assert!(res);

        res = db.try_resolve_dispute(xfer.client_id, xfer.txn_id).unwrap();
        assert!(!res);

        res = db.try_insert_dispute(xfer.client_id, xfer.txn_id).unwrap();
        assert!(!res);
    }

    #[test]
    fn test_resolve_dispute() {
        let mut db = init();
        let _ = db.create_client_state(123);
        let xfer = BalanceTransfer {
            client_id: 123,
            txn_id: 1,
            amount: 1.0,
        };

        let mut res = db.try_insert_balance_transfer(xfer).unwrap();
        assert!(res);

        res = db.try_insert_dispute(xfer.client_id, xfer.txn_id).unwrap();
        assert!(res);

        res = db.try_resolve_dispute(xfer.client_id, xfer.txn_id).unwrap();
        assert!(res);

        // duplicate dispute
        res = db.try_insert_dispute(xfer.client_id, xfer.txn_id).unwrap();
        assert!(!res);

        res = db
            .try_chargeback_dispute(xfer.client_id, xfer.txn_id)
            .unwrap();
        assert!(!res);
    }
}
