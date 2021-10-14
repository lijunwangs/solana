/// A concurrent implementation for writing accounts into the PostgreSQL in parallel.
use {
    crate::accountsdb_plugin_postgres::{
        AccountsDbPluginPostgresConfig, AccountsDbPluginPostgresError,
    },
    chrono::Utc,
    crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender},
    log::*,
    postgres::{Client, NoTls, Statement},
    solana_accountsdb_plugin_interface::accountsdb_plugin_interface::{
        AccountsDbPluginError, ReplicaAccountInfo, SlotStatus,
    },
    solana_metrics::datapoint_info,
    solana_sdk::timing::AtomicInterval,
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
    tokio_postgres::types::ToSql,
};

/// The maximum asynchronous requests allowed in the channel to avoid excessive
/// memory usage. The downside -- calls after this threshold is reached can get blocked.
const MAX_ASYNC_REQUESTS: usize = 10240;
const DEFAULT_ACCOUNTS_INSERT_BATCH_SIZE: usize = 10;
const ACCOUNT_COLUMN_COUNT: usize = 8;

struct PostgresSqlClientWrapper {
    client: Client,
    update_account_stmt: Statement,
    bulk_account_insert_stmt: Statement,
}

pub struct SimplePostgresClient {
    batch_size: usize,
    accounts: Vec<DbAccountInfo>,
    client: Mutex<PostgresSqlClientWrapper>,
}

struct PostgresClientWorker {
    client: SimplePostgresClient,
}

impl Eq for DbAccountInfo {}

#[derive(Clone, PartialEq, Debug)]
pub struct DbAccountInfo {
    pub pubkey: Vec<u8>,
    pub lamports: i64,
    pub owner: Vec<u8>,
    pub executable: bool,
    pub rent_epoch: i64,
    pub data: Vec<u8>,
    pub slot: i64,
}

impl DbAccountInfo {
    fn new<T: ReadableAccountInfo>(account: &T, slot: u64) -> DbAccountInfo {
        let data = account.data().to_vec();
        Self {
            pubkey: account.pubkey().to_vec(),
            lamports: account.lamports() as i64,
            owner: account.owner().to_vec(),
            executable: account.executable(),
            rent_epoch: account.rent_epoch() as i64,
            data,
            slot: slot as i64,
        }
    }
}

pub trait ReadableAccountInfo: Sized {
    fn pubkey(&self) -> &[u8];
    fn owner(&self) -> &[u8];
    fn lamports(&self) -> i64;
    fn executable(&self) -> bool;
    fn rent_epoch(&self) -> i64;
    fn data(&self) -> &[u8];
}

impl ReadableAccountInfo for DbAccountInfo {
    fn pubkey(&self) -> &[u8] {
        &self.pubkey
    }

    fn owner(&self) -> &[u8] {
        &self.owner
    }

    fn lamports(&self) -> i64 {
        self.lamports
    }

    fn executable(&self) -> bool {
        self.executable
    }

    fn rent_epoch(&self) -> i64 {
        self.rent_epoch
    }

    fn data(&self) -> &[u8] {
        &self.data
    }
}

impl<'a> ReadableAccountInfo for ReplicaAccountInfo<'a> {
    fn pubkey(&self) -> &[u8] {
        self.pubkey
    }

    fn owner(&self) -> &[u8] {
        self.owner
    }

    fn lamports(&self) -> i64 {
        self.lamports as i64
    }

    fn executable(&self) -> bool {
        self.executable
    }

    fn rent_epoch(&self) -> i64 {
        self.rent_epoch as i64
    }

    fn data(&self) -> &[u8] {
        self.data
    }
}

pub trait PostgresClient {
    fn join(&mut self) -> thread::Result<()> {
        Ok(())
    }

    fn update_account(
        &mut self,
        account: DbAccountInfo,
        at_startup: bool,
    ) -> Result<(), AccountsDbPluginError>;

    fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> Result<(), AccountsDbPluginError>;
}

impl SimplePostgresClient {
    fn connect_to_db(
        config: &AccountsDbPluginPostgresConfig,
    ) -> Result<Client, AccountsDbPluginError> {
        let connection_str = if let Some(port) = config.port {
            format!("host={} user={} port={}", config.host, config.user, port)
        } else {
            format!("host={} user={}", config.host, config.user)
        };

        match Client::connect(&connection_str, NoTls) {
            Err(err) => {
                let msg = format!(
                        "Error in connecting to the PostgreSQL database: {:?} host: {:?} user: {:?} config: {:?}",
                        err, config.host, config.user, connection_str);
                error!("{}", msg);
                return Err(AccountsDbPluginError::Custom(Box::new(
                    AccountsDbPluginPostgresError::DataStoreConnectionError { msg },
                )));
            }
            Ok(client) => Ok(client),
        }
    }

    fn build_bulk_account_insert_statement(
        client: &mut Client,
        config: &AccountsDbPluginPostgresConfig,
    ) -> Result<Statement, AccountsDbPluginError> {
        let batch_size = config
            .batch_size
            .unwrap_or(DEFAULT_ACCOUNTS_INSERT_BATCH_SIZE);
        let mut stmt = String::from("INSERT INTO account (pubkey, slot, owner, lamports, executable, rent_epoch, data, updated_on) VALUES");
        for j in 0..batch_size {
            let row = j * ACCOUNT_COLUMN_COUNT;
            let val_str = format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                row + 1,
                row + 2,
                row + 3,
                row + 4,
                row + 5,
                row + 6,
                row + 7,
                row + 8,
            );

            if j == 0 {
                stmt = format!("{} {}", &stmt, val_str);
            } else {
                stmt = format!("{}, {}", &stmt, val_str);
            }
        }

        stmt = format!("{} ON CONFLICT (pubkey) DO UPDATE SET slot=excluded.slot, owner=excluded.owner, lamports=excluded.lamports, executable=excluded.executable, rent_epoch=excluded.rent_epoch, \
        data=excluded.data, updated_on=excluded.updated_on WHERE slot <= excluded.slot", stmt);

        info!("{}", stmt);
        let bulk_stmt = client.prepare(&stmt);

        match bulk_stmt {
            Err(err) => {
                return Err(AccountsDbPluginError::Custom(Box::new(AccountsDbPluginPostgresError::DataSchemaError {
                    msg: format!(
                        "Error in preparing for the accounts update PostgreSQL database: {} host: {} user: {} config: {:?}",
                        err, config.host, config.user, config
                    ),
                })));
            }
            Ok(update_account_stmt) => Ok(update_account_stmt),
        }
    }

    fn build_single_account_upsert_statement(
        client: &mut Client,
        config: &AccountsDbPluginPostgresConfig,
    ) -> Result<Statement, AccountsDbPluginError> {
        let stmt = "INSERT INTO account (pubkey, slot, owner, lamports, executable, rent_epoch, data, updated_on) \
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
        ON CONFLICT (pubkey) DO UPDATE SET slot=excluded.slot, owner=excluded.owner, lamports=excluded.lamports, executable=excluded.executable, rent_epoch=excluded.rent_epoch, \
        data=excluded.data, updated_on=excluded.updated_on WHERE slot <= excluded.slot";

        let stmt = client.prepare(stmt);

        match stmt {
            Err(err) => {
                return Err(AccountsDbPluginError::Custom(Box::new(AccountsDbPluginPostgresError::DataSchemaError {
                    msg: format!(
                        "Error in preparing for the accounts update PostgreSQL database: {} host: {} user: {} config: {:?}",
                        err, config.host, config.user, config
                    ),
                })));
            }
            Ok(update_account_stmt) => Ok(update_account_stmt),
        }
    }

    fn upsert_account(&mut self, account: &DbAccountInfo) -> Result<(), AccountsDbPluginError> {
        let lamports = account.lamports() as i64;
        let rent_epoch = account.rent_epoch() as i64;
        let updated_on = Utc::now().naive_utc();
        let client = self.client.get_mut().unwrap();
        let result = client.client.query(
            &client.update_account_stmt,
            &[
                &account.pubkey(),
                &account.slot,
                &account.owner(),
                &lamports,
                &account.executable(),
                &rent_epoch,
                &account.data(),
                &updated_on,
            ],
        );

        if let Err(err) = result {
            let msg = format!(
                "Failed to persist the update of account to the PostgreSQL database. Error: {:?}",
                err
            );
            error!("{}", msg);
            return Err(AccountsDbPluginError::AccountsUpdateError { msg });
        }

        Ok(())
    }

    fn insert_accounts_in_batch(
        &mut self,
        account: DbAccountInfo,
    ) -> Result<(), AccountsDbPluginError> {
        self.accounts.push(account);

        if self.accounts.len() == self.batch_size {
            let mut values: Vec<&(dyn ToSql + Sync)> =
                Vec::with_capacity(self.batch_size * ACCOUNT_COLUMN_COUNT);
            let updated_on = Utc::now().naive_utc();
            for j in 0..self.batch_size {
                let account = &self.accounts[j];

                values.push(&account.pubkey);
                values.push(&account.slot);
                values.push(&account.owner);
                values.push(&account.lamports);
                values.push(&account.executable);
                values.push(&account.rent_epoch);
                values.push(&account.data);
                values.push(&updated_on);
            }

            let client = self.client.get_mut().unwrap();
            let result = client
                .client
                .query(&client.bulk_account_insert_stmt, &values);

            self.accounts.clear();
            if let Err(err) = result {
                let msg = format!(
                    "Failed to persist the update of account to the PostgreSQL database. Error: {:?}",
                    err
                );
                error!("{}", msg);
                return Err(AccountsDbPluginError::AccountsUpdateError { msg });
            }
        }
        Ok(())
    }

    pub fn new(config: &AccountsDbPluginPostgresConfig) -> Result<Self, AccountsDbPluginError> {
        info!("Creating SimplePostgresClient...");
        let mut client = Self::connect_to_db(config)?;
        let bulk_account_insert_stmt =
            Self::build_bulk_account_insert_statement(&mut client, config)?;
        let update_account_stmt = Self::build_single_account_upsert_statement(&mut client, config)?;

        let batch_size = config
            .batch_size
            .unwrap_or(DEFAULT_ACCOUNTS_INSERT_BATCH_SIZE);
        info!("Created SimplePostgresClient.");
        Ok(Self {
            batch_size,
            accounts: Vec::with_capacity(batch_size),
            client: Mutex::new(PostgresSqlClientWrapper {
                client,
                update_account_stmt,
                bulk_account_insert_stmt,
            }),
        })
    }
}

impl PostgresClient for SimplePostgresClient {
    fn update_account(
        &mut self,
        account: DbAccountInfo,
        at_startup: bool,
    ) -> Result<(), AccountsDbPluginError> {
        trace!(
            "Updating account {} with owner {} at slot {}",
            bs58::encode(account.pubkey()).into_string(),
            bs58::encode(account.owner()).into_string(),
            account.slot,
        );
        if !at_startup {
            return self.upsert_account(&account);
        }
        self.insert_accounts_in_batch(account)
    }

    fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> Result<(), AccountsDbPluginError> {
        info!("Updating slot {:?} at with status {:?}", slot, status);

        let slot = slot as i64; // postgres only supports i64
        let parent = parent.map(|parent| parent as i64);
        let updated_on = Utc::now().naive_utc();
        let status_str = status.as_str();
        let client = self.client.get_mut().unwrap();

        let result = match parent {
                        Some(parent) => {
                            client.client.execute(
                                "INSERT INTO slot (slot, parent, status, updated_on) \
                                VALUES ($1, $2, $3, $4) \
                                ON CONFLICT (slot) DO UPDATE SET parent=$2, status=$3, updated_on=$4",
                                &[
                                    &slot,
                                    &parent,
                                    &status_str,
                                    &updated_on,
                                ],
                            )
                        }
                        None => {
                            client.client.execute(
                                "INSERT INTO slot (slot, status, updated_on) \
                                VALUES ($1, $2, $3) \
                                ON CONFLICT (slot) DO UPDATE SET status=$2, updated_on=$3",
                                &[
                                    &slot,
                                    &status_str,
                                    &updated_on,
                                ],
                            )
                        }
                };

        match result {
            Err(err) => {
                let msg = format!(
                    "Failed to persist the update of slot to the PostgreSQL database. Error: {:?}",
                    err
                );
                error!("{:?}", msg);
                return Err(AccountsDbPluginError::SlotStatusUpdateError { msg });
            }
            Ok(rows) => {
                assert_eq!(1, rows, "Expected one rows to be updated a time");
            }
        }

        Ok(())
    }
}

struct UpdateAccountRequest {
    account: DbAccountInfo,
    at_startup: bool,
}

struct UpdateSlotRequest {
    slot: u64,
    parent: Option<u64>,
    slot_status: SlotStatus,
}

enum DbWorkItem {
    UpdateAccount(UpdateAccountRequest),
    UpdateSlot(UpdateSlotRequest),
}

impl PostgresClientWorker {
    fn new(config: AccountsDbPluginPostgresConfig) -> Result<Self, AccountsDbPluginError> {
        let result = SimplePostgresClient::new(&config);
        match result {
            Ok(client) => Ok(PostgresClientWorker { client }),
            Err(err) => {
                error!("Error in creating SimplePostgresClient: {}", err);
                Err(err)
            }
        }
    }

    fn do_work(
        &mut self,
        receiver: Receiver<DbWorkItem>,
        exit_worker: Arc<AtomicBool>,
    ) -> Result<(), AccountsDbPluginError> {
        while !exit_worker.load(Ordering::Relaxed) {
            let work = receiver.recv_timeout(Duration::from_millis(500));

            match work {
                Ok(work) => match work {
                    DbWorkItem::UpdateAccount(request) => {
                        self.client
                            .update_account(request.account, request.at_startup)?;
                    }
                    DbWorkItem::UpdateSlot(request) => {
                        self.client.update_slot_status(
                            request.slot,
                            request.parent,
                            request.slot_status,
                        )?;
                    }
                },
                Err(err) => match err {
                    RecvTimeoutError::Timeout => {
                        continue;
                    }
                    _ => {
                        error!("Error in receiving the item {:?}", err);
                        break;
                    }
                },
            }
        }
        Ok(())
    }
}
pub struct ParallelPostgresClient {
    workers: Vec<JoinHandle<Result<(), AccountsDbPluginError>>>,
    exit_worker: Arc<AtomicBool>,
    sender: Sender<DbWorkItem>,
    last_report: AtomicInterval,
}

impl ParallelPostgresClient {
    pub fn new(config: &AccountsDbPluginPostgresConfig) -> Result<Self, AccountsDbPluginError> {
        info!("Creating ParallelPostgresClient...");
        let (sender, receiver) = bounded(MAX_ASYNC_REQUESTS);
        let exit_worker = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::default();

        for i in 0..config.threads.unwrap() {
            let cloned_receiver = receiver.clone();
            let exit_clone = exit_worker.clone();
            let config = config.clone();
            let worker = Builder::new()
                .name(format!("worker-{}", i))
                .spawn(move || -> Result<(), AccountsDbPluginError> {
                    let mut worker = PostgresClientWorker::new(config)?;
                    worker.do_work(cloned_receiver, exit_clone)?;
                    Ok(())
                })
                .unwrap();

            workers.push(worker);
        }

        info!("Created ParallelPostgresClient.");
        Ok(Self {
            last_report: AtomicInterval::default(),
            workers,
            exit_worker,
            sender,
        })
    }

    pub fn join(&mut self) -> thread::Result<()> {
        self.exit_worker.store(true, Ordering::Relaxed);
        while !self.workers.is_empty() {
            let worker = self.workers.pop();
            if worker.is_none() {
                break;
            }
            let worker = worker.unwrap();
            let result = worker.join().unwrap();
            if result.is_err() {
                error!("The worker thread has failed: {:?}", result);
            }
        }

        Ok(())
    }

    pub fn update_account(
        &mut self,
        account: &ReplicaAccountInfo,
        slot: u64,
        at_startup: bool,
    ) -> Result<(), AccountsDbPluginError> {
        if self.last_report.should_update(30000) {
            datapoint_info!(
                "postgres-plugin-stats",
                ("message-queue-length", self.sender.len() as i64, i64),
            );
        }
        if let Err(err) = self
            .sender
            .send(DbWorkItem::UpdateAccount(UpdateAccountRequest {
                account: DbAccountInfo::new(account, slot),
                at_startup,
            }))
        {
            return Err(AccountsDbPluginError::AccountsUpdateError {
                msg: format!(
                    "Failed to update the account {:?}, error: {:?}",
                    bs58::encode(account.pubkey()).into_string(),
                    err
                ),
            });
        }
        Ok(())
    }

    pub fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> Result<(), AccountsDbPluginError> {
        if let Err(err) = self.sender.send(DbWorkItem::UpdateSlot(UpdateSlotRequest {
            slot,
            parent,
            slot_status: status,
        })) {
            return Err(AccountsDbPluginError::SlotStatusUpdateError {
                msg: format!("Failed to update the slot {:?}, error: {:?}", slot, err),
            });
        }
        Ok(())
    }
}

pub struct PostgresClientBuilder {}

impl PostgresClientBuilder {
    pub fn build_pararallel_postgres_client(
        config: &AccountsDbPluginPostgresConfig,
    ) -> Result<ParallelPostgresClient, AccountsDbPluginError> {
        ParallelPostgresClient::new(config)
    }

    pub fn build_simple_postgres_client(
        config: &AccountsDbPluginPostgresConfig,
    ) -> Result<SimplePostgresClient, AccountsDbPluginError> {
        SimplePostgresClient::new(config)
    }
}
