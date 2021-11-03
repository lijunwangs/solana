/**
 * This plugin implementation for PostgreSQL requires the following tables
 */
-- The table storing accounts


CREATE TABLE account (
    pubkey BYTEA PRIMARY KEY,
    owner BYTEA,
    lamports BIGINT NOT NULL,
    slot BIGINT NOT NULL,
    executable BOOL NOT NULL,
    rent_epoch BIGINT NOT NULL,
    data BYTEA,
    write_version BIGINT NOT NULL,
    updated_on TIMESTAMP NOT NULL
);

-- The table storing slot information
CREATE TABLE slot (
    slot BIGINT PRIMARY KEY,
    parent BIGINT,
    status VARCHAR(16) NOT NULL,
    updated_on TIMESTAMP NOT NULL
);


CREATE TYPE CompiledInstruction AS (
    program_id_index SMALLINT,
    accounts SMALLINT[],
    data BYTEA
);

CREATE TYPE TransactionStatusMeta AS (
    status VARCHAR(256),
    fee BIGINT,
    pre_balances BIGINT[],
    post_balances BIGINT[],


);

CREATE TYPE InnerInstructions AS (
    index SMALLINT,
    instructions CompiledInstruction[]
);

-- The table storing transaction logs
CREATE TABLE transaction_log (
    signature BYTEA PRIMARY KEY,
    is_vote BOOL NOT NULL,
    result VARCHAR(256),
    slot BIGINT NOT NULL,
    logs TEXT[],
    num_required_signatures SMALLINT,
    num_readonly_signed_accounts SMALLINT,
    num_readonly_unsigned_accounts SMALLINT,
    account_keys BYTEA[],
    recent_blockhash BYTEA,
    instructions CompiledInstruction[],
    message_hash BYTEA,
    signatures BYTEA[],
    updated_on TIMESTAMP NOT NULL
);

/**
 * The following is for keeping historical data for accounts and is not required for plugin to work.
 */
-- The table storing historical data for accounts
CREATE TABLE account_audit (
    pubkey BYTEA,
    owner BYTEA,
    lamports BIGINT NOT NULL,
    slot BIGINT NOT NULL,
    executable BOOL NOT NULL,
    rent_epoch BIGINT NOT NULL,
    data BYTEA,
    write_version BIGINT NOT NULL,
    updated_on TIMESTAMP NOT NULL
);

CREATE FUNCTION audit_account_update() RETURNS trigger AS $audit_account_update$
    BEGIN
		INSERT INTO account_audit (pubkey, owner, lamports, slot, executable, rent_epoch, data, write_version, updated_on)
            VALUES (OLD.pubkey, OLD.owner, OLD.lamports, OLD.slot,
                    OLD.executable, OLD.rent_epoch, OLD.data, OLD.write_version, OLD.updated_on);
        RETURN NEW;
    END;

$audit_account_update$ LANGUAGE plpgsql;

CREATE TRIGGER account_update_trigger AFTER UPDATE OR DELETE ON account
    FOR EACH ROW EXECUTE PROCEDURE audit_account_update();
