# scripts/init_immudb_audit_schema.py

from immudb import ImmudbClient

# -----------------------------
# CONFIG
# -----------------------------
IMMUDB_URL = "localhost:3322"
IMMUDB_USER = "immudb"
IMMUDB_PASSWORD = "immudb"

# login first to defaultdb as admin, create audit db there
DEFAULT_DB = b"defaultdb"
AUDIT_DB_NAME = "credit_audit_db"
AUDIT_DB = AUDIT_DB_NAME.encode()


def safe_create_database(client: ImmudbClient, db_name: str) -> None:
    """
    Creates the database if it does not exist.
    If it already exists, continue silently.
    """
    try:
        client.createDatabase(db_name)
        print(f"Database created: {db_name}")
    except Exception as e:
        msg = str(e).lower()
        if "already" in msg or "exist" in msg:
            print(f"Database already exists: {db_name}")
        else:
            raise


def exec_ddl(client, sql: str):
    print("\n--- Executing DDL ---")
    print(sql)
    print("--- End DDL ---\n")
    client.sqlExec(sql)


def main():
    # ---------------------------------------
    # STEP 1: connect to defaultdb and create audit DB
    # ---------------------------------------
    admin_client = ImmudbClient(IMMUDB_URL)
    admin_client.login(IMMUDB_USER, IMMUDB_PASSWORD, database=DEFAULT_DB)

    safe_create_database(admin_client, AUDIT_DB_NAME)

    # ---------------------------------------
    # STEP 2: reconnect directly to audit DB
    # ---------------------------------------
    client = ImmudbClient(IMMUDB_URL)
    client.login(IMMUDB_USER, IMMUDB_PASSWORD, database=AUDIT_DB)

    # ---------------------------------------
    # TABLE 1: customer_info
    # raw/identity/basic metadata from ingestion layer
    # ---------------------------------------
    exec_ddl(
        client,
        """
        CREATE TABLE IF NOT EXISTS customer_info (
    application_id VARCHAR[64],
    audit_id VARCHAR[64] NOT NULL,
    customer_id VARCHAR[64],
    full_name VARCHAR[200],
    phone VARCHAR[32],
    email VARCHAR[150],
    dob VARCHAR[32],
    pan_number VARCHAR[32],
    aadhaar_last4 VARCHAR[8],
    address VARCHAR[500],
    city VARCHAR[100],
    state VARCHAR[100],
    pincode VARCHAR[20],
    loan_amount FLOAT,
    loan_tenure_months INTEGER,
    source_channel VARCHAR[50],
    submission_mode VARCHAR[50],
    ingestion_timestamp TIMESTAMP,
    created_at TIMESTAMP,
    PRIMARY KEY application_id
);
        """
    )

    # ---------------------------------------
    # TABLE 2: extracted_processed_data
    # output of orchestrator / extractor + light processed fields
    # ---------------------------------------
    exec_ddl(
        client,
        """
        CREATE TABLE IF NOT EXISTS extracted_processed_data (
            application_id VARCHAR[64],
            audit_id VARCHAR[64] NOT NULL,
            customer_id VARCHAR[64],
            amt_income_total FLOAT,
            no_of_dependents INTEGER,
            employment_type VARCHAR[100],
            occupation_type VARCHAR[100],
            residence_type VARCHAR[100],
            monthly_utility_bill_avg FLOAT,
            monthly_bank_credit_avg FLOAT,
            monthly_bank_debit_avg FLOAT,
            emi_obligation_estimate FLOAT,
            bank_account_vintage_months INTEGER,
            utility_payment_regularity_score FLOAT,
            extraction_status VARCHAR[50],
            extractor_version VARCHAR[50],
            preprocessing_version VARCHAR[50],
            processed_timestamp TIMESTAMP,
            created_at TIMESTAMP,
            PRIMARY KEY(application_id)
        );
        """
    )

    # ---------------------------------------
    # TABLE 3: model_governance
    # model metadata + monitoring / governance flags
    # ---------------------------------------
    exec_ddl(
        client,
        """
        CREATE TABLE IF NOT EXISTS model_governance (
            application_id VARCHAR[64],
            audit_id VARCHAR[64] NOT NULL,
            customer_id VARCHAR[64],
            model_name VARCHAR[100],
            model_version VARCHAR[50],
            model_type VARCHAR[100],
            feature_schema_version VARCHAR[50],
            training_date TIMESTAMP,
            deployed_date TIMESTAMP,
            validation_roc_auc FLOAT,
            validation_accuracy FLOAT,
            model_health_status VARCHAR[30],
            drift_status VARCHAR[30],
            prediction_drift_status VARCHAR[30],
            feature_drift_status VARCHAR[30],
            fairness_status VARCHAR[30],
            protected_attributes_removed BOOLEAN,
            manual_override_flag BOOLEAN,
            governance_timestamp TIMESTAMP,
            created_at TIMESTAMP,
            PRIMARY KEY(application_id)
        );
        """
    )

    # ---------------------------------------
    # TABLE 4: decision_engine_output
    # model prediction + SHAP + decision recommendation
    # ---------------------------------------
    exec_ddl(
        client,
        """
        CREATE TABLE IF NOT EXISTS decision_engine_output (
            application_id VARCHAR[64],
            audit_id VARCHAR[64] NOT NULL,
            customer_id VARCHAR[64],
            model_version VARCHAR[50],
            probability_default FLOAT,
            confidence_score FLOAT,
            risk_band VARCHAR[50],
            recommendation VARCHAR[50],
            shap_top_factors VARCHAR[2000],
            shap_summary_json VARCHAR[8000],
            bias_check_status VARCHAR[30],
            decision_engine_timestamp TIMESTAMP,
            created_at TIMESTAMP,
            PRIMARY KEY(application_id)
        );
        """
    )

    # ---------------------------------------
    # TABLE 5: final_customer_status
    # final output after manager / decision layer
    # ---------------------------------------
    exec_ddl(
        client,
        """
        CREATE TABLE IF NOT EXISTS final_customer_status (
            application_id VARCHAR[64],
            audit_id VARCHAR[64] NOT NULL,
            customer_id VARCHAR[64],
            final_status VARCHAR[50],
            manager_decision VARCHAR[50],
            approved_loan_amount FLOAT,
            interest_rate FLOAT,
            loan_tenure_months INTEGER,
            emi FLOAT,
            collateral_required BOOLEAN,
            collateral_details VARCHAR[500],
            rejection_reason VARCHAR[1000],
            manual_review_required BOOLEAN,
            final_decision_timestamp TIMESTAMP,
            created_at TIMESTAMP,
            PRIMARY KEY(application_id)
        );
        """
    )

    print("immudb audit schema initialization completed successfully.")


if __name__ == "__main__":
    main()