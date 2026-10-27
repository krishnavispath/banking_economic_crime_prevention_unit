from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
  
spark = SparkSession.builder \
        .appName('CrimeFlowPipeline') \
        .config('spark.eventLog.enabled','false') \
        .getOrCreate()
print('Spark Session Created Successfully') 

#CONFIG
PROJECT_ID = 'project-dcd27d37-1227-41c6-839'
DATASET = 'BankingCrimeFlow'
BUCKET = 'banking_economic_crime_prevention_unit'

#GCS FILE PATHS
txn_path = ('gs://banking_economic_crime_prevention_unit/raw_files/Datafiles_transactions_raw.csv')
cust_kyc_path = ('gs://banking_economic_crime_prevention_unit/raw_files/Datafiles_customers_kyc.json')
branch_path = ('gs://banking_economic_crime_prevention_unit/raw_files/Datafiles_branch_lookup.csv')

#READ TRANSACTION FILE
print('Reading Transaction File..')
txn_df = spark.read \
        .option('header','true') \
        .option('inferschema','true') \
        .csv(txn_path)

print('TRANSACTION FILE LOADED')
txn_df.show(5)
txn_df.printSchema()

# READ CUSTOMER FILE
print('Reading Customer File..')
customer_df = spark.read \
            .option('multiline','true') \
            .json(cust_kyc_path)

print('CUSTOMER FILE LOADED')
customer_df.show(5)
customer_df.printSchema()

# READ LOOKUP_BRANCH FILE
print('Reading Branch File..')
branch_df = spark.read \
            .option('header','true') \
            .option('inferschema','true') \
            .csv(branch_path)
print('BRANCH FILE LOADED')
branch_df.show(5)
branch_df.printSchema()

#VALID RECORDS
valid_df = txn_df.filter(F.col('customer_id').isNotNull())
valid_df.show(5)

#INVALID RECORDS
invalid_df = txn_df.filter(F.col('customer_id').isNull()) \
                .withColumn('failure_reason',F.lit('NULL_CUSTOMER_ID'))
invalid_df.show(5)

#WRITE INVALID RECORDS TO QUARANTINE FOLDER
invalid_df.write.mode('overwrite').option('header','true').csv(f"gs://{BUCKET}/quarantine/invalid_records/")
print('INVALID RECORDS WRITTEN')

#CLEAN AMOUNT Transaction amount — create amount_gbp
clean_df = valid_df.withColumn(
    'amount',
    F.regexp_replace(F.col('amount').cast('string'), r'[£,]', '')
)
clean_df.show(5)
print('INVALID SYMBOLS FROM AMOUNT COLUMN ARE REMOVED')
#clean_df.printSchema()

clean_df = clean_df.withColumn(
    'amount',
    F.col('amount').cast('double')
)
clean_df.printSchema()

#clean_df = clean_df.withColumn('amount_gbp',abs(F.col('amount')))
clean_df.show(5)

clean_df = clean_df.withColumn(
    'is_refund',
    F.col('amount') < 0
)
clean_df.show(5)
clean_df.filter(F.col('is_refund')=='true').show()

clean_df = clean_df.withColumn(
    'amount_gbp',
    F.abs(F.col('amount'))
)

#CLEAN CURRENCY Currency normalization and quarantine
clean_df = clean_df.withColumn(
    'currency_normalized',
    F.upper(F.trim(F.col('currency_unit')))
)

valid_gbp_values = [
    'GBP',
    '£',
    'POUNDS STERLING'
]

invalid_currency_df = clean_df.filter(
    ~F.col('currency_normalized').isin(valid_gbp_values)
    | F.col('currency_normalized').isNull()
).withColumn(
    'failure_reason',
    F.lit('INVALID_CURRENCY')
)

invalid_currency_df.write \
    .mode('overwrite') \
    .option('header', 'true') \
    .csv(f'gs://{BUCKET}/quarantine/invalid_currency/')

clean_df = clean_df.filter(
    F.col('currency_normalized').isin(valid_gbp_values)
).withColumn(
    'currency_unit',
    F.lit('GBP')
).drop('currency_normalized')
clean_df.show(10)

#CLEAN CHANNEL Channel normalization
valid_channels = ['ONLINE', 'BRANCH', 'MOBILE', 'ATM']

clean_df = clean_df.withColumn(
    'channel',
    F.upper(F.trim(F.col('channel')))
)

clean_df = clean_df.withColumn(
    'channel',
    F.when(
        F.col('channel').isin(valid_channels),
        F.col('channel')
    ).otherwise('UNKNOWN')
)
clean_df.show(10)

#CLEAN DATE
clean_df=clean_df.withColumn('transaction_date',
                    F.coalesce(
                        F.to_date(F.col('transaction_date'),'dd/MM/yyyy'),
                        F.to_date(F.col('transaction_date'),'yyyy-MM-dd')
                    ))
clean_df.show(10)
print('DATE CLEANSING COMPLETED')

#DEDUPLICATION  OF transaction_id(REMOVE DUPLICATES)
window_spec = Window.partitionBy('transaction_id').orderBy(F.col('transaction_date'))
clean_df = clean_df.withColumn('row_num',F.row_number().over(window_spec))

clean_df = clean_df.filter(F.col('row_num') == 1).drop('row_num')
print('Duplicates are removed')

#Branch-code audit
branch_audit_df = clean_df.filter(
    (F.col('channel') == 'BRANCH') &
    (
        F.col('branch_code').isNull() |
        (F.trim(F.col('branch_code')) == '')
    )
).withColumn(
    'failure_reason',
    F.lit('MISSING_BRANCH_CODE')
)

branch_audit_df.write \
    .mode('overwrite') \
    .option('header', 'true') \
    .csv(f'gs://{BUCKET}/audit/missing_branch_code/')

#CUSTOMER CLEANING
#pep_flag AND risk_rating CLEANSING
customer_df=customer_df.withColumn('pep_flag',F.coalesce(F.col('pep_flag'),F.lit(False)))
customer_df=customer_df.withColumn('risk_rating',F.initcap(F.col('risk_rating')))
customer_df.show(10)

#Customer deduplication using latest onboarded_date
customer_df = customer_df.withColumn(
    'onboarded_date',
    F.coalesce(
        F.to_date('onboarded_date', 'dd/MM/yyyy'),
        F.to_date('onboarded_date', 'yyyy-MM-dd')
    )
)

customer_window = Window.partitionBy(
    'customer_id'
).orderBy(
    F.col('onboarded_date').desc_nulls_last()
)

customer_ranked_df = customer_df.withColumn(
    'row_num',
    F.row_number().over(customer_window)
)

duplicate_customers_df = customer_ranked_df.filter(
    F.col('row_num') > 1
).drop('row_num').withColumn(
    'failure_reason',
    F.lit('DUPLICATE_CUSTOMER_ID')
)

duplicate_customers_df.write \
    .mode('overwrite') \
    .option('header', 'true') \
    .csv(f'gs://{BUCKET}/quarantine/duplicate_customers/')

customer_df = customer_ranked_df.filter(
    F.col('row_num') == 1
).drop('row_num')

#Customer date_of_birth standardization
customer_df = customer_df.withColumn(
    'date_of_birth',
    F.coalesce(
        F.to_date('date_of_birth', 'dd/MM/yyyy'),
        F.to_date('date_of_birth', 'yyyy-MM-dd')
    )
)

#Risk rating normalization and auditing
customer_df = customer_df.withColumn(
    'risk_rating_original',
    F.col('risk_rating')
)

customer_df = customer_df.withColumn(
    'risk_rating',
    F.initcap(F.trim(F.col('risk_rating')))
)

risk_audit_df = customer_df.filter(
    F.col('risk_rating').isNull() |
    ~F.col('risk_rating').isin('High', 'Medium', 'Low')
).withColumn(
    'failure_reason',
    F.lit('INVALID_RISK_RATING_DEFAULTED')
)

risk_audit_df.write \
    .mode('overwrite') \
    .option('header', 'true') \
    .csv(f'gs://{BUCKET}/audit/invalid_risk_rating/')

customer_df = customer_df.withColumn(
    'risk_rating',
    F.when(
        F.col('risk_rating').isin('High', 'Medium', 'Low'),
        F.col('risk_rating')
    ).otherwise('Medium')
)

#Country-code validation
# Example: ISO country-code reference CSV in GCS
country_ref_df = spark.read \
    .option('header', 'true') \
    .csv(f'gs://{BUCKET}/reference/iso_country_codes.csv')

country_ref_df = country_ref_df.select(
    F.upper(F.trim(F.col('country_code'))).alias('valid_country_code')
).distinct()

customer_df = customer_df.withColumn(
    'country_code',
    F.upper(F.trim(F.col('country_code')))
)

customer_df = customer_df.join(
    F.broadcast(country_ref_df),
    customer_df.country_code == country_ref_df.valid_country_code,
    'left'
)

customer_df = customer_df.withColumn(
    'country_code',
    F.when(
        F.col('valid_country_code').isNotNull(),
        F.col('country_code')
    ).otherwise('UNKNOWN')
).drop('valid_country_code')

#PEP ENRICHMENT
#Enrichment A — PEP Flag Propagation
final_df = clean_df.join(
customer_df.select('customer_id','full_name','risk_rating','pep_flag'),on='customer_id',how='left')

final_df=final_df.withColumn('is_pep_transaction',F.when(F.col('pep_flag') == True, True).otherwise(False))
final_df.show(10)
print('PEP Flag Propagation completed')

#Enrichment B — Transaction Velocity Risk
#WINDOW FUNCTION
cust_window = Window.partitionBy('customer_id')
final_df = final_df.withColumn('txn_count',F.count('transaction_id').over(cust_window))

final_df = final_df.withColumn('total_amount',F.sum('amount').over(cust_window))
final_df.show(10)
print('total amount calculated')

#VELOCITY RISK BAND
final_df = final_df.withColumn('velocity_risk_band',
                    F.when(
                            (F.col('txn_count') > 10) |
                            (F.col('total_amount') > 50000), 'HIGH')
                            .otherwise('LOW')
                   )
final_df.show(10)
print('Transaction Velocity Risk COMPLETED')

#JOIN BRANCH LOOKUP
final_df = final_df.join(branch_df,on='branch_code',how='left')
final_df.show(10)
print('BRANCH JOIN COMPLETED')

#WRITE CLEANSED PARQUET
final_df.write.mode('overwrite') \
        .parquet(f"gs://{BUCKET}/cleansed/final_transactions/")
print('PARQUET WRITTEN')

#PARQUET FILE DATA VALIDATION
print('Reading Final Parquet File..')
parquet_df = spark.read \
        .parquet(f"gs://{BUCKET}/cleansed/final_transactions/")
parquet_df.show()
record_count = parquet_df.count()
print(f"Total records: {record_count}")

count_df=spark.createDataFrame(
        [(record_count,)],
        ['record_count'])
count_df.write \
    .mode('overwrite') \
    .option('header','true') \
    .csv(f"gs://{BUCKET}/audit/")
print('record count written')
cnt_df = spark.read \
        .option('header','true') \
        .option('inferschema','true') \
        .csv(f"gs://{BUCKET}/audit/")
cnt_df.show()

#FACT TRANSACTIONS TABLE
bqpath = f"{PROJECT_ID}:{DATASET}.fact_transactions"
gcs_parquetpath = f"gs://{BUCKET}/cleansed/final_transactions/*.parquet"
print(bqpath)
!bq load \
    --replace \
      --source_format=PARQUET \
      {bqpath} \
    {gcs_parquetpath}
print("FACT TABLE LOADED")

#DIM CUSTOMER TABLE
TEMP_BUCKET = BUCKET   # Bucket name only, no gs://

dim_customer_df = customer_df.select(
                'customer_id',
                'full_name',
                'risk_rating',
                'pep_flag',
                'country_code',
                'kyc_status',
                'onboarded_date').dropDuplicates()
dim_customer_df.write.format('bigquery') \
                .option("temporaryGcsBucket", TEMP_BUCKET) \
                .option('table',f"{PROJECT_ID}:{DATASET}.dim_customer") \
                .mode('overwrite').save()
print('DIM CUSTOMER LOADED')

#HIGH RISK MART
mart_df = final_df.filter(
    (F.col('is_pep_transaction') == True) |
    (F.col('velocity_risk_band') == 'HIGH')
)

mart_df.write.format('bigquery') \
                .option("temporaryGcsBucket", TEMP_BUCKET) \
                .option('table',f"{PROJECT_ID}:{DATASET}.mart_high_risk_activity") \
                .mode('overwrite').save()
print('MART TABLE LOADED')

#QUALITY LOG TABLE
quality_log_df = spark.createDataFrame(
    [
        (
            "SUCCESS",
            valid_df.count(),
            invalid_df.count()
        )
    ],
    [
        'pipeline_status',
        'valid_records',
        'invalid_records'
    ]
)

quality_log_df.write.format('bigquery') \
                .option("temporaryGcsBucket", TEMP_BUCKET) \
                .option('table',f"{PROJECT_ID}:{DATASET}.pipeline_quality_log") \
                .mode('overwrite').save()
print('QUALITY LOG TABLE LOADED')
print('PIPELINE COMPLETED SUCCESSFULLY')