/**************************************************************************************************
  Project Name   : SupaStore Data (Quality Checks)
  Script Name    : DQ_SupaStore_Checks.sql
  Description    :
      This script performs data quality validation for consistency, completeness, and standardization
      within the 'supastore_db' schema. It identifies potential issues after data loading (silver layer)
      to ensure the dataset is clean and reliable for analytics.

      Quality checks include:
          1. Record completeness for key fields
          2. Null and duplicate detection in key identifiers
          3. Unwanted whitespace in text columns
          4. Cross-field validation (order vs. ship dates)
          5. Business rule enforcement (quantity >= 1)

  Author         : Oladigbolu Taofeek
  Version        : 1.1
  Last Updated   : 2025-10-29
  Target DBMS    : MySQL 8.0+
  Execution Mode : Post-ETL Validation / Silver Layer
  Expected Output:
        - Zero records returned for anomaly checks
        - Summaries showing 0 missing, 0 duplicates, 0 invalid records

  Revision History:
      Version | Date        | Author   | Description
      --------|-------------|----------|-----------------------------------------------
      1.0     | 2025-10-28  | Taofeek  | Initial version created
      1.1     | 2025-10-29  | Taofeek  | Standardized formatting & improved logic
**************************************************************************************************/

/*==============================================================================================*/
/* - COMPLETENESS CHECK: Incomplete Rows in Key Columns                                */
/*==============================================================================================*/
-- Expectation: (total rows - non-null rows) = 0 for all key fields
select 
count(*) as total_row,
count(*) - count(order_id) no_order_id_row,
count(*) - count(customer_id) no_customer_id_row,
count(*) - count(product_id) no_product_id_row
from supastore_db;

/*==============================================================================================*/
/* - NULL VALUE CHECKS                                                                 */
/*==============================================================================================*/
-- Expectation: 0 rows returned
select 
order_id,
product_id,
customer_id
from supastore_db
where order_id is null or
product_id is null or 
customer_id is null;

/*==============================================================================================*/
/* - DUPLICATE RECORD CHECKS (on Key Identifiers)                                      */
/*==============================================================================================*/
-- Expectation: 0 rows returned for all keys
select 
count(order_id), 
count(customer_id),
count(product_id)
from supastore_db
where count(order_id) > 1 or 
count(customer_id) > 1 or 
count(product_id)  > 1
group by order_id;

/*==============================================================================================*/
/* - UNWANTED WHITESPACE CHECKS (String Fields)                                        */
/*==============================================================================================*/
-- Expectation: All differences = 0
select 
length(ship_mode) - length(trim(ship_mode)),
length(customer_name) - length(trim(customer_name)),
length(segment) - length(trim(segment)),
length(country) - length(trim(country)),
length(city) - length(trim(city)),
length(state) - length(trim(state)),
length(region) - length(trim(region)),
length(category) - length(trim(category))
from supastore_db;

/*==============================================================================================*/
/* - CROSS-FIELD VALIDATION (Order Date vs. Ship Date)                                 */
/*==============================================================================================*/
-- Expectation: No records where ship_date < order_date
    select *
    from supastore_db
    where ship_date < order_date; 
	
  /*==============================================================================================*/
/* - BUSINESS RULE VALIDATION (Quantity >= 1)                                          */
/*==============================================================================================*/
-- Expectation: 0 rows returned
  select *
    from supastore_db 
    where quantity < 1;
    
/*==============================================================================================*/
/* -  SUMMARY REPORT                                                         */
/*==============================================================================================*/
-- This summary aggregates anomaly counts across key checks for quick reporting.
select 
    (count(*) - count(order_id))    as missing_order_id,
    (count(*) - count(customer_id)) as missing_customer_id,
    (count(*) - count(product_id))  as missing_product_id,
    sum(case when ship_date < order_date then 1 else 0 end) as invalid_ship_dates,
    sum(case when quantity < 1 then 1 else 0 end) as invalid_quantities
from supastore_db;
