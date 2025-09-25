select transaction_id as id
       , account_id::text as account_id
       , transaction_date
       , description
       , transaction_type
       , amount
       , currency
       , created_at::date as created_at
from {{
  source('sap_source','financial_transactions')
}}
