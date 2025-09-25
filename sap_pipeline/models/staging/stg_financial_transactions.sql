select *
from {{
  source('sap_source','financial_transactions')
}}