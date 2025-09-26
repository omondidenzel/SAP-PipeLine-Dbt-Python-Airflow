select *
from {{ref('stg_financial_transactions')}}
where amount < 0