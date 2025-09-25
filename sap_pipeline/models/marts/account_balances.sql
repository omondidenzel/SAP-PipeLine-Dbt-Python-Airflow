select account_id
    , sum(case when transaction_type = 'credit' then amount else 0 end) as total_credits
    , sum(case when transaction_type = 'debit' then amount else 0 end) as total_debits
    , sum(case when transaction_type = 'credit' then amount else -amount end) as balance
    , case 
        when sum(case when transaction_type = 'credit' then amount else -amount end) < 0 then 'negative'
        when sum(case when transaction_type = 'credit' then amount else -amount end) = 0 then 'zero'
        else 'positive' 
    end as balance_status
from {{ ref('stg_financial_transactions') }}
group by 1
order by 1
