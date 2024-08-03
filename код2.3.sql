select * from rd.account_balance
where account_rk = 2000536--для наглядности изменений выбрала один из аккаунтов

--2.3A
	
WITH lagged_data AS (
    SELECT 
        account_rk,
        effective_date,
        account_in_sum,
        account_out_sum,
        LAG(account_out_sum) OVER (PARTITION BY account_rk ORDER BY effective_date) AS lag_account_out_sum
    FROM rd.account_balance
)
SELECT 
    account_rk,
    effective_date,
    CASE 
        WHEN account_in_sum <> lag_account_out_sum THEN lag_account_out_sum
        ELSE account_in_sum
    END AS correct_account_in_sum,
	account_out_sum
FROM lagged_data
WHERE account_rk = 2000536


--2.3B

WITH lagged_data AS (
    SELECT 
        account_rk,
        effective_date,
        account_in_sum,
        account_out_sum,
        LAG(account_out_sum) OVER (PARTITION BY account_rk ORDER BY effective_date) AS lag_account_out_sum,
        LEAD(account_in_sum) OVER (PARTITION BY account_rk ORDER BY effective_date) AS lead_account_in_sum
    FROM rd.account_balance
)
SELECT 
    account_rk,
    effective_date,
	account_in_sum,
    CASE 
        WHEN account_out_sum <> lag_account_out_sum THEN lead_account_in_sum
        ELSE account_out_sum
    END AS correct_account_out_sum
FROM lagged_data
WHERE account_rk = 2000536	
	

--2.3C

WITH lagged_data AS (
    SELECT 
        account_rk,
        effective_date,
        account_in_sum,
        account_out_sum,
        LAG(account_out_sum) OVER (PARTITION BY account_rk ORDER BY effective_date) AS lag_account_out_sum
    FROM rd.account_balance
)
UPDATE rd.account_balance AS ab
SET account_in_sum = corrected_data.correct_account_in_sum
FROM (
    SELECT 
        account_rk,
        effective_date,
        CASE 
            WHEN account_in_sum <> lag_account_out_sum THEN lag_account_out_sum
            ELSE account_in_sum
        END AS correct_account_in_sum
    FROM lagged_data
) AS corrected_data
WHERE ab.account_rk = corrected_data.account_rk
AND ab.effective_date = corrected_data.effective_date;

--проверяем
select * from rd.account_balance
where account_rk = 2000536


--2.3D

select * from dm.account_balance_turnover
where account_rk = 2000536
 
CREATE OR REPLACE PROCEDURE reload_account_balance_turnover()
LANGUAGE plpgsql
AS $$
BEGIN
    --Удаление существующих данных из витрины account_balance_turnover
    DELETE FROM dm.account_balance_turnover;

    -- Вставка новых данных в витрину account_balance_turnover
INSERT INTO dm.account_balance_turnover (account_rk, currency_name, department_rk, effective_date, account_in_sum, account_out_sum)
SELECT a.account_rk, COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name, a.department_rk, ab.effective_date, ab.account_in_sum, ab.account_out_sum
FROM rd.account a
LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
LEFT JOIN dm.dict_currency dc ON a.currency_cd::BIGINT = dc.currency_cd::BIGINT;
   -- Вывод сообщения об успешном обновлении данных
    RAISE NOTICE 'Данные успешно перезагружены в витрину account_balance_turnover';
END;
$$;


CALL reload_account_balance_turnover();


select * from dm.account_balance_turnover
where account_rk = 2000536