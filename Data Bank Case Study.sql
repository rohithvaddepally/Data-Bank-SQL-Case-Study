#**************Data Bank Case Study***************#

#A. Customer Nodes Exploration

# 1.How many unique nodes are there on the Data Bank system?

SELECT COUNT(distinct node_id) AS number_of_unique_nodes
FROM customer_nodes;

#There are fives unique number of nodes

SELECT DISTINCT node_id AS distinct_nodes
FROM customer_nodes
ORDER BY distinct_nodes;

# node_id's are 1,2,3,4,5


# 2. What is the number of nodes per region?


SELECT r.region_name,
	   COUNT(node_id) AS node_cnt
FROM customer_nodes cn
LEFT JOIN regions r
	 ON cn.region_id = r.region_id
GROUP BY r.region_name;

# For Africa-714, Europe-616, Australia-770, America-735, Asia-665



# 3.How many customers are allocated to each region?

SELECT r.region_name,
       COUNT(Distinct customer_id) AS customer_cnt
FROM customer_nodes cn
LEFT JOIN regions r
	 ON cn.region_id = r.region_id
GROUP BY r.region_name;

# Africa - 102, America-105, Asia-95, Australia-110, Europe-88



# 4. How many days on average are customers reallocated to a different node?

#for each customer
WITH corrected_cust_nodes AS (
SELECT customer_id,
       region_id,
       node_id,
       start_date,
      (CASE
          WHEN end_date = '9999-12-31' THEN '2020-12-31'
          ELSE end_date
	  END) AS modified_end_date
FROM customer_nodes
),

diff_in_days AS (
SELECT *,
       datediff(modified_end_date,start_date) AS diff_in_days
FROM corrected_cust_nodes
)

SELECT customer_id,
       ROUND(AVG(diff_in_days)) AS avg_rellocation_days
FROM diff_in_days
GROUP BY customer_id;



#overall average
WITH corrected_cust_nodes AS (
SELECT customer_id,
       region_id,
       node_id,
       start_date,
      (CASE
          WHEN end_date = '9999-12-31' THEN '2020-12-31'
          ELSE end_date
	  END) AS modified_end_date
FROM customer_nodes
),

diff_in_days AS (
SELECT *,
       datediff(modified_end_date,start_date) AS diff_in_days
FROM corrected_cust_nodes
)

SELECT 
       ROUND(AVG(diff_in_days)) AS avg_rellocation_days
FROM diff_in_days;

#It takes averagely 49 days to reallocate the customers to a different nodes.




# 5.What is the median, 80th and 95th percentile for this same reallocation days metric for each region?

WITH corrected_cust_nodes AS (
SELECT customer_id,
       region_id,
       node_id,
       start_date,
      (CASE
          WHEN end_date = '9999-12-31' THEN '2020-12-31'
          ELSE end_date
	  END) AS modified_end_date
FROM customer_nodes
),

diff_in_days AS (
SELECT *,
       (datediff(modified_end_date,start_date)) AS diff_in_days
FROM corrected_cust_nodes
),

ranked_regions AS(
SELECT region_id,
       diff_in_days,
       ROW_NUMBER() OVER (PARTITION BY region_id
                          ORDER BY diff_in_days
                          ) AS rn,
	   COUNT(*) OVER(PARTITION BY region_id) AS total_rows 
FROM diff_in_days
)

/*SELECT *
FROM ranked_regions*/

SELECT region_id, 
       MAX(CASE WHEN rn=CEIL(total_rows*0.50) THEN diff_in_days END) AS 50_percentile,
       MAX(CASE WHEN rn=CEIL(total_rows*0.80) THEN diff_in_days END) AS 80_percentile,
       MAX(CASE WHEN rn=CEIL(total_rows*0.95) THEN diff_in_days END) AS 95_percentile
FROM ranked_regions
GROUP BY region_id;




# B. Customer Transactions

# 1.What is the unique count and total amount for each transaction type?

SELECT txn_type, 
       COUNT(*) AS unique_txn_count,
       CONCAT('$',SUM(txn_amount)) AS txn_amount
FROM customer_transactions
GROUP BY txn_type;




# 2. What is the average total historical deposit counts and amounts for all customers?

WITH deposit_stats AS (
SELECT customer_id,
       COUNT(*) AS deposit_counts,
       SUM(txn_amount) AS deposit_amount 
FROM customer_transactions
WHERE txn_type = "deposit"
GROUP BY customer_id
)

SELECT FLOOR(AVG(deposit_counts)) AS avg_number_of_deposits,
       CONCAT("$", CEIL(AVG(deposit_amount))) AS avg_deposited_amount
FROM deposit_stats;

# average number of deposits made by each customer is 5, and average amount deposited is $2719 by all the customers



# 3. For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month

WITH txn_type AS (
SELECT DATE_FORMAT(txn_date,'%Y-%m') AS Month, 
        customer_id,
       (CASE WHEN txn_type = "deposit" THEN 1 ELSE 0 END) AS deposit_type,
       (CASE WHEN txn_type = "purchase" THEN 1 ELSE 0 END) AS purchase_type,
       (CASE WHEN txn_type = "withdrawal" THEN 1 ELSE 0 END) AS withdrawal_type
FROM customer_transactions
),

summing_txn AS (
SELECT Month,
	   (customer_id),
       SUM(deposit_type) as deposit_type,
       SUM(purchase_type) as purchase_type,
       SUM(withdrawal_type) as withdrawal_type
FROM txn_type
GROUP BY Month, customer_id
HAVING deposit_type > 1 AND (purchase_type >=1 OR withdrawal_type>=1)
)

SELECT Month,
       COUNT(customer_id) AS customer_cnt
FROM summing_txn
GROUP BY Month
ORDER BY Month;


# customers cnt with more than 1 deposit txn and atleast 1 purchase or 1 withdrawal are jan-168, feb-181, Mar-192, Apr-70



# 4. What is the closing balance for each customer at the end of the month?

WITH balances AS (
SELECT customer_id,
       txn_date,
       DATE_FORMAT(txn_date,'%Y-%m') AS month,
       SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE - txn_amount END) 
		   OVER(PARTITION BY customer_id 
		   ORDER BY txn_date) AS running_balance
FROM customer_transactions
),

ranking_month_end AS (
SELECT customer_id,
       txn_date,
       month,
       running_balance, 
       ROW_NUMBER() OVER (PARTITION BY customer_id, month
                          ORDER BY txn_date DESC) AS rn
FROM balances
)

SELECT customer_id,
       month,
       running_balance AS closing_balance
FROM ranking_month_end
where rn=1
ORDER BY customer_id, month;



# 5. What is the percentage of customers who increase their closing balance by more than 5%?


WITH balances AS (
SELECT customer_id,
       txn_date,
       DATE_FORMAT(txn_date,'%Y-%m') AS month,
       SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE - txn_amount END) 
           OVER(PARTITION BY customer_id 
                ORDER BY txn_date) AS running_balance
FROM customer_transactions
),

ranking_month_end AS (
SELECT customer_id,
       txn_date,
       month,
       running_balance, 
       ROW_NUMBER() OVER (PARTITION BY customer_id, month
                          ORDER BY txn_date DESC) AS rn
FROM balances
),

closing_balance AS (
SELECT customer_id,
       month,
       running_balance AS closing_balance
FROM ranking_month_end
where rn=1
),

previous_month_bal AS (
SELECT *,
        LAG(closing_balance) OVER(PARTITION BY customer_id 
                   ORDER BY month) AS previous_month_closing_balance
FROM closing_balance
),

increase_by AS (
SELECT *,
       ROUND((closing_balance - previous_month_closing_balance)/previous_month_closing_balance, 2) * 100  AS pcnt_increase
FROM previous_month_bal
WHERE previous_month_closing_balance IS NOT NULL 
      AND closing_balance > previous_month_closing_balance
      AND ((closing_balance - previous_month_closing_balance)/previous_month_closing_balance) * 100 > 5
)

SELECT ROUND(COUNT(DISTINCT customer_id) * 100 / (SELECT COUNT(DISTINCT customer_id) 
									 FROM customer_transactions)) AS pcnt_of_customers_with_5pcnt_increase
FROM increase_by;


#37% of customers increased their closing balance by 5%.



# C. Data Allocation Challenge
/*To test out a few different hypotheses - the Data Bank team wants to run an experiment where different groups of customers would be allocated data using 3 different options:

Option 1: data is allocated based off the amount of money at the end of the previous month
Option 2: data is allocated on the average amount of money kept in the account in the previous 30 days
Option 3: data is updated real-time
For this multi-part challenge question - you have been requested to generate the following data elements to help the Data Bank team estimate how much data will need to be provisioned for each option:

running customer balance column that includes the impact each transaction
customer balance at the end of each month
minimum, average and maximum values of the running balance for each customer
Using all of the data available - how much data would have been required for each option on a monthly basis?
*/

WITH running_balances AS (
SELECT customer_id, 
	   txn_date, 
       MONTHNAME(txn_date) AS month,
       SUM(CASE WHEN txn_type = "deposit" THEN txn_amount ELSE -txn_amount END) 
           OVER(PARTITION BY customer_id
				ORDER BY txn_date) AS running_balance
FROM customer_transactions
),

ranking_closing_bal AS (
SELECT *
FROM (
    SELECT *,
            ROW_NUMBER() OVER (PARTITION BY customer_id, month
						   ORDER BY txn_date DESC) AS rn
    FROM running_balances
) rcb
WHERE rn = 1 
),

running_bal_stats AS (
SELECT customer_id,
       month,
       ROUND(AVG(running_balance)) AS avg_bal,
       MIN(running_balance) AS min_bal,
       MAX(running_balance) AS max_bal
FROM running_balances
GROUP BY  month, customer_id
),

option1 AS (
SELECT month,
       SUM(running_balance) AS data_required
FROM ranking_closing_bal
GROUP BY month
),

option2 AS (
SELECT month,
       SUM(avg_bal) AS data_required
FROM running_bal_stats
GROUP BY month
),

option3 AS (
SELECT month,
       SUM(max_bal) AS data_required
FROM running_bal_stats
GROUP BY month
)

SELECT *, 'Option 1-Monthly closing balance' AS option_type
FROM option1
UNION ALL
SELECT *, 'Option 2-average balance'
FROM option2
UNION ALL
SELECT *, 'Option 3-maximum balance'
FROM option3
ORDER BY option_type, FIELD(month, 'January','February', 'March', 'April');




# D. Extra Challenge 
/*Data Bank wants to try another option which is a bit more difficult to implement - they want to calculate data growth using an interest calculation, just like in a traditional savings account you might have with a bank.

If the annual interest rate is set at 6% and the Data Bank team wants to reward its customers by increasing their data allocation based off the interest calculated on a daily basis at the end of each day, 
how much data would be required for this option on a monthly basis?

Special notes:
Data Bank wants an initial calculation which does not allow for compounding interest, 
however they may also be interested in a daily compounding interest calculation so you can try to perform this calculation if you have the stamina!
*/


WITH running_balances AS (
SELECT customer_id, 
	   txn_date, 
       MONTHNAME(txn_date) AS month,
       SUM(CASE WHEN txn_type = "deposit" THEN txn_amount ELSE -txn_amount END) 
           OVER(PARTITION BY customer_id
				ORDER BY txn_date) AS balance
FROM customer_transactions
),

daily_simple_interest AS (
SELECT customer_id, 
       txn_date, 
       DATE_FORMAT(txn_date, '%Y-%m') AS month,
       balance,
       ROUND(balance * (0.06/365),2) AS daily_interest
FROM running_balances
),

next_transaction AS (
SELECT customer_id, 
	   txn_date, 
       balance,
       LEAD(txn_date) OVER(PARTITION BY customer_id
                           ORDER BY txn_date
                           ) AS next_txn_date
FROM running_balances
),

activity AS (
SELECT customer_id,
       txn_date, 
       balance, 
       DATEDIFF(COALESCE(next_txn_date, '2020-12-31'), txn_date) AS number_of_days_active_for
FROM next_transaction
),

compounding AS (
SELECT customer_id,
       txn_date, 
       DATE_FORMAT(txn_date, '%Y-%m') AS month,
       balance, 
       ROUND((balance * POW(1 + 0.06/365, number_of_days_active_for))- balance,2) AS compound_interest,
       ROUND(balance * POW(1 + 0.06/365, number_of_days_active_for),2) AS balance_with_ci
FROM activity
)

SELECT c.month,
       ROUND(SUM(dsi.daily_interest),2) AS total_interest,
	   ROUND(SUM(c.compound_interest),2) AS total_compound_interest,
       ROUND(SUM(c.balance_with_ci),2) AS required_data
FROM compounding c
LEFT JOIN daily_simple_interest dsi
     ON c.customer_id = dsi.customer_id
GROUP BY month
ORDER BY month

