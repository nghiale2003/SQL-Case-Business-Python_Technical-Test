# SQL-Case-Business-Python_Technical-Test
The test's purpose is checking the SQL function(Join, CTE, Datetime Function,...), ML(Datetime Function,NTL, Supervised Learning) and Problem Solving skill

# Summary

The test has 3 part, you can check them in 'Screening Test' file i attached. Now, i will summary the workflow and answer for each question

# Question 1: i use Postgresql language for this question

## a) Portfolio management: Display the cumulative approval rate by month for each source in 2023

**Note:**

- cum_appoval_rate= Lay num_approved tich luy cua tung source / tong total_num_approved tung source

        # Merge 'LEADS' voi 'CUSTOMERS', Extra 'MONTH', filter "approved" LEADS
        
        WITH 
        table_info AS(
        	SELECT  TO_CHAR(l.apply_date, 'YYYY-mm') as month_in_2023
        			, l.lastest_status
        			, c.source
        	FROM LEADS AS l
        	INNER JOIN CUSTOMERS AS c
        		USING(customer_id)
        	WHERE EXTRACT(YEAR FROM apply_date)=2023
        			AND latest_status='approved')
        
        #Groupby theo 'source','month'
        
        ,table_group AS(
        	SELECT source
        			, month_in_2023
        			,COUNT(latest_status) as num_of_approved
        	FROM table_info
        	GROUP BY 1,2)
        
        #Tinh Cumulative Sum
        ,table_cumsum AS( 
        	SELECT source
        			,month_in_2023
        			,num_of_approved
        			,SUM(num_of_approved) OVER(PARTITION BY source ORDER BY month_in_2023 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_sum_approved
        			,SUM(num_of_approved) OVER(PARTITION BY source) as total_by_month
        	FROM table_group)
        
        #TInh cum_approval_rate by month
        SELECT source
        		, month_in_2023
        		, ROUND(cum_sum_approved*100.0 / total_by_month,2) as cum_sum_approval_rate
        FROM table_cumsum
        ORDER BY 1,2


## b) Marketing: Display the names of customers who applied in 2023 and are older than 92.5% of all leads who applied in 2022

      #Merge CUSTOMERS voi LEADS
      WITH
      data_raw AS (
      	SELECT c.customer_id
      			, c.customer_name
      			, c.customer_age
      			,l.apply_date
      	FROM LEADS AS l
      	INNER JOIN CUSTOMERS AS c
      		USING(customer_id))
      
      #Tinh quantile 92.5% age
      , customer_age_2022_quantile AS (
      
      	SELECT PERCENTILE_CONT(0.925) WITHIN GROUP (ORDER BY customer_age) as cal_quantile_age
      	FROM (
      			SELECT DISTINCT customer_id
      							, customer_age
      			FROM data_raw
      			WHERE EXTRACT(YEAR FROM apply_date)=2022) as cus_applied_2022 )
      
      #So sanh "customer_age" cua khach hang apply in 2023 => display name
      SELECT DISTINCT customer_id
      		, customer_name
      FROM data_raw
      WHERE EXTRACT(YEAR FROM apply_date)=2023
      	AND customer_age >= (SELECT cal_quantile_age 
      							FROM customer_age_2022_quantile)


## c) Rollover detection: Display the names of customers 
-- who applied 5 times consecutively (1)

-- 25-35 days gap between any two continuous application dates (2)

-- the loan amount is increasing (each subsequent application being larger than the previous one)(3)

-- all 5 banks applied being different(4)

    #Merge LEADS voi PRODUCTS
    WITH
    data_raw AS (
    	SELECT
    			l.customer_id
    			,l.apply_date
    			,p.loan_amount
    			,p.bank_id
    	FROM LEADS AS l
    	INNER JOIN PRODUCTS AS p
    		USING(product_id))
    
    ,pre_loan_amount AS (
    	SELECT customer_id
    			,customer_id
    			,apply_date
    			,loan_amount
    			,bank_id
    			,LAG(apply_date) OVER(PARTITION customer_id ORDER BY apply_date) as pre_apply
    			,LAG(loan_amount) OVER(PARTITION customer_id ORDER BY apply_date) as pre_loan_amount
    	FROM data_raw)
    
    #Filter cac applied thoa dieu kien
    
    ## (4) Cac Khach hang apply tu 5 ngan hang khac nhau va co toi thieu 5 leads
    ,list_customer_filter_bank AS (
    	SELECT DISTINCT customer_id
    	FROM data_raw
    	GROUP BY customer_id
    	HAVING COUNT(DISTINCT bank_id) >= 5
    			AND COUNT(apply_date) >= 5)
    
    ## (1),(2),(3) Cac khach hang co 5 giao dich lien tiep
    , condition_flag_table AS (
    	SELECT customer_id
    			,apply_date
    			,loan_amount
    			,pre_apply
    			,pre_loan_amount
    			,CASE WHEN (DATE_PART('day', apply_date - pre_apply) BETWEEN 25 AND 35)
    						AND (loan_amount > pre_loan_amount)
    					THEN 0
    			ELSE 1 END condition_flag
    	FROM pre_loan_amount
    
    #Tao streak de GROUP BY theo customer_id, streak tim khach hang thoa dieu kien de bai
    , streak_table AS (
    	SELECT customer_id
    			,apply_date
    			,loan_amount
    			,pre_apply
    			,pre_loan_amount
    			, SUM(runtot) OVER(PARTITION BY customer_id ORDER BY apply_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as streak
    	FROM filter_condition_table)
    
    -- Tao list customer thoa dieu kien co 5 lan apply lien tuc (25-35 days gap)
    , list_customer_5_apply AS (
    
    		SELECT DISTINCT customer_id
    		FROM streak_table
    		GROUP BY customer_id, streak
    		HAVING COUNT(apply_date) >=5)
    
    -- Tao list khach hang theo dieu kien de bai
    , list_customer_outcome AS (
    		SELECT DISTINCT lb.customer_id
    		FROM list_customer_filter_bank AS lb
    		INNER JOIN list_customer_5_apply AS la
    			USING(customer_id)
    		)
    
    #Display names of filter customers
    SELECT DISTINCT customer_id, customer_name
    FROM CUSTOMERS
    WHERE customer_id IN (SELECT customer_id FROM list_customer_outcome)

## d) Create a user-defined SQL function that takes a customer_id as input. The output should be a message congratulating the customer if they can apply for the largest possible loan amount from a bank name, or a message expressing regret if no suitable loan product is found. Ensure that A customer with a certain risk level cannot be matched with products that accept a lower risk level.

Output example:

"Congratulations, You can apply for a loan with the amount of 10,000 USD from HSBC bank."

Or

"Sorry, we cannot find any suitable loan products for you at this time."

    CREATE FUNCTION identify_loan(customer_id_input as int)
    RETURNS VAR(100) AS $$
    BEGIN
    	RETURN QUERY
    
    	#Tim estimated_risk_level cua khach hang => transform ra so
    	WITH
    	filter_customer AS (
    		SELECT DISTINCT customer_id
    				,CASE WHEN estimated_risk_level='low' THEN 1
    					WHEN estimated_risk_level ='medium' THEN 2
    					WHEN estimated_risk_level ='high' THEN 3
    				END as num_estimated
    		FROM CUSTOMERS
    		WHERE customer_id=customer_id_input)
    
    	#Tim accepted_risk_level cua bank => transform ra so
    	,products_and_bank AS (
    		SELECT p.product_id
    				, p.product_name
    				, p.loan_amount
    				, b.bank_name
    				,CASE WHEN p.accepted_risk_level ='low' THEN 1
    					WHEN p.accepted_risk_level  ='medium' THEN 2
    					WHEN p.accepted_risk_level  ='high' THEN 3
    				END as num_accepted
    		FROM PRODUCTS AS p
    		LEFT JOIN BANKS AS b
    			USING(bank_id))
    	#Tim cac product_name khach hang co the vay => best choice
    	,best_choice AS (
    		SELECT product_name
    				,loan_amount
    				,bank_name
    		FROM (
    
    			SELECT product_id
    				, product_name
    				, loan_amount
    				, bank_name
    				,DENSE_RANK() OVER(ORDER BY loan_amount DESC) as rn
    			FROM products_and_bank
    			WHERE num_accepted >= (SELECT num_estimated FROM filter_customer)) as filter_product
    		
    		WHERE rn=1
    
    	SELECT CASE WHEN product_id IS NOT NULL THEN
    						CONCAT('Congratulations, You can apply for a loan with the amount of ', loan_amount,' from ', bank_name,'.' )
    				ELSE 'Sorry, we cannot find any suitable loan products for you at this time.'
    			END string_ouput
    	FROM best_choice;
    
        
    END; $$
    LANGUAGE plpgsql;







## e) The race of products: For each month in the year 2023, identify and display the following information:
● The month (formatted as YYYY-MM).
● The number of unique loan products that had at least 100,000 applications in that month and in every previous month of 2023 (“good” product).
● The name of the "best" loan product for the month.
● The number of applications for the "best" loan product.
-- The “best” product must be a “good” product and have the highest number of applications for
-- the month. If there are more than one product has maximum number of applications, the product
-- with the lower proportion of estimated 'high' risk level applicants is considered better.

    #Merge LEADS voi CUSTOMERS va filter LEADS in 2023
    
    WITH
    data_raw AS(
    	SELECT customer_id
    			, product_id
    			,TO_CHAR(apply_date, 'YYYY-mm') as formatted_month
    			,EXTRACT(MONTH FROM apply_date) as month
    			,CASE WHEN estimated_risk_level='high' THEN 1 ELSE 0 END high_risk
    
    	FROM LEADS
    	INNER JOIN CUSTOMERS
    	WHERE EXTRACT(MONTH FROM apply_date)=2023)
    
    #Tinh number_applicant by product, month
    , products_by_month AS (
    	SELECT  month
    			, formatted_month
    			, product_id
    			,COUNT(customer_id) as number_applicant
    			,CASE WHEN COUNT(customer_id) >= 100000 THEN 1 ELSE 0 END check_product 
    			,SUM(high_risk) / COUNT(*) AS high_risk_rate
    	FROM data_raw
    	GROUP BY 1,2,3)
    
    #check good_product (do good_product tinh tu thang 1 toi thang hien tai phai co number_applicant>= 100000)
    -- => cumulative 'check_product' = month
    , good_product_raw AS (
    	SELECT formatted_month
    			,month
    			,product_id
    			,number_applicant
    			,high_risk_rate
    			,SUM(check_product) OVER(PARTITION BY product_id ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_check_product 
    			END good_product
    	FROM products_by_month)
    
    , check_product_table AS (
    	SELECT formatted_month
    			,product_id
    			,number_applicant
    			,high_risk_rate
    			,CASE WHEN cum_check_product= month THEN 'True' END good_product
    	FROM good_product_raw)
    
    #Dem so good_product
    , number_good_product_outcome AS (
    
    	SELECT formatted_month
    			,COUNT(product_id) AS Number_good_product
    	FROM check_product_table
    	WHERE good_product='True'
    	GROUP BY 1)
    
    #Tim best product trong so good_product
    , best_product AS (
    	SELECT b.formatted_month
    			,b.product_id
    			,p.product_name
    			,b.number_applicant as maximum_applications
    	FROM (
    			SELECT formatted_month
    					, product_id
    					, number_applicant
    					,DENSE_RANK() OVER(PARTITION BY month, formatted_month ORDER BY number_applicant DESC, high_risk_rate) as rn
    			FROM check_good_table
    			WHERE good_product='True') AS b
    
    	LEFT JOIN PRODUCTS AS p
    		USING(product_id)
    	WHERE rn=1)
    
    #Tao 1 bang dimension month (do co the co thang ko co good_product)
    , generate_month AS(
    	SELECT TO_CHAR(months, 'YYYY-mm') AS formatted_month
    	FROM generate_series(
    	    '2023-01-01' :: DATE,
    	    '2023-12-01' :: DATE ,
    	    '1 month') AS months)
    
    #Join cac bang lai voi nhau
    SELECT gm.formatted_month
    		,no.Number_good_product
    		,bp.product_name as Best_product
    		,bp.maximum_applications 
    FROM generate_month as gm
    LEFT JOIN number_good_product_outcome as no
    	USING(formatted_month)
    LEFT JOIN best_product as bp
    	USING(formatted_month)

# Question 2a):

The budget of $60,000 should be allocated as the following table:

![image](https://github.com/user-attachments/assets/9374a4f6-d7e7-4e1c-889d-6964cc1efe36)

**Explain:**

Based on the potential customer pool size information of the next quarter => calculate the percentage of budget that should be analyzed additionally for each customer 

![image](https://github.com/user-attachments/assets/4f2f8b8a-74e8-481c-9777-d5b8f6ea48b5)

Because the business has just started a new product, the top priority is to maximize revenue, so here I will find ways to maximize revenue.

Below are the metrics calculated for each customer segment of interest:

- Cost per channel: total_revenue: revenue through channel, cost_by_revenue: % of cost in total revenue of each channel
- Reach per channel: number_customer: number of customers who sent information, saw_review_rate: % of customers who clicked to view the news, bounced_rate: percentage of customers who bounced
- Conversion per channel: conversion_rate: conversion rate

**Cost per channel**

![image](https://github.com/user-attachments/assets/fc32a55d-2cb3-4846-8c35-26e86c429638)

**Reach per channel**

![image](https://github.com/user-attachments/assets/ba49c1e8-c765-40aa-ba3b-22e594b83d3b)

**Conversation per channel**

![image](https://github.com/user-attachments/assets/5c3bc89e-0115-4769-99d6-0a886714f233)

### Age Group 18-30:
- This is the youngest customer group and contributes significantly to the company's revenue.
- Revenue generated from the SMS channel accounts for 65% of the segment, and the cost of SMS is cheaper than Email.
- The SMS channel reaches 54% of customers, while Email reaches 46%. However, the SMS saw_review_rate is 4.6% higher.
- The add_to_cart and conversion rates for SMS are higher than for Email, indicating that higher marketing spending on SMS leads to more customers adding items to their cart, anticipating promotional events.
- 
**=> Budget allocation: SMS: 65% * 13,200 = 8,580, Email 35%: 4,620.**

### Age Group 31-45:
- This customer group is one of the two largest revenue sources for the company.
- The Email channel generates 2.4 times more revenue than SMS, with marketing costs being half as much.
- The saw_review_rate for Email is nearly twice that of SMS, though the number of customers reached is lower.
- Conversely, the add_to_cart rate for SMS is higher after customers click to view a product.
- The conversion rate for Email is three times that of SMS.
- 
**=> Budget allocation: Email: 60% * 15,600 = 9,360, SMS 40%: 6,240.**

### Age Group 46-60:
- This is the most promising customer segment for the company, though current revenue from this group is below expectations.
- The SMS channel holds a near-absolute advantage, generating three times the revenue of Email at a lower cost. Additionally, the add_to_cart and conversion rates for SMS are double those of Email.
- 
**=> Budget allocation: SMS: 70% * 22,200 = 15,540, Email 30%: 6,660.**

### Age Group 60+:
- This is the least promising customer segment for the company, with costs almost always exceeding revenue.
- Moreover, the conversion rate is extremely low, at just 0.05%.
- 
**=> Reallocate the initial budget of 9,000 evenly among the remaining customer segments.**

# Question 3: 
[Question 3_Machine Learning Model](https://colab.research.google.com/drive/1KqrCZeHO_kOl2StQ8MEmTDgPBFztoj9y)

  












