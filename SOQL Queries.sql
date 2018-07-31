--Example SOQL Queries to see null leads who have not converted

 SELECT COUNT(Id) FROM Lead WHERE CreatedDate = LAST_N_DAYS:30 AND IsDeleted = false
=> 2470

SELECT COUNT(Id) FROM Lead WHERE CreatedDate = LAST_N_DAYS:30 AND IsDeleted = false AND mk_customer_fit_segment__c = NULL
=> 318

SELECT COUNT(Id) FROM Lead WHERE CreatedDate = LAST_N_DAYS:30 AND IsDeleted = false AND IsConverted = false
=> 1806

SELECT COUNT(Id) FROM Lead WHERE CreatedDate = LAST_N_DAYS:30 AND IsDeleted = false AND IsConverted = false AND mk_customer_fit_segment__c = NULL
=> 0


-- Count field we don't have in our tables
SELECT Product_Type__c, COUNT(Id) FROM Opportunity GROUP BY Product_Type__c



Run a bulk query and upload to redshift:
SELECT Id, Product_Type__c FROM Opportunity WHERE Product_Type__c = 'Enterprise Plan' OR Product_Type__c = 'Team Plan'


--Updated table with workbench's function: Run a bulk query and upload to redshift 
UPDATE crm_opportunities
SET a_opportunity_type__c = 'Sales'
WHERE
source_key_value IN (SELECT id FROM prototype_salesforce_raw_bulk)

-- OR
UPDATE crm_opportunities
SET a_Product_Type__c = p.Product_Type__c
FROM prototype_salesforce_raw_bulk p
WHERE crm_opportunities.source_key_value = p.Id

SELECT Id, MK_Account_Count_of_Active_Users_Last_30__c FROM Contact WHERE CreatedDate = LAST_N_DAYS:365

