/*Objective - SQL program to join and summarize tables in large-scale healthcare administrative claims databases
Code will consolidate newly joined tables into one table and save to Databricks cloud platform for other users to analyze*/

--OPTUM EHR PanTher  --data pull

CREATE OR REPLACE TABLE pwb_ttoney.titan11_de AS 
SELECT distinct SOURCE_VOCABULARY_ID AS drug_SOURCE_VOCABULARY_ID, format_number(COUNT (*), "#,###")  AS Medication_Count,
COUNT (distinct PERSON_ID)  AS Person_Count, COUNT (PERSON_ID)  AS Record_Count,
--UPDATE SCHEMAS IN NESTED QUERIES, not just on 'from' line!  
  (SELECT COUNT(distinct PERSON_ID) FROM rwd_optum_ehr_omopv5.drug_exposure WHERE PERSON_ID is not null) AS Total_Person_Count, 
  (SELECT COUNT(PERSON_ID) FROM rwd_optum_ehr_omopv5.drug_exposure WHERE PERSON_ID is not null) AS Total_Record_Count, 
Person_Count / Total_Person_Count AS Incidence
FROM rwd_optum_ehr_omopv5.drug_exposure
WHERE SOURCE_VOCABULARY_ID is not null
GROUP BY SOURCE_VOCABULARY_ID;

CREATE OR REPLACE TABLE pwb_ttoney.titan11_co AS 
SELECT distinct source_vocabulary_id AS condition_SOURCE_VOCABULARY_ID, format_number(COUNT (*), "#,###")  AS Condition_Count,
COUNT (distinct PERSON_ID)  AS Person_Count, COUNT (PERSON_ID)  AS Record_Count,
--UPDATE SCHEMAS IN NESTED QUERIES, not just on 'from' line!  
  (SELECT COUNT(distinct PERSON_ID) FROM rwd_optum_ehr_omopv5.condition_occurrence WHERE PERSON_ID is not null) AS Total_Person_Count, 
  (SELECT COUNT(PERSON_ID) FROM rwd_optum_ehr_omopv5.condition_occurrence WHERE PERSON_ID is not null) AS Total_Record_Count, 
Person_Count / Total_Person_Count AS Incidence
FROM rwd_optum_ehr_omopv5.condition_occurrence
WHERE SOURCE_VOCABULARY_ID is not null
GROUP BY SOURCE_VOCABULARY_ID;

CREATE OR REPLACE TABLE pwb_ttoney.titan11_pr AS 
SELECT distinct source_vocabulary_id AS procedure_SOURCE_VOCABULARY_ID, format_number(COUNT (*), "#,###")  AS Procedure_Count,
COUNT (distinct PERSON_ID)  AS Person_Count, COUNT (PERSON_ID)  AS Record_Count,
--UPDATE SCHEMAS IN NESTED QUERIES, not just on 'from' line!  
  (SELECT COUNT(distinct PERSON_ID) FROM rwd_optum_ehr_omopv5.procedure_occurrence WHERE PERSON_ID is not null) AS Total_Person_Count, 
  (SELECT COUNT(PERSON_ID) FROM rwd_optum_ehr_omopv5.procedure_occurrence WHERE PERSON_ID is not null) AS Total_Record_Count, 
Person_Count / Total_Person_Count AS Incidence
FROM rwd_optum_ehr_omopv5.procedure_occurrence
WHERE SOURCE_VOCABULARY_ID is not null
GROUP BY SOURCE_VOCABULARY_ID;

--view the results
SELECT *, 'drug'  AS source FROM pwb_ttoney.titan11_de UNION ALL
SELECT *, 'condition'  AS source  FROM pwb_ttoney.titan11_co UNION ALL
SELECT *, 'procedure'  AS source  FROM pwb_ttoney.titan11_pr













MAKE FINAL TABLE – FORMATTING COLUMNS 
--OPTUM EHR PanTher
--UPDATE TO ABOVE TABLE --STACK INSTEAD OF JOIN

--UPDATE SCHEMA, TABLE NAMES AND COLUMN VALUES FOR EACH SCHEMA

CREATE OR REPLACE TABLE pwb_ttoney.titan11
AS
--drugs
SELECT
'OPTUM EHR PanTher' AS Name,
'Yes' AS OMOP,
'Drug' AS Domain,
 drug_SOURCE_VOCABULARY_ID AS Vocabulary, 
 Medication_Count AS Prevalence, 
 'drug_exposure' AS Table_Name,
 --Add date
(select SOURCE_RELEASE_DATE from rwd_optum_ehr_omopv5.cdm_source) AS Data_Through_Date,
format_number(Incidence, "#,###.##%") AS Incidence, 
format_number(Person_Count, "#,###") AS Person_Count,
format_number(Record_Count, "#,###") AS Record_Count    
--MAKE SURE TO UPDATE SCHEMA!
 FROM pwb_ttoney.titan11_de UNION ALL

--conditions 
SELECT 
'OPTUM EHR PanTher' AS Name,
'Yes' AS OMOP,
'Condition' AS Domain,
condition_SOURCE_VOCABULARY_ID AS Vocabulary, 
Condition_Count AS Prevalence, 
'condition_occurrence' AS Table_Name,
 --Add date
(select SOURCE_RELEASE_DATE from rwd_optum_ehr_omopv5.cdm_source) AS Data_Through_Date,
format_number(Incidence, "#,###.##%") AS Incidence, 
format_number(Person_Count, "#,###") AS Person_Count,
format_number(Record_Count, "#,###") AS Record_Count    
FROM pwb_ttoney.titan11_co UNION ALL

--procedures
SELECT 
'OPTUM EHR PanTher' AS Name,
'Yes' AS OMOP,
'Procedure' AS Domain,
procedure_SOURCE_VOCABULARY_ID AS Vocabulary, 
Procedure_Count AS Prevalence, 
'procedure_occurrence' AS Table_Name,
 --Add date
(select SOURCE_RELEASE_DATE from rwd_optum_ehr_omopv5.cdm_source) AS Data_Through_Date,
format_number(Incidence, "#,###.##%") AS Incidence, 
format_number(Person_Count, "#,###") AS Person_Count,
format_number(Record_Count, "#,###") AS Record_Count    
FROM pwb_ttoney.titan11_pr;

--view results
SELECT * FROM pwb_ttoney.titan11


















--DeSC --data pull

--for DeSC, must join drug table to vocab concept table
CREATE OR REPLACE TABLE pwb_ttoney.titan14_de AS 
SELECT distinct a.VOCABULARY_ID AS drug_SOURCE_VOCABULARY_ID, format_number(COUNT (*), "#,###") AS Medication_Count,
--Verify if getting person count columns from this table is correct since this table structure differs slightly form the traditional OMOP tables 
COUNT (distinct b.person_id)  AS Person_Count, COUNT (b.person_id)  AS Record_Count,
--UPDATE SCHEMAS IN NESTED QUERIES, not just on 'from' line!  
  (SELECT COUNT(distinct person_id) FROM pwb_desc_omop_20230831.drug_exposure WHERE person_id is not null) AS Total_Person_Count, 
  (SELECT COUNT(person_id) FROM pwb_desc_omop_20230831.drug_exposure WHERE person_id is not null) AS Total_Record_Count, 
Person_Count / Total_Person_Count AS Incidence 
FROM rwd_omop_vocabulary_v5.concept a
JOIN pwb_desc_omop_20230831.drug_exposure b
ON a.CONCEPT_CODE= b.drug_source_value
WHERE VOCABULARY_ID is not null
GROUP BY VOCABULARY_ID;

CREATE OR REPLACE TABLE pwb_ttoney.titan14_co AS 
SELECT distinct condition_source_value_2_type AS condition_SOURCE_VOCABULARY_ID, format_number(COUNT (*), "#,###")  AS Condition_Count,
COUNT (distinct person_id)  AS Person_Count, COUNT (person_id)  AS Record_Count,
--UPDATE SCHEMAS IN NESTED QUERIES, not just on 'from' line!  
  (SELECT COUNT(distinct person_id) FROM pwb_desc_omop_20230831.condition_occurrence WHERE person_id is not null) AS Total_Person_Count, 
  (SELECT COUNT(person_id) FROM pwb_desc_omop_20230831.condition_occurrence WHERE person_id is not null) AS Total_Record_Count, 
Person_Count / Total_Person_Count AS Incidence 
FROM pwb_desc_omop_20230831.condition_occurrence
WHERE condition_source_value_2_type is not null
GROUP BY condition_source_value_2_type;

CREATE OR REPLACE TABLE pwb_ttoney.titan14_pr AS 
SELECT distinct a.VOCABULARY_ID AS procedure_SOURCE_VOCABULARY_ID, format_number(COUNT (*), "#,###") AS Procedure_Count,
--Verify if getting person count columns from this table is correct since this table structure differs slightly form the traditional OMOP tables 
COUNT (distinct b.person_id)  AS Person_Count, COUNT (b.person_id)  AS Record_Count,
--UPDATE SCHEMAS IN NESTED QUERIES, not just on 'from' line!  
  (SELECT COUNT(distinct person_id) FROM pwb_desc_omop_20230831.procedure_occurrence WHERE person_id is not null) AS Total_Person_Count, 
  (SELECT COUNT(person_id) FROM pwb_desc_omop_20230831.procedure_occurrence WHERE person_id is not null) AS Total_Record_Count, 
Person_Count / Total_Person_Count AS Incidence  
FROM rwd_omop_vocabulary_v5.concept a
JOIN pwb_desc_omop_20230831.procedure_occurrence b
ON a.CONCEPT_CODE= b.procedure_source_value
WHERE VOCABULARY_ID is not null
GROUP BY VOCABULARY_ID;

--view the results
SELECT *, 'drug'  AS source FROM pwb_ttoney.titan14_de UNION ALL
SELECT *, 'condition'  AS source  FROM pwb_ttoney.titan14_co UNION ALL
SELECT *, 'procedure'  AS source  FROM pwb_ttoney.titan14_pr



--drug part 2
--de-duplicate drug becasue CFR and EMR overlap. 
--deduplicate by aggregating by sum
CREATE OR REPLACE TABLE pwb_ttoney.titan17_de_b AS 
SELECT distinct drug_SOURCE_VOCABULARY_ID, format_number(SUM(Medication_Count) , "#,###")  AS Medication_Count  
FROM pwb_ttoney.titan17_de_a 
GROUP BY drug_SOURCE_VOCABULARY_ID;


