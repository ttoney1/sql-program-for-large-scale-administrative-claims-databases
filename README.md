# sql-program-for-large-scale-administrative-claims-databases
SQL program to create a new table within the cloud data management platform by joining multiple tables and summarizing tables in large-scale healthcare administrative claims databases

Part of SQL program to perform multiple joins and summaries of tables within large-scale healthcare administrative claims databases (billions of patient records for each hospital visit, diagnosis, lab, condition) Final table created in Databrick cloud data management platform  provides a summary of data sources from  claims databases that have been analyzed. Here's what each column represents:

Name: The name of the database (e.g., Clinical Practice Research Datalink (CPRD) GOLD).

OMOP: Indicates whether the database has been mapped to the OMOP (Observational Medical Outcomes Partnership) Common Data Model.

Domain: The type of data represented (e.g., Condition, Drug, Lab).

Vocabulary: The coding system or vocabulary used (e.g., Read, Gemscript).

Prevalence: The number of data points or prevalence values recorded.

Table_Name: The OMOP table where the data is stored (e.g., drug_exposure, condition_occurrence).

Data_Through_Date: The most recent date for which data was available.

Incidence: The percentage of new cases/events in the dataset.

Person_Count: The number of unique individuals represented.

Record_Count: The total number of data records associated with the domain.

In short:
The final table created wihtin in the cloud data management platform catalogs detailed metadata for datasets from multiple domains within claims databases, showing how much data is available, what it's about, and how it has been standardized using OMOP. Let me know if you'd like stats or insights from the full dataset. 


