# sql-program-for-large-scale-administrative-claims-databases
SQL program to create a new consolidated data table within the cloud data management platform by joining multiple tables, computing new columns through sub-queries, subsetting, aggregating, and summarizing tables from large-scale healthcare administrative claims databases
Note. Created the new database for the table in the terminal through Bash commands. 

Part of the SQL program to perform multiple joins and summaries of tables within large-scale healthcare administrative claims databases (billions of patient records for each hospial visit that collected medication exposure, lab, condition). Final table created in Databricks cloud data management platform provides a summary of data sources from claims databases that have been analyzed. Below is what each column in the final represents:

Name: The name of the database (e.g., Clinical Practice Research Datalink (CPRD) GOLD).

OMOP: Indicates whether the database has been mapped to the OMOP (Observational Medical Outcomes Partnership) Common Data Model.

Domain: The type of data represented (e.g., Condition, Drug, Lab).

Vocabulary: The coding system or vocabulary used (e.g., Read, Gemscript).

Prevalence (Record Count): The number of data points or prevalence values recorded for that vocabulary with the domain.

Table_Name: The OMOP table where the data is stored (e.g., drug_exposure, condition_occurrence).

Data_Through_Date: The most recent date for which data was available.

Incidence: The percentage of new cases/events in the dataset.

Person_Count: The number of unique individuals represented for that vocabulary with that domain.

In short:
The final table created within the cloud data management platform catalogs detailed metadata for datasets from multiple domains within claims databases, showing how much data is available, what it's about, and how it has been standardized using OMOP. Let me know if you'd like stats or insights from the full dataset. 


