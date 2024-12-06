# ADF-Pipeline-Databricks-Pyspark

Create Application 

Using Microsoft Entra Id (formerly know as Azure Active Directory-AAD) App Registration

Create Certificates and Secrets and copy the secret value imeediately and store it .


![alt text](<Reference Images/App Registration .png>)

Storage Account - IAM 

Create Role Assignment - Storage Blob Contributor 

Under Service Principle - Add the existing Application - Review and Assign 

Create Key Vault - Access Policies 

Under Secrets - Store the Client Secret Values 

In Azure Databricks Create Secrete Scope use the Vault URL and Resouce Id for DNS Name and Resource ID 