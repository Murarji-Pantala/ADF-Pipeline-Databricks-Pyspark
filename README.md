# ADF-Pipeline-Databricks-Pyspark

Create Application 

Using Microsoft Entra Id (formerly know as Azure Active Directory-AAD) App Registration

Create Certificates and Secrets and copy the secret value imeediately and store it .


![alt text](<Reference Images/App Registration .png>)

![alt text](<Reference Images/Project App Certificates and Secrets.png>)

Storage Account - IAM 

Create Role Assignment - Storage Blob Contributor 

Under Service Principle - Add the existing Application - Review and Assign 

Create Key Vault - Access Policies 

![alt text](<Reference Images/Key Vault Access Policies.png>)

![alt text](<Reference Images/Key Vault Properties.png>)

![alt text](<Reference Images/Key Vault Secrets - Project Token.png>)

![alt text](<Reference Images/Key Vault Secrets - Service Credentials.png>)

Under Secrets - Store the Client Secret Values 

In Azure Databricks Create Secrete Scope use the Vault URL and Resouce Id for DNS Name and Resource ID 

![alt text](<Reference Images/Create Scope.png>)

![alt text](<Reference Images/Databricks Acces Tokens.png>)

![alt text](<Reference Images/ADF Linked Services.png>)

![alt text](<Reference Images/Pipeline .png>)

![alt text](<Reference Images/Processed Container.png>)

![alt text](<Reference Images/Staging .png>)