# Utility to check connectivity to ADLS Gen2 from Databricks Workspace or Any Server.
This is a piece of java code to check the access connectivity from a given machine to the adls gen2 storage account.

What script does?:
1. Tests if the folder creation/deletion works fine.
2. Tests if the file upload to folder works fine.
3. Tests if the listing of folder works fine.

## Usage

#### Connect by using an account key
```
$ java ADLSClientConnector 
    --account_name <adls_gen2_storage_account_name>
    --container_name <storage_container_name> 
    --auth_type account_key 
    --account_key <your_storage_account_key>
```  
#### Connect by using Azure Active Directory (Azure AD)/Service Principal
```  
 $ java ADLSClientConnector 
    --account_name <adls_gen2_storage_account_name>
    --auth_type service_principal 
    --tenant_id <Azure tenant/directory ID> 
    --client_id <Azure Client App ID>
    --client_secret <Azure Client Secret>
``` 
 
 #### Sample Output:
 ```  
 $ Creating directory under the container
 Successfully created/overwrote the iwx_test_connectivity directory under the given container!
 Trying to upload a sample file to container csv-test...
 Creation of sample file succeeded!
 Trying to list the directory contents csv-test...
      PathName: iwx_test_connectivity/sample.txt
 Done Listing!
 Trying to remove a sample file from the container csv-test...
 Sample file deletion succeeded!
 Trying to remove the iwx_test_connectivity directory from the container csv-test...
 Directory iwx_test_connectivity deletion succeeded!
 Testing Complete!
 
 Process finished with exit code 0
```  
