# Introduction 
Demo project to deploy Azure Data Factory and use the Managed VNET to create a private link to a Data Lake Gen2 Storage Account.

## Create a resource group
We need to create a resource group to hold Data Factory and the storage account. 
The following command will create this in the **UKSouth** region:

```bash
resource_group=adftest
az group create --name $resource_group --location uksouth
```

## Deploy the infrastructure
Next, we need to deploy the Data Factory and Storage Account infrastructure.
The following command will do this in the newly created resource group:

```bash
git checkout main
output=$(az deployment group create --resource-group $resource_group --template-file infra/datafactory.json --query properties.outputs)
data_factory=$(echo $output | jq -r .factoryName.value)
storage_account=$(echo $output | jq -r .storageName.value)
storage_id=$(echo $output | jq -r .storageResourceId.value)
storage_url=$(echo $output | jq -r .storageUrl.value)
storage_key=$(az storage account keys list --account-name $storage_account --query "[0].value" -otsv)
```

## Deploy the Managed VNET Runtime
Next, we need to deploy the published ARM template from Data Factory:

```bash
git checkout adf_publish
az deployment group create \
    --resource-group $resource_group \
    --template-file datafactory-sdaletest/ARMTemplateForFactory.json \
    --parameters \
        factoryName=$data_factory \
        DataLake_accountKey=$storage_key \
        DataLake_properties_typeProperties_url=$storage_url \
        DataLakeEndpoint_properties_privateLinkResourceId=$storage_id        
```

## Approve the Private Link
Finally, once all of the templates have been deployed, a Private Link Connection
should have been created on the Data Lake Gen2 Storage Account.

This must be approved, which can either be done through the Azure Portal for on-demand
requests, or using the following commands for a script:

```bash
git checkout main
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

subscription=$(az account show --query id -otsv)

python scripts/datafactory-link/datafactory.py \
    --subscription $subscription \
    --data-factory-rg $resource_group \
    --data-factory-name $data_factory

deactivate
```


