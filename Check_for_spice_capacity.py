import boto3   #Needs to be v1.10.28 or later
import numpy as np

AWS_ACCOUNT_ID = 'xxxxxxxx'
qsc = boto3.client("quicksight", region_name='eu-west-1')

print("Listing all quicksight datasets...")
#Identify all QuickSight datasets

all_datasets = []
token = ""
use_token = False

#Call the list_datasets methhod, using a token if we have one. 
#Stop when we get a response with no token.
while token is not None:        
    if use_token:
        dataset_call = qsc.list_data_sets(
            AwsAccountId=AWS_ACCOUNT_ID,
            NextToken=token
        )
    else:
        dataset_call = qsc.list_data_sets(
            AwsAccountId=AWS_ACCOUNT_ID,
        )

    datasets = dataset_call['DataSetSummaries']

    #If the response contains a NextToken field, get the token and use it in the next request
    try:
        token = dataset_call["NextToken"]
        use_token = True
    except KeyError:
        token=None
        
    all_datasets.extend(datasets)
    

print(f"{len(all_datasets)} datasets identified")

#Identify all of the datasets in Quicksight for which the import Mode is SPICE
print("Identifying SPICE datasets...")

spice_dict = {}
for dataset in all_datasets:
    if dataset["ImportMode"]=='SPICE':
        spice_dict[dataset['Name']] = dataset['DataSetId']


total_gb = 0


#For each of the SPICE datasets, obtain the SPICE usage in bytes and convert to GB.
#Then output the usage of each dataset and the total.
print("Calculating SPICE capacity usage...\n")

for dataset, dataset_id in spice_dict.items():

    dataset_details = qsc.describe_data_set(
    AwsAccountId=AWS_ACCOUNT_ID,
    DataSetId=dataset_id)['DataSet']

    usage = dataset_details['ConsumedSpiceCapacityInBytes']
    
    usage_gb = np.round(usage/(1024**3),2)
    total_gb += usage_gb
    print(f"{dataset}: Dataset id={dataset_id},    SPICE capacity used= {usage_gb}GB")

print(f"\nTotal: {total_gb}GB")