###########################################
# Azure Sign-In
###########################################
# Sign in
$azureCredential = Get-AutomationPSCredential -Name 'cred-admin' #Automation credential for Azure login
Login-AzureRmAccount -Credential $azureCredential

# Select the subscription to use
$subscriptionID = Get-AutomationVariable -Name 'var-subscriptionid' #Automation variable for subscription id
Select-AzureRmSubscription -SubscriptionId $subscriptionID

###########################################
# Create HDInsight Cluster
###########################################
# Set Cluster Variables
$resourceGroupName = "" #Provide Resource Group Name
$storageAccountName = "" #Provide Storage Account Name
$containerName = "" #Provide Blob Container
$storageAccountKey = Get-AzureRmStorageAccountKey -Name $storageAccountName -ResourceGroupName $resourceGroupName | %{ $_.Key1 }
$clusterName = $containerName 
$clusterCredential = Get-AutomationPSCredential -Name 'cred-clusteruser' #Automation credential for cluster user
$sshCredential = Get-AutomationPSCredential -Name 'cred-sshuser' #Automation credential for ssh user
$clusterType = "Hadoop"
$clusterOS = "Linux"
$clusterNodes = 1  
$clusterNodeSize = "Standard_A3"
$location = Get-AzureRmStorageAccount -ResourceGroupName $resourceGroupName -StorageAccountName $storageAccountName | %{$_.Location}

# Create a new HDInsight cluster
New-AzureRmHDInsightCluster -ClusterName $clusterName -ResourceGroupName $resourceGroupName -HttpCredential $clusterCredential -Location $location -DefaultStorageAccountName "$storageAccountName.blob.core.windows.net" -DefaultStorageAccountKey $storageAccountKey -DefaultStorageContainer $containerName  -ClusterSizeInNodes $clusterNodes -ClusterType $clusterType -OSType $clusterOS -Version "3.2" -SshCredential $sshCredential -HeadNodeSize $clusterNodeSize -WorkerNodeSize $clusterNodeSize