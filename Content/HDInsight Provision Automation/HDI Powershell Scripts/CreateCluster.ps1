###########################################
# Azure Sign-In
###########################################
# Sign in
$azureUser = "" #Provide Microsoft account username
$azurePSWD = $password = ConvertTo-SecureString "" -AsPlainText -Force #Provide Microsoft Account Password
$azureCredential = New-Object System.Management.Automation.PSCredential ($azureUser, $azurePSWD)
Login-AzureRmAccount -Credential $azureCredential

# Select the subscription to use
$subscriptionID = "" # Provide Azure SubscriptionID
Select-AzureRmSubscription -SubscriptionId $subscriptionID

###########################################
# Create an HDInsight Cluster
###########################################
# Cluster Variables
$resourceGroupName = "" #Provide Resource Group Name
$storageAccountName = "" #Provide Storage Account Name
$containerName = "" #Provide Blob Container 
$storageAccountKey = Get-AzureRmStorageAccountKey -Name $storageAccountName -ResourceGroupName $resourceGroupName | %{ $_.Key1 }
$clusterName = $containerName                   
$clusterNodes = 1  
$clusterUser = "" #Provide Cluster Username
$clusterSSHUser = "" #Provide SSH Username
$clusterPSWD = ConvertTo-SecureString "" -AsPlainText -Force #Provide password to use for cluster and ssh if the same
$clusterCredential = New-Object System.Management.Automation.PSCredential ($clusterUser, $clusterPSWD)
$sshCredential = New-Object System.Management.Automation.PSCredential ($clusterSSHUser, $clusterPSWD)
$clusterType = "Hadoop"
$clusterOS = "Linux" 
$clusterNodeSize = "Standard_A3"
$location = Get-AzureRmStorageAccount -ResourceGroupName $resourceGroupName -StorageAccountName $storageAccountName | %{$_.Location}

# Create a new HDInsight cluster
New-AzureRmHDInsightCluster -ClusterName $clusterName -ResourceGroupName $resourceGroupName -HttpCredential $clusterCredential -Location $location -DefaultStorageAccountName "$storageAccountName.blob.core.windows.net" -DefaultStorageAccountKey $storageAccountKey -DefaultStorageContainer $containerName  -ClusterSizeInNodes $clusterNodes -ClusterType $clusterType -OSType $clusterOS -Version "3.2" -SshCredential $sshCredential -HeadNodeSize $clusterNodeSize -WorkerNodeSize $clusterNodeSize