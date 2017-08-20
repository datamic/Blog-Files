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
# Delete HDInsight Cluster
###########################################
# Delete Cluster
Remove-AzureRmHDInsightCluster -ClusterName "hdi-edxlab4" # Provide Cluster Name