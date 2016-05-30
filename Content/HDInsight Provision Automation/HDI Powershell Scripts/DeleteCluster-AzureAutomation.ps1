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
# Delete HDInsight Cluster
###########################################
# Delete Cluster
Remove-AzureRmHDInsightCluster -ClusterName "hdi-edxlab4" # Provide Cluster Name