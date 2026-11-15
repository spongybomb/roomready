# ROOMREADY - remove every AWS resource created by deploy.ps1.
# Usage: .\scripts\teardown.ps1 [-Stage dev] [-StackName roomready-dev] [-Region us-east-1]
#
# Deleting the stack removes the DynamoDB table (all demo data), Lambda functions, HTTP API,
# EventBridge bus/rules and the state machine. No S3 artifact bucket is used by deploy.ps1,
# so there is no bucket to empty. If you created one manually (e.g. for `aws cloudformation
# package`), empty and delete it yourself:  aws s3 rm s3://<bucket> --recursive; aws s3 rb s3://<bucket>
# Lambda may have written default log groups (/aws/lambda/roomready-*); delete them if the lab permits.
param(
    [string]$Stage = "dev",
    [string]$StackName = "",
    [string]$Region = "us-east-1"
)
$ErrorActionPreference = "Stop"
if (-not $StackName) { $StackName = "roomready-$Stage" }
$root = Split-Path -Parent $PSScriptRoot

Write-Host "Deleting stack $StackName in $Region"
aws cloudformation delete-stack --region $Region --stack-name $StackName
if ($LASTEXITCODE -ne 0) { throw "delete-stack failed" }
aws cloudformation wait stack-delete-complete --region $Region --stack-name $StackName
if ($LASTEXITCODE -ne 0) { throw "stack deletion did not complete; check the CloudFormation console" }

$build = Join-Path $root "build"
if (Test-Path $build) { Remove-Item -Recurse -Force $build }
Write-Host "Stack deleted and local build/ removed."
