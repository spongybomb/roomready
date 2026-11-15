# ROOMREADY - deploy to AWS Academy Learner Lab (us-east-1) using the existing LabRole.
# NOT executed by the authors yet; see README "Verification status".
#
# Usage:
#   .\scripts\deploy.ps1 -LabRoleArn arn:aws:iam::<ACCOUNT_ID>:role/LabRole [-Stage dev] [-StackName roomready-dev]
#
# Steps: package Lambda zip -> render template (embeds the ASL) -> cloudformation deploy
#        -> upload the zip to the four functions with `aws lambda update-function-code`.
# No S3 artifact bucket is used, so there is nothing extra to empty at teardown.
param(
    [Parameter(Mandatory = $true)][string]$LabRoleArn,
    [string]$Stage = "dev",
    [string]$StackName = "",
    [string]$Region = "us-east-1",
    [string]$AdminUsers = "",
    [ValidateSet("false", "true")][string]$DemoAllowHeaderIdentity = "false",
    [ValidateSet("AWS_IAM", "NONE")][string]$RouteAuthorization = "AWS_IAM",
    [string]$Python = "python"
)
$ErrorActionPreference = "Stop"
if (-not $StackName) { $StackName = "roomready-$Stage" }
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

Write-Host "1/4 Packaging Lambda code"
& $Python scripts\package_lambda.py
if ($LASTEXITCODE -ne 0) { throw "packaging failed" }

Write-Host "2/4 Rendering CloudFormation template"
& $Python scripts\render_template.py
if ($LASTEXITCODE -ne 0) { throw "render failed" }

Write-Host "3/4 Deploying stack $StackName in $Region (no IAM capabilities required)"
aws cloudformation deploy `
    --region $Region `
    --stack-name $StackName `
    --template-file build\template.rendered.yaml `
    --parameter-overrides "LabRoleArn=$LabRoleArn" "Stage=$Stage" "AdminUsers=$AdminUsers" `
        "DemoAllowHeaderIdentity=$DemoAllowHeaderIdentity" "RouteAuthorization=$RouteAuthorization" `
    --no-fail-on-empty-changeset
if ($LASTEXITCODE -ne 0) { throw "cloudformation deploy failed" }

Write-Host "4/4 Uploading code to Lambda functions"
$outputs = aws cloudformation describe-stacks --region $Region --stack-name $StackName `
    --query "Stacks[0].Outputs" --output json | ConvertFrom-Json
$names = @("ApiFunctionName", "PollerFunctionName", "StepFunctionName", "StarterFunctionName")
foreach ($key in $names) {
    $fn = ($outputs | Where-Object { $_.OutputKey -eq $key }).OutputValue
    Write-Host "   updating $fn"
    aws lambda update-function-code --region $Region --function-name $fn `
        --zip-file fileb://build/roomready_lambda.zip --output text --query "LastUpdateStatus" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "update-function-code failed for $fn" }
    aws lambda wait function-updated --region $Region --function-name $fn
}

$api = ($outputs | Where-Object { $_.OutputKey -eq "ApiEndpoint" }).OutputValue
Write-Host ""
Write-Host "Deployed. API endpoint: $api"
Write-Host "Requests must be SigV4-signed (AWS_IAM) unless RouteAuthorization=NONE was chosen for a demo."
Write-Host "Remember: run .\scripts\teardown.ps1 -Stage $Stage when finished to protect the lab budget."
