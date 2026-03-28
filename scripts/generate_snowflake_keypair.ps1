$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$keyDir = Join-Path $projectRoot ".keys"
$privateKeyPath = Join-Path $keyDir "snowflake_rsa_key.p8"
$publicKeyPath = Join-Path $keyDir "snowflake_rsa_key.pub"
$sqlPath = Join-Path $keyDir "snowflake_set_public_key.sql"

New-Item -ItemType Directory -Force -Path $keyDir | Out-Null

# Generate a local passphrase for the encrypted private key.
$passphrase = openssl rand -base64 24

openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out $privateKeyPath -passout "pass:$passphrase"
openssl rsa -in $privateKeyPath -pubout -out $publicKeyPath -passin "pass:$passphrase"

$publicKeyBody = (Get-Content $publicKeyPath | Where-Object { $_ -notmatch "BEGIN PUBLIC KEY|END PUBLIC KEY" }) -join ""
$snowflakeUser = $env:SNOWFLAKE_USER
if (-not $snowflakeUser) {
    $snowflakeUser = "YOUR_SNOWFLAKE_USER"
}

$sql = @"
ALTER USER $snowflakeUser SET RSA_PUBLIC_KEY='$publicKeyBody';
"@
Set-Content -LiteralPath $sqlPath -Value $sql

Write-Output "Private key created: $privateKeyPath"
Write-Output "Public key created: $publicKeyPath"
Write-Output "SQL created: $sqlPath"
Write-Output ""
Write-Output "Add these lines to your .env:"
Write-Output "SNOWFLAKE_PRIVATE_KEY_PATH=.keys/snowflake_rsa_key.p8"
Write-Output "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=$passphrase"
