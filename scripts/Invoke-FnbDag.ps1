param(
 [Parameter(Mandatory=$true)][ValidateSet('fnb_overture_ingestion','fnb_google_crawl','fnb_integration_gold')][string]$DagId,
 [Parameter(Mandatory=$true)][string]$ConfigFile,
 [string]$ProjectName='fnb-platform',
 [string]$ResultsRoot=(Join-Path ([IO.Path]::GetTempPath()) ('fnb-airflow01-'+[guid]::NewGuid().ToString('N')))
)
$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$compose=@('compose','--project-name',$ProjectName,'-f','compose.yaml','-f','compose.host.yaml')
if ((Test-Path $ResultsRoot) -and @(Get-ChildItem $ResultsRoot -Force).Count) { throw 'Use a fresh evidence directory' }
[System.IO.Directory]::CreateDirectory($ResultsRoot) | Out-Null
$conf=Get-Content -LiteralPath $ConfigFile -Raw | ConvertFrom-Json
if ($conf.mode -and $conf.mode -ne 'replay') { throw 'Initial verification runner permits pinned replay only' }
if (-not $conf.campaign_id -or -not $conf.pins -or -not $conf.pins.PSObject.Properties.Count) { throw 'Explicit campaign_id and real pins are required' }
$payload=$conf | ConvertTo-Json -Depth 50 -Compress
$metadata=@{dag_id=$DagId;status='RUNNING';started=[DateTime]::UtcNow.ToString('o');config_file=(Resolve-Path $ConfigFile).Path}
$metadata | ConvertTo-Json | Set-Content (Join-Path $ResultsRoot 'checkpoint.json')
try {
 & docker @compose exec -T airflow-scheduler python /opt/airflow/fnb_entry.py dags test $DagId --conf $payload 2>&1 | Tee-Object (Join-Path $ResultsRoot 'dag-test.log')
 $code=$LASTEXITCODE
 $metadata.exit_code=$code
 $metadata.status=if ($code -eq 0) {'PASS'} else {'FAIL'}
 & docker @compose exec -T airflow-scheduler python /opt/airflow/fnb_entry.py dags list-runs -d $DagId --output json *> (Join-Path $ResultsRoot 'dag-runs.log')
} catch { $metadata.status='FAIL';$metadata.error=$_.Exception.Message;$metadata.exit_code=1 }
finally {
 $metadata | ConvertTo-Json -Depth 10 | Set-Content (Join-Path $ResultsRoot 'verification.json')
 Write-Output "Artifacts: $ResultsRoot"
}
exit $metadata.exit_code
