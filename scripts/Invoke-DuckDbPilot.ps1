#requires -Version 7.2
[CmdletBinding()]
param(
    [ValidatePattern('^[a-z0-9-]{1,30}$')][string]$RunName='mentor-demo',
    [ValidateSet('Demo','Real','Both')][string]$Mode='Demo',
    [ValidateSet('Setup','Stop','Restart')][string]$Action='Setup',
    [switch]$FreshClone,
    [switch]$ResumeFresh,
    [string]$InputVolume='',
    [switch]$WithRuntime,
    [ValidateSet('','dimensional_analytics','business_analytics','gold_staging','area_enrichment')][string]$Docs='',
    [string]$GoldRelease='', [string]$GoldManifestSha='',
    [string]$AnalyticsRelease='', [string]$AnalyticsManifestSha='',
    [string]$DemographicsRelease='', [string]$DemographicsManifestSha='',
    [ValidateRange(0,65535)][int]$Port=0,
    [ValidateRange(0,65535)][int]$PostgresPort=0,
    [ValidateRange(0,65535)][int]$DocsPort=0,
    [ValidateRange(0,65535)][int]$AirflowPort=0
)
$ErrorActionPreference='Stop'
$repo=(Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location -LiteralPath $repo
function Invoke-DockerCommand([string[]]$Arguments){
    $executable=(Get-Command docker -CommandType Application -ErrorAction Stop|Select-Object -First 1).Source
    & $executable @Arguments
    if($LASTEXITCODE -ne 0){throw "Docker failed, exit $LASTEXITCODE; inspect stage log"}
}
function Read-Settings([string]$Path){
    $settings=@{}
    foreach($line in [IO.File]::ReadAllLines($Path)){
        if($line -match '^\s*(FNB_[A-Z0-9_]+)\s*=\s*(.*?)\s*$'){
            $value=$Matches[2]
            if($value.Length -ge 2 -and (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'")))){$value=$value.Substring(1,$value.Length-2)}
            $settings[$Matches[1]]=$value
        }
    }
    return $settings
}
function Restore-Environment([hashtable]$Original){
    foreach($name in $Original.Keys){
        # PowerShell can coerce $null to an empty string at a .NET string boundary.
        # Preserve absence explicitly so the next invocation still reads .env.
        if($null -eq $Original[$name]){Remove-Item -LiteralPath ('Env:'+$name) -ErrorAction SilentlyContinue}
        else{[Environment]::SetEnvironmentVariable($name,$Original[$name])}
    }
}
function Assert-Ports([hashtable]$Services,[string]$OwnerProject=$project){
    $claimed=@{}
    foreach($service in $Services.Keys){
        $port=[int]$Services[$service]
        if($port -lt 1024 -or $port -gt 65535 -or $claimed.ContainsKey($port)){throw 'UI/database ports must be distinct and between 1024 and 65535'}
        $claimed[$port]=$true
        $owned=@(Invoke-DockerCommand @('ps','--filter',"label=com.docker.compose.project=$OwnerProject",'--filter',"label=com.docker.compose.service=$service",'--filter',"publish=$port",'-q'))
        if($owned.Count){continue}
        $listener=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,$port)
        try{$listener.Start()}catch{throw "Port $port is busy ($service). Choose unused ports in .env or explicit -Port/-PostgresPort/-DocsPort; preserve existing runs."}finally{$listener.Stop()}
    }
}
function Save-State {$state|ConvertTo-Json -Depth 12|Set-Content -Encoding utf8 -LiteralPath ($checkpoint+'.tmp');[IO.File]::Move($checkpoint+'.tmp',$checkpoint,$true)}
function Step([string]$Name,[scriptblock]$Action,[switch]$Always){
    if(-not $Always -and $state.stages[$Name] -eq 'PASS'){Write-Host "$Name : checkpoint reused";return}
    $state.phase=$Name;$state.stages[$Name]='RUNNING';Save-State
    $log=Join-Path $run ($Name+'-'+[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')+'.log')
    Write-Host "$Name : running; full log $log"
    try{& $Action *> $log;$state.stages[$Name]='PASS';Save-State}
    catch{$state.stages[$Name]='FAIL';Add-Content -Encoding utf8 -LiteralPath $log -Value ('ERROR: '+$_.Exception.Message);throw "Stage $Name failed; log/checkpoint preserved: $log"}
}
function Verify-Browser([string]$Name){Invoke-DockerCommand ($compose+@('run','--rm','--no-deps','bi-verify','--url','http://127.0.0.1:8088','--output',('/verification-out/'+$Name)))}
function Logical-Tables([string]$Output,[string]$BundleId=''){
    if(-not $BundleId){$pointer=Get-Content -Raw -LiteralPath (Join-Path $Output 'current.json')|ConvertFrom-Json -AsHashtable;$BundleId=$pointer.bundle_id}
    if($BundleId -notmatch '^[a-f0-9]{24}$'){throw 'Invalid warehouse bundle identity'}
    $manifest=Get-Content -Raw -LiteralPath (Join-Path $Output ('releases/'+$BundleId+'/manifest.json'))|ConvertFrom-Json -AsHashtable
    return (($manifest.tables.GetEnumerator()|Sort-Object Key|ForEach-Object {$_.Key+'='+$_.Value.rows+':'+$_.Value.logical_sha256}) -join '|')
}

# Export only committed files plus the explicit DX change set. No .env, data,
# existing warehouse, unrelated crawler edits or previous metadata volume follows.
if($ResumeFresh -and -not $FreshClone){throw 'ResumeFresh requires FreshClone and its original run name/ports'}
if($FreshClone){
    if($Mode -ne 'Demo' -or $Action -ne 'Setup' -or $Docs -or $WithRuntime){throw 'FreshClone verifies the public BI Demo only'}
    $owned=Join-Path $repo ('.local/mentor-verification/'+$RunName)
    [IO.Directory]::CreateDirectory($owned)|Out-Null
    $source=Join-Path $owned 'source'
    $files=@('.dockerignore','.env.example','README.md','compose.analytics.yaml','docker/analytics/Dockerfile','docker/analytics/configure_superset.py','docker/analytics/superset_config.py','docker/analytics/market_overview.zip','scripts/Invoke-DuckDbPilot.ps1','scripts/Invoke-FnbDag.ps1','scripts/verify_superset.py','scripts/init_analytics_secrets.py','src/fnb_platform/analytics/demo.py','src/fnb_platform/analytics/warehouse.py','tests/test_superset_bootstrap.py','docs/MONITORING_HANDOFF.md')
    $revision=git rev-parse HEAD;if($LASTEXITCODE){throw 'FreshClone needs Git and a repository checkout'}
    $identity=[ordered]@{revision=$revision;files=@($files|ForEach-Object{Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $repo $_)|Select-Object Hash})}|ConvertTo-Json -Depth 5 -Compress
    $marker=Join-Path $owned 'source-identity.json'
    if($ResumeFresh){
        if(-not(Test-Path $marker)){throw 'No frozen source identity exists for ResumeFresh'}
        $saved=Get-Content -Raw -LiteralPath $marker|ConvertFrom-Json -AsHashtable
        if($saved.files.Count -ne $files.Count){throw 'Frozen source inventory differs; preserve evidence and use a new run'}
        for($i=0;$i -lt $files.Count;$i++){
            if((Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $source $files[$i])).Hash -ne $saved.files[$i].Hash){throw ('Frozen source changed: '+$files[$i]+'. Resume rejected.')}
        }
        # Resume executes the original exported source, never copies newer code
        # into its checkpoint. Each child retains its own signature/input guards.
        $result=Get-Content -Raw -LiteralPath (Join-Path $owned 'verification.json')|ConvertFrom-Json -AsHashtable
        Copy-Item -LiteralPath (Join-Path $owned 'verification.json') -Destination (Join-Path $owned ('verification-before-resume-'+[guid]::NewGuid().ToString('N')+'.json'))
        $result.status='RUNNING';[void]$result.Remove('error');$result.resumed_frozen_source=$saved.revision
    }
    elseif(Test-Path $marker){if((Get-Content -Raw $marker).Trim() -ne $identity){throw 'FreshClone source changed; retain this evidence and choose a new RunName or ResumeFresh for the unchanged frozen export'}}
    else{
        try{Invoke-DockerCommand @('info','--format','{{.ServerVersion}}')}catch{throw 'Docker is unavailable. Start Docker Desktop with Linux containers before FreshClone verification.'}
        $freshProject='fnb-duckdb-'+$RunName
        $containers=@(Invoke-DockerCommand @('ps','-a','--filter',"label=com.docker.compose.project=$freshProject",'-q'))
        $volumes=@(Invoke-DockerCommand @('volume','ls','--filter',('name=^'+$freshProject+'_(postgres_data|superset_home|demo_outputs)$'),'-q'))
        if($containers.Count -or $volumes.Count){throw 'FreshClone needs an unused project/volume name. Retain existing data and choose a new RunName.'}
        if(Test-Path $source){throw 'Unowned existing fresh source directory; choose a new RunName'}
        $archive=Join-Path $owned 'source.zip'
        git archive --format=zip --output=$archive HEAD
        if($LASTEXITCODE){throw 'Git source export failed'}
        Expand-Archive -LiteralPath (Join-Path $owned 'source.zip') -DestinationPath $source
        foreach($file in $files){$target=Join-Path $source $file;[IO.Directory]::CreateDirectory((Split-Path $target -Parent))|Out-Null;Copy-Item -LiteralPath (Join-Path $repo $file) -Destination $target}
        $identity|Set-Content -Encoding utf8 -LiteralPath $marker
    }
    $environment=@{};Get-ChildItem Env:|Where-Object{$_.Name -match '^(FNB_|COMPOSE_)'}|ForEach-Object{$environment[$_.Name]=$_.Value;Remove-Item -LiteralPath ('Env:'+$_.Name)}
    if(-not $ResumeFresh){$result=@{status='RUNNING';source='Git archive + explicit DX files';stages=@{}}}
    try{
        $child=Join-Path $source 'scripts/Invoke-DuckDbPilot.ps1'
        $childOutput=Join-Path $source ('.local/duckdb-final/'+$RunName+'/warehouse')
        if($ResumeFresh -and $result.stages.FIRST_SETUP -eq 'PASS'){
            $completed=Get-Content -Raw -LiteralPath (Join-Path (Split-Path $childOutput -Parent) 'verification.json')|ConvertFrom-Json -AsHashtable
            if($completed.status -ne 'PASS'){throw 'Completed first setup evidence is required to resume its remaining verification'}
            $bundle=$completed.snapshot.bundle_id
            $manifestPath=Join-Path $childOutput ('releases/'+$bundle+'/manifest.json')
            if((Get-FileHash -Algorithm SHA256 -LiteralPath $manifestPath).Hash.ToLowerInvariant() -ne $completed.snapshot.source_manifest_sha256){throw 'First setup manifest SHA256 changed; resume rejected'}
            $before=Logical-Tables $childOutput $bundle
        }
        foreach($stage in @('FIRST_SETUP','STOP','RERUN_SETUP','RESTART')){
            if($ResumeFresh -and $result.stages[$stage] -eq 'PASS'){Write-Host "$stage : verified frozen-source checkpoint retained";continue}
            $result.stages[$stage]='RUNNING';$result|ConvertTo-Json -Depth 6|Set-Content (Join-Path $owned 'verification.json')
            Write-Host "$stage : fresh source; full log $(Join-Path $owned ($stage+'.log'))"
            $next=if($stage -eq 'STOP'){'Stop'}elseif($stage -eq 'RESTART'){'Restart'}else{'Setup'}
            # Isolate process environment as well as files/volumes. Older frozen
            # exports with the null/empty restoration bug can resume unchanged.
            $pwsh=(Get-Command pwsh -CommandType Application -ErrorAction Stop|Select-Object -First 1).Source
            & $pwsh -NoProfile -File $child -Mode Demo -RunName $RunName -Port $Port -PostgresPort $PostgresPort -DocsPort $DocsPort -Action $next *> (Join-Path $owned ($stage+'.log'))
            if($LASTEXITCODE -ne 0){throw "Fresh stage $stage failed; inspect $(Join-Path $owned ($stage+'.log'))"}
            if($stage -eq 'STOP'){
                $running=@(Invoke-DockerCommand @('ps','--filter',('label=com.docker.compose.project=fnb-duckdb-'+$RunName),'-q'))
                if($running.Count){throw 'Fresh Stop left a BI container running'}
            }
            if($stage -eq 'FIRST_SETUP'){$before=Logical-Tables (Join-Path $source ('.local/duckdb-final/'+$RunName+'/warehouse'))}
            if($stage -eq 'RERUN_SETUP' -and $before -ne (Logical-Tables (Join-Path $source ('.local/duckdb-final/'+$RunName+'/warehouse')))){throw 'Fresh rerun changed logical warehouse content'}
            $result.stages[$stage]='PASS'
        }
        $result.status='PASS';$result.linux_host='NOT_VERIFIED'
        Write-Host "PASS: fresh source setup/rerun/restart; evidence $owned"
    }catch{$result.status='FAIL';$result.stages[$stage]='FAIL';$result.error=$_.Exception.Message;throw}
    finally{$result|ConvertTo-Json -Depth 6|Set-Content (Join-Path $owned 'verification.json');Restore-Environment $environment;Set-Location -LiteralPath $repo}
    return
}

$envFile=Join-Path $repo '.env'
if(-not (Test-Path -LiteralPath $envFile)){Copy-Item -LiteralPath (Join-Path $repo '.env.example') -Destination $envFile;Write-Host 'Created .env from .env.example; existing credentials are never overwritten.'}
$settings=Read-Settings $envFile
function Setting([string]$Name,[string]$Default=''){$value=[Environment]::GetEnvironmentVariable($Name);if($null -ne $value){return $value};if($settings.ContainsKey($Name)){return $settings[$Name]};return $Default}
if(-not $Port){$Port=[int](Setting 'FNB_SUPERSET_PORT' '18101')}
if(-not $PostgresPort){$PostgresPort=[int](Setting 'FNB_POSTGRES_PORT' '15501')}
if(-not $DocsPort){$DocsPort=[int](Setting 'FNB_DBT_DOCS_PORT' '18102')}
if(-not $AirflowPort){$AirflowPort=[int](Setting 'FNB_AIRFLOW_PORT' '18103')}
if(-not $InputVolume){$InputVolume=Setting 'FNB_DEMO_VOLUME' 'fnb-platform_demo_outputs'}
if(-not $GoldRelease){$GoldRelease=Setting 'FNB_GOLD_RELEASE'}
if(-not $GoldManifestSha){$GoldManifestSha=Setting 'FNB_GOLD_MANIFEST_SHA256'}
if(-not $AnalyticsRelease){$AnalyticsRelease=Setting 'FNB_ANALYTICS_RELEASE'}
if(-not $AnalyticsManifestSha){$AnalyticsManifestSha=Setting 'FNB_ANALYTICS_MANIFEST_SHA256'}
if(-not $DemographicsRelease){$DemographicsRelease=Setting 'FNB_DEMOGRAPHICS_RELEASE'}
if(-not $DemographicsManifestSha){$DemographicsManifestSha=Setting 'FNB_DEMOGRAPHICS_MANIFEST_SHA256'}
if($Mode -eq 'Both'){
    if($RunName.Length -gt 25){throw 'Both mode needs a run name of at most 25 characters'}
    & $PSCommandPath -Mode Demo -RunName ($RunName+'-demo') -Port ($Port+1) -PostgresPort ($PostgresPort+2) -DocsPort ($DocsPort+2)
    & $PSCommandPath -Mode Real -RunName $RunName -InputVolume $InputVolume -WithRuntime:$WithRuntime -Port $Port -PostgresPort $PostgresPort -DocsPort $DocsPort -AirflowPort $AirflowPort -GoldRelease $GoldRelease -GoldManifestSha $GoldManifestSha -AnalyticsRelease $AnalyticsRelease -AnalyticsManifestSha $AnalyticsManifestSha -DemographicsRelease $DemographicsRelease -DemographicsManifestSha $DemographicsManifestSha
    return
}
if($Mode -eq 'Real' -and -not $Docs -and $Action -eq 'Setup' -and @($GoldRelease,$GoldManifestSha,$AnalyticsRelease,$AnalyticsManifestSha,$DemographicsRelease,$DemographicsManifestSha).Where({-not $_}).Count){throw 'Real mode requires six explicit release paths/SHA256 values in .env or named parameters; see .env.example. Demo never substitutes for missing Real data.'}
$run=Join-Path $repo ('.local/duckdb-final/'+$RunName);$output=Join-Path $run 'warehouse';$secrets=Join-Path $run 'secrets'
foreach($folder in @($run,$output,$secrets)){[IO.Directory]::CreateDirectory($folder)|Out-Null}
$project='fnb-duckdb-'+$RunName
$compose=@('compose','--project-name',$project,'--project-directory',$repo,'--env-file',$envFile,'-f','compose.analytics.yaml')
$names=@('FNB_ANALYTICS_OUTPUT','FNB_VERIFICATION_OUTPUT','FNB_SECRETS_ROOT','FNB_POSTGRES_PORT','FNB_SUPERSET_PORT','FNB_DBT_DOCS_PORT','FNB_DEMO_VOLUME','FNB_DBT_DOCS_PROJECT','FNB_IMAGE_PREFIX','FNB_AIRFLOW_PORT','FNB_ALLOW_LIVE_CRAWL','FNB_ALLOW_DOWNLOADS','FNB_BUSINESS_INPUT_ROOT','FNB_DEMOGRAPHICS_INPUT_ROOT','FNB_AREA_RELEASES_ROOT','FNB_AREA_DBT_RELEASE','FNB_INPUT_ROOT')
$prior=@{};foreach($name in $names){$prior[$name]=[Environment]::GetEnvironmentVariable($name)}
$checkpoint=Join-Path $run 'checkpoint.json';$state=@{status='RUNNING';mode=$Mode;stages=@{}};$lock=$null;$checkpointAccepted=$false
try{
    foreach($name in $names){if($settings.ContainsKey($name) -and $null -eq $prior[$name]){[Environment]::SetEnvironmentVariable($name,$settings[$name])}}
    $env:FNB_ANALYTICS_OUTPUT=$output;$env:FNB_VERIFICATION_OUTPUT=$run;$env:FNB_SECRETS_ROOT=$secrets
    $env:FNB_POSTGRES_PORT=[string]$PostgresPort;$env:FNB_SUPERSET_PORT=[string]$Port;$env:FNB_DBT_DOCS_PORT=[string]$DocsPort;$env:FNB_AIRFLOW_PORT=[string]$AirflowPort
    $env:FNB_DBT_DOCS_PROJECT='dimensional_analytics';$env:FNB_IMAGE_PREFIX=$project
    $env:FNB_DEMO_VOLUME=if($Mode -eq 'Real'){$InputVolume}else{$project+'_demo_outputs'}
    $env:FNB_ALLOW_LIVE_CRAWL='0';$env:FNB_ALLOW_DOWNLOADS='0'
    $lock=[IO.File]::Open((Join-Path $run '.runner.lock'),[IO.FileMode]::OpenOrCreate,[IO.FileAccess]::ReadWrite,[IO.FileShare]::None)
    $code=@('compose.analytics.yaml','docker/analytics/Dockerfile','docker/analytics/serving.sql','docker/analytics/configure_superset.py','docker/analytics/superset_config.py','docker/analytics/market_overview.zip','scripts/Invoke-DuckDbPilot.ps1','scripts/dbt_docs.py','scripts/verify_superset.py','scripts/bootstrap_analytics.py','scripts/init_analytics_secrets.py')
    $code+=@(Get-ChildItem src/fnb_platform/analytics,pipelines/dimensional_analytics,pipelines/business_analytics -Recurse -File|Where-Object{$_.Extension -in '.py','.sql','.yml'}|ForEach-Object{$_.FullName})
    if($WithRuntime){$code+=@('compose.yaml','compose.host.yaml','docker/runtime/Dockerfile','docker/runtime/airflow_entry.py','docker/runtime/smoke.py','docker/postgres/init.sh','docker/postgres/migrations/001_crawler.sql','requirements/crawl-state.txt');$code+=@(Get-ChildItem src/fnb_platform,crawl_experiment,dags -Recurse -File|Where-Object{$_.Extension -eq '.py'}|ForEach-Object{$_.FullName})}
    $identity=[ordered]@{mode=$Mode;runtime=[bool]$WithRuntime;volume=$InputVolume;input_root=$env:FNB_INPUT_ROOT;business_root=$env:FNB_BUSINESS_INPUT_ROOT;demographics_root=$env:FNB_DEMOGRAPHICS_INPUT_ROOT;inputs=@($GoldRelease,$GoldManifestSha,$AnalyticsRelease,$AnalyticsManifestSha,$DemographicsRelease,$DemographicsManifestSha);ports=@($Port,$PostgresPort,$DocsPort,$AirflowPort);hashes=@($code|Sort-Object -Unique|ForEach-Object{(Get-FileHash -LiteralPath $_ -Algorithm SHA256).Hash})}|ConvertTo-Json -Depth 5 -Compress
    $signature=[Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($identity)))
    if(Test-Path -LiteralPath $checkpoint){$loaded=Get-Content -Raw -LiteralPath $checkpoint|ConvertFrom-Json -AsHashtable;if(-not $Docs -and $Action -ne 'Stop' -and $loaded.signature -ne $signature){throw 'Code/options changed; preserve this run and choose a new -RunName'};$state=$loaded}
    try{Invoke-DockerCommand @('info','--format','{{.ServerVersion}}')}catch{throw 'Docker is unavailable. Start Docker Desktop with Linux containers, then rerun this command.'}
    Invoke-DockerCommand @('compose','version')
    if($Action -eq 'Stop'){Invoke-DockerCommand ($compose+@('stop','superset','dbt-docs','postgres'));Write-Host 'Stopped this BI project; all volumes, credentials and warehouse files retained.';return}
    Assert-Ports @{superset=$Port;postgres=$PostgresPort;'dbt-docs'=$DocsPort}
    if($Docs){$env:FNB_DBT_DOCS_PROJECT=$Docs;Invoke-DockerCommand ($compose+@('up','-d','--no-deps','--no-build','--force-recreate','--wait','--wait-timeout','180','dbt-docs'));Write-Host "dbt Docs: http://127.0.0.1:$DocsPort";return}
    if($Action -eq 'Restart'){
        if($state.status -ne 'PASS'){throw 'Restart requires a completed setup; rerun Setup to recover'}
        Invoke-DockerCommand ($compose+@('stop','superset','dbt-docs','postgres'))
        Invoke-DockerCommand ($compose+@('up','-d','--no-deps','--no-build','--wait','--wait-timeout','120','postgres'))
        Invoke-DockerCommand ($compose+@('up','-d','--no-deps','--no-build','--wait','--wait-timeout','180','superset','dbt-docs'))
        Verify-Browser 'browser-after-restart';Write-Host "Restart verified: http://127.0.0.1:$Port";return
    }
    $checkpointAccepted=$true;$state.signature=$signature;$state.input_volume=$InputVolume;$state.status='RUNNING';Save-State
    Step 'CONFIG' {
        Invoke-DockerCommand ($compose+@('config','--quiet'))
        $writers=@(Invoke-DockerCommand ($compose+@('ps','--status','running','-q','analytics-build')))
        if($writers.Count){throw 'An owned warehouse writer is running; preserve its checkpoint before retrying'}
        $volumes=@(Invoke-DockerCommand @('volume','ls','--filter',"label=com.docker.compose.project=$project",'--filter','label=com.docker.compose.volume=postgres_data','-q'))
        if($volumes.Count){
            $missing=@('admin_password','superset_metadata_password','superset_secret_key','superset_admin_password')|Where-Object{-not(Test-Path -LiteralPath (Join-Path $secrets $_))}
            if($missing){throw ('Existing metadata volume has missing credentials: '+($missing -join ', ')+'. Restore the original secret files; existing metadata will not be reset.')}
        }
    } -Always
    Step 'BUILD_IMAGES' {Invoke-DockerCommand ($compose+@('build','analytics-build','superset-init','bi-verify'))}
    Step 'SECRETS' {if(-not $IsWindows){[IO.File]::SetUnixFileMode($secrets,[IO.UnixFileMode]::UserRead -bor [IO.UnixFileMode]::UserWrite -bor [IO.UnixFileMode]::UserExecute)};Invoke-DockerCommand @('run','--rm','--network','none','--user','0:0','--mount',("type=bind,source=$secrets,target=/secrets"),'--entrypoint','python',($project+'/analytics:1.0.0'),'-B','/app/scripts/init_analytics_secrets.py','--directory','/secrets','--compose-secrets');if($WithRuntime){& ./scripts/Initialize-FnbRuntime.ps1 -SecretsDirectory $secrets}} -Always
    Step 'FOCUSED_TESTS' {Invoke-DockerCommand ($compose+@('run','--rm','--no-deps','--entrypoint','python','analytics-build','-B','-m','pytest','-q','/app/tests/test_dimensional_analytics.py','/app/tests/test_business_mvp.py','/app/tests/test_analytics_demographics.py','/app/tests/test_superset_bootstrap.py','/app/tests/test_superset_browser.py','/app/tests/test_duckdb_serving.py','--basetemp','/analytics-out/verification-fixtures','-o','cache_dir=/analytics-out/pytest-cache'))}
    $build=@('--demo','--serve')
    if($Mode -eq 'Real'){$build=@('--gold',$GoldRelease,'--gold-sha',$GoldManifestSha,'--analytics',$AnalyticsRelease,'--analytics-sha',$AnalyticsManifestSha,'--demographics',$DemographicsRelease,'--demographics-sha',$DemographicsManifestSha,'--serve');Step 'REAL_INPUT_ADMISSION' {Invoke-DockerCommand ($compose+@('run','--rm','--no-deps','analytics-build')+$build[0..($build.Length-2)]+@('--check-only'))} -Always}
    Step 'DBT_SNAPSHOT_ONE' {Invoke-DockerCommand ($compose+@('run','--rm','--no-deps','analytics-build')+$build)}
    $first=Get-Content -Raw -LiteralPath (Join-Path $output 'serving-current.json')|ConvertFrom-Json -AsHashtable
    $logical=Logical-Tables $output
    Step 'METADATA_POSTGRES' {Invoke-DockerCommand ($compose+@('up','-d','--no-deps','--no-build','--wait','--wait-timeout','120','postgres'));Invoke-DockerCommand ($compose+@('run','--rm','--no-deps','analytics-init'))} -Always
    Step 'SUPERSET_IMPORT' {Invoke-DockerCommand ($compose+@('run','--rm','--no-deps','superset-init'))}
    Step 'SUPERSET_SERVICE' {Invoke-DockerCommand ($compose+@('up','-d','--no-deps','--no-build','--wait','--wait-timeout','180','superset'))} -Always
    Step 'BROWSER_ONE' {Verify-Browser 'browser-one'} -Always
    Step 'CONCURRENT_REFRESH' {
        $job=Start-Job -ArgumentList @($compose,$build,(Join-Path $run 'refresh-two.log'),$repo) -ScriptBlock {param($prefix,$buildArgs,$log,$cwd);Set-Location -LiteralPath $cwd;& docker @prefix run --rm --no-deps analytics-build @buildArgs *> $log;if($LASTEXITCODE){throw 'Second DuckDB build failed; inspect refresh-two.log'}}
        try{Verify-Browser 'browser-during-refresh'}finally{if(-not (Wait-Job -Job $job -Timeout 900)){throw 'Refresh timeout; retained job/container/partial artifacts'};Receive-Job -Job $job -ErrorAction Stop;Remove-Job -Job $job}
        if($logical -ne (Logical-Tables $output)){throw 'Refresh changed logical warehouse table content'}
        Invoke-DockerCommand ($compose+@('run','--rm','--no-deps','--entrypoint','python','superset-init','-B','/workspace/docker/analytics/configure_superset.py'))
        Verify-Browser 'browser-after-refresh'
    } -Always
    Step 'DBT_DOCS' {Invoke-DockerCommand ($compose+@('up','-d','--no-deps','--no-build','--wait','--wait-timeout','180','dbt-docs'));$manifest=Invoke-RestMethod "http://127.0.0.1:$DocsPort/manifest.json";if(@($manifest.nodes.PSObject.Properties|Where-Object{$_.Name -like 'model.*'}).Count -ne 7){throw 'Dimensional Docs must expose seven models'};$null=Invoke-WebRequest "http://127.0.0.1:$DocsPort/"} -Always
    if($WithRuntime){
        Assert-Ports @{'airflow-webserver'=$AirflowPort;postgres=($PostgresPort+1)} ($project+'-runtime')
        $env:FNB_POSTGRES_PORT=[string]($PostgresPort+1)
        $runtime=@('compose','--project-name',($project+'-runtime'),'--project-directory',$repo,'--env-file',$envFile,'-f','compose.yaml','-f','compose.host.yaml')
        Step 'RUNTIME_IMAGES' {Invoke-DockerCommand ($runtime+@('build','pipeline','dbt','crawler','airflow-init'))}
        Step 'RUNTIME_SMOKE' {Invoke-DockerCommand ($runtime+@('run','--rm','--no-deps','pipeline','python','docker/runtime/smoke.py','pipeline'));Invoke-DockerCommand ($runtime+@('run','--rm','--no-deps','crawler'))}
        Step 'AIRFLOW_UI' {Invoke-DockerCommand ($runtime+@('up','-d','--no-build','--wait','--wait-timeout','240','airflow-webserver','airflow-scheduler'));Invoke-DockerCommand ($runtime+@('exec','-T','airflow-scheduler','python','/opt/airflow/dags/verify_orchestration.py'));Invoke-DockerCommand ($runtime+@('exec','-T','airflow-scheduler','python','-c',"from airflow.models import DagBag;from airflow.serialization.serialized_objects import SerializedDAG;b=DagBag('/opt/airflow/dags',include_examples=False);assert not b.import_errors,b.import_errors;assert len(b.dags)==3;[SerializedDAG.to_dict(d) for d in b.dags.values()];print('THREE_DAGS_IMPORT_SERIALIZE_PASS')"));$null=Invoke-WebRequest "http://127.0.0.1:$AirflowPort/health"}
        $env:FNB_POSTGRES_PORT=[string]$PostgresPort
    }
    Step 'RESOURCE_SNAPSHOT' {$ids=@(& docker @compose ps -q postgres superset);& docker stats --no-stream --format '{{json .}}' @ids|Set-Content -Encoding utf8 -LiteralPath (Join-Path $run 'resources.jsonl');if($LASTEXITCODE){throw 'Resource sampling failed'}} -Always
    Step 'QUERY_TIMINGS' {Invoke-DockerCommand ($compose+@('run','--rm','--no-deps','--entrypoint','python','analytics-build','-B','-m','fnb_platform.analytics.duckdb_serving','--output','/analytics-out','--probe'))} -Always
    $state.status='PASS';Save-State
    @{status='PASS';mode=$Mode;project=$project;snapshot=$first;duckdb_refresh_twice='PASS';dashboard='20 charts / 3 tabs';browser_during_after_refresh='PASS';dbt_docs="http://127.0.0.1:$DocsPort";superset="http://127.0.0.1:$Port/superset/dashboard/hcmc-fnb-market-overview/";airflow=$(if($WithRuntime){"http://127.0.0.1:$AirflowPort"}else{'NOT_ENABLED'});analytics_warehouse='DuckDB';host_python_required=$false}|ConvertTo-Json -Depth 16|Set-Content -Encoding utf8 -LiteralPath (Join-Path $run 'verification.json')
    Write-Host "PASS: http://127.0.0.1:$Port/superset/dashboard/hcmc-fnb-market-overview/"
    Write-Host "dbt Docs: http://127.0.0.1:$DocsPort ; Airflow: $(if($WithRuntime){'http://127.0.0.1:'+$AirflowPort}else{'NOT_ENABLED'})"
    Write-Host "Username: portfolio_admin ; password file: $(Join-Path $secrets 'superset_admin_password')"
    Write-Host "Evidence: $run"
}catch{
    if($checkpointAccepted){$state.status='FAIL';Save-State;@{status='FAIL';phase=$state.phase;error=$_.Exception.Message}|ConvertTo-Json|Set-Content -Encoding utf8 -LiteralPath (Join-Path $run 'verification.json')}
    throw
}finally{if($lock){$lock.Dispose()};Restore-Environment $prior}
