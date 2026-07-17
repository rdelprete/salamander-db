param(
    [ValidateRange(1, 1000000)]
    [int]$Iterations = 1000,
    [string]$Bin
)

$ErrorActionPreference = "Stop"

if (-not $Bin) {
    $Bin = if ($IsWindows) {
        ".\target\release\salamander-demo.exe"
    } else {
        "./target/release/salamander-demo"
    }
}

$workdir = Join-Path ([System.IO.Path]::GetTempPath()) (
    "salamander-crash-" + [System.Guid]::NewGuid().ToString("N")
)
$scenarios = @("append", "batch", "snapshot", "heal", "retention")
$passed = 0
$failed = 0

New-Item -ItemType Directory -Path $workdir | Out-Null
try {
    for ($i = 1; $i -le $Iterations; $i++) {
        $scenario = $scenarios[($i - 1) % $scenarios.Count]
        $dir = Join-Path $workdir "run_$i"
        New-Item -ItemType Directory -Path $dir | Out-Null

        & $Bin crashtest parent $dir $scenario
        if ($LASTEXITCODE -eq 0) {
            $passed++
        } else {
            $failed++
            Write-Host "iteration $i ($scenario) FAILED at $dir"
        }
    }

    Write-Host (
        "$Iterations process crashes across $($scenarios -join ', '), " +
        "$failed violation(s), $passed clean"
    )
    if ($failed -ne 0) {
        exit 1
    }
} finally {
    if (Test-Path -LiteralPath $workdir) {
        Remove-Item -LiteralPath $workdir -Recurse -Force
    }
}
