#Requires -Version 5.1
<#
.SYNOPSIS
  Build Mesh Command Post into a standalone Windows .exe using PyInstaller.
.EXAMPLE
  .\build.ps1
  .\build.ps1 -Clean
#>
param(
    [switch]$Clean
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$SrcDir    = Join-Path $ScriptDir "src"
$DistDir   = Join-Path $ScriptDir "dist"
$BuildDir  = Join-Path $ScriptDir "build"
$SpecFile  = Join-Path $ScriptDir "mesh_command_post.spec"

if ($Clean) {
    Write-Host "Cleaning previous build artifacts..." -ForegroundColor Yellow
    if (Test-Path $DistDir)  { Remove-Item $DistDir  -Recurse -Force }
    if (Test-Path $BuildDir) { Remove-Item $BuildDir -Recurse -Force }
    if (Test-Path $SpecFile) { Remove-Item $SpecFile -Force }
}

Write-Host "Building Mesh Command Post..." -ForegroundColor Cyan
Write-Host "Source: $SrcDir"

# Verify pyinstaller is available
$pyinstaller = (Get-Command pyinstaller -ErrorAction SilentlyContinue)
if (-not $pyinstaller) {
    Write-Error "pyinstaller not found. Run: pip install pyinstaller"
    exit 1
}

# Collect Qt WebEngine resources (needed for the map view)
# PyInstaller PySide6 hook should handle most of this automatically.

$Args = @(
    "--noconfirm",
    "--onefile",
    "--windowed",
    "--name", "MeshCommandPost",
    "--paths", $SrcDir,
    "--collect-all", "PySide6",
    "--hidden-import", "PySide6.QtWebEngineWidgets",
    "--hidden-import", "PySide6.QtWebEngineCore",
    "--hidden-import", "PySide6.QtWebChannel",
    "--hidden-import", "paho.mqtt.client",
    "--add-data", "$SrcDir;src",
    (Join-Path $SrcDir "main.py")
)

Write-Host "Running: pyinstaller $Args" -ForegroundColor Gray
& pyinstaller @Args

if ($LASTEXITCODE -ne 0) {
    Write-Error "PyInstaller failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

$Exe = Join-Path $DistDir "MeshCommandPost.exe"
if (Test-Path $Exe) {
    $Size = [math]::Round((Get-Item $Exe).Length / 1MB, 1)
    Write-Host ""
    Write-Host "Build complete!" -ForegroundColor Green
    Write-Host "  Output: $Exe ($Size MB)"
    Write-Host ""
    Write-Host "NOTE: The .exe requires internet access to load OpenStreetMap tiles in the Map tab."
} else {
    Write-Warning "Build may have succeeded but exe not found at expected location: $Exe"
    Write-Host "Check the dist/ directory."
}
