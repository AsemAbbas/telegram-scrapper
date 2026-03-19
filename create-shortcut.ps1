# Create Desktop Shortcut for Telegram Scraper
# Run this script once to create a shortcut on your desktop

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$desktopPath = [Environment]::GetFolderPath("Desktop")
$shortcutPath = Join-Path $desktopPath "Telegram Scraper.lnk"

# Create WScript.Shell COM object
$WshShell = New-Object -ComObject WScript.Shell

# Create shortcut
$Shortcut = $WshShell.CreateShortcut($shortcutPath)
$Shortcut.TargetPath = Join-Path $scriptDir "start.bat"
$Shortcut.WorkingDirectory = $scriptDir
$Shortcut.Description = "Telegram Scraper - Scrape Telegram channels to Google Sheets"
$Shortcut.WindowStyle = 1  # Normal window

# Try to use a system icon (satellite dish / network)
$Shortcut.IconLocation = "%SystemRoot%\System32\shell32.dll,13"

$Shortcut.Save()

Write-Host ""
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "  Desktop shortcut created successfully!" -ForegroundColor Green
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Location: $shortcutPath" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Double-click 'Telegram Scraper' on your desktop to start!" -ForegroundColor White
Write-Host ""

# Keep window open
Read-Host "Press Enter to close"
