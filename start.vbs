' Telegram Scraper - Silent Launcher
' This script starts the app without showing a command window

Set WshShell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

' Get the directory where this script is located
scriptPath = fso.GetParentFolderName(WScript.ScriptFullName)

' Change to the script directory and run the batch file hidden
WshShell.CurrentDirectory = scriptPath
WshShell.Run "cmd /c start.bat", 0, False
