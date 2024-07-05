@echo off
echo=

echo build
go build -o %cd%\cmd\ipfs\dist\ipfs.exe .\cmd\ipfs

echo done