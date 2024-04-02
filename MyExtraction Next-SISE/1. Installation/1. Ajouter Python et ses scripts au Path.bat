@echo off
REM Ajoute les chemins au PATH de l'utilisateur actuel de manière permanente en utilisant le profil de l'utilisateur
setx PATH "%PATH%;%USERPROFILE%\Desktop\Python;%USERPROFILE%\Desktop\Python\Scripts"

echo Les chemins suivants ont ete ajoutes au PATH de l'utilisateur : 
echo - %USERPROFILE%\Desktop\Python
echo - %USERPROFILE%\Desktop\Python\Scripts
pause > nul
