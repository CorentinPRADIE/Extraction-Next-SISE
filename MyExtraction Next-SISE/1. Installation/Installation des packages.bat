@echo off
REM Ajoute les chemins au PATH de l'utilisateur actuel de manière permanente
setx PATH "%PATH%;C:\Users\ADSL\Desktop\Python;C:\Users\ADSL\Desktop\Python\Scripts"

echo Installation des packages Python...
pip install pandas streamlit dask
echo Les chemins ont été ajoutés au PATH de l'utilisateur et les packages ont été installés.
echo Fin de l'installation. Appuyez sur une touche pour fermer.
pause > nul
