@echo off

echo Installation des packages Python...
pip install pandas streamlit dask[dataframe] XlsxWriter

echo.
echo.
echo.
echo Les packages suivants ont ete installes :
echo - pandas - Une bibliotheque offrant des structures de donnees et des outils d'analyse de donnees hautement performants. Documentation : https://pandas.pydata.org/
echo.
echo - streamlit - Permet de creer rapidement des applications web pour vos projets de donnees. Documentation : https://streamlit.io/
echo.
echo - dask - Fournit des structures paralleles avancees et des calculs distribues, permettant d'accelerer votre code. Documentation : https://dask.org/
echo.
echo - XlsxWriter - Une bibliotheque Python pour ecrire des fichiers Excel 2010 xlsx/xlsm. Documentation : https://xlsxwriter.readthedocs.io/
echo.
echo Fin de l'installation. Appuyez sur une touche pour fermer.
pause > nul
