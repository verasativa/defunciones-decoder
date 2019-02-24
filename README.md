# Base de datos de defunciones Chile 1998-2016
### Base de datos
Ejemplo de carga en pandas
### Origen y porqué
Los datos originales provienen del [deis](http://www.deis.cl/bases-de-datos-defunciones/) y fueron organizados en c
### App
Los datos crudos de deis fueron organizados en un [dataset en floydhub](https://www.floydhub.com/veras/datasets/defunciones-deis/), el que se argumentó con mapas de:
 - [Códigos CIE-10](https://github.com/verasativa/CIE-10)
 - [Códigos de comunas hitoricos del deis](https://www.floydhub.com/veras/datasets/defunciones-deis/5/_ref/Divisio%CC%81n-Poli%CC%81tico-Administrativa-y-Servicios-de-Salud-Histo%CC%81rico.xls)
 - [Códigos de columnas](https://www.floydhub.com/veras/datasets/defunciones-deis/5/_ref/columns_codes.csv) a partir del [equesma de regiostro de deis](https://www.floydhub.com/veras/datasets/defunciones-deis/5/_ref/EsquemaRegistroDefunciones.pdf)
 
A razón de que desde 1998 se clasifica en códigos CIE-10, se decidió concatenar desde esa fecha para evitar los problemas de concatenar 2 bases de códificación distintas.
 
#### Requerimientos
Para completar los requerimientos de software debes ejecutar ```./setup.sh``` y fue ejecutado en una maquina con 32GB en ram, llegó al 60% de uso exportan a csv, 