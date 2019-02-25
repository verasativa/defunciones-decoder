# Base de datos de defunciones Chile 1998-2016
### Base de datos
Este dataset contiente el total de las defunciones registradas por el [deis](http://www.deis.cl/bases-de-datos-defunciones/) en Chile desde 1998 a 2016. 
#### Archivos
En el [data set final](https://www.floydhub.com/veras/datasets/defunciones) encontrarás los siguientes archivos:

 - ```defunciones-deis-1998-2016-parsed-1M.csv``` archivo principal que contiene los datos de defunciones en Chile desde 1998 a 2016
 - ```dtypes.json``` mapa del tipo de campos del archivo anterior (no es necesario, pero si trabajas en pandas, te ahorrará el trabajo de convertirlos con el código de ejemplo)
 - ```cie-10.csv``` mapa jerarquico de códigos de diagnosticos cie-10 (no es necesario, pero te permite convertir de códigos a descripciones los diagnosticos, y navegar su agrupación jerarquica)
 - ```ejemplo.ipynb``` jupyter notebook que ejemplifica la carga y analisís de los datos en pandas

#### Ejemplo de carga en pandas (Python)
```Python
# Load defunciones
%matplotlib inline
import pandas as pd
import json

# Dtypes
# Load dtypes from json
with open('dtypes.json') as json_data:
    read_dtypes = json.load(json_data)
date_fields = []

# Capture datetime fields
for col in read_dtypes:
    if read_dtypes[col] == 'datetime64[ns]':
        date_fields.append(col)
        
# Remove datetime fields
for field in date_fields:
    del read_dtypes[field]
    
defunciones = pd.read_csv('defunciones-deis-1998-2016-parsed-1M.csv', dtype=read_dtypes, parse_dates=date_fields,index_col=0)
```

### Origen y porqué
Los datos originales provienen del [deis](http://www.deis.cl/bases-de-datos-defunciones/) y fueron argumentados con [códigos CIE-10 jerarquizados](https://github.com/verasativa/CIE-10). Estando los originales en distintos formatos, codificaciones y columnas, dificultaba analisís generales como el que se muestra en el ejemplo.

### Pendientes / known issues
 - Valores numericos con 9 o 99 que en realidad son nulos, y deberían ser agregados como tales a los [códigos de columnas](https://www.floydhub.com/veras/datasets/defunciones-deis/5/_ref/columns_codes.csv)
 - Generar / revisar mapa de servicios de salud en distintas fechas
 
### App
Los datos crudos de deis fueron organizados en un [dataset en floydhub](https://www.floydhub.com/veras/datasets/defunciones-deis/), el que se argumentó con mapas de:
 - [Códigos CIE-10](https://github.com/verasativa/CIE-10)
 - [Códigos de comunas hitoricos del deis](https://www.floydhub.com/veras/datasets/defunciones-deis/5/_ref/Divisio%CC%81n-Poli%CC%81tico-Administrativa-y-Servicios-de-Salud-Histo%CC%81rico.xls)
 - [Códigos de columnas](https://www.floydhub.com/veras/datasets/defunciones-deis/5/_ref/columns_codes.csv) a partir del [equesma de registro de deis](https://www.floydhub.com/veras/datasets/defunciones-deis/5/_ref/EsquemaRegistroDefunciones.pdf)
 
A razón de que desde 1998 se clasifica en códigos CIE-10, se decidió concatenar desde esa fecha para evitar los problemas de concatenar 2 bases de códificación distintas.
 
#### Requerimientos
Para completar los requerimientos de software debes ejecutar ```./setup.sh``` y fue ejecutado en una maquina con 32GB en ram, llegó al 60% de uso exportando a csv.

#### Pull request / contrib
Si mejoras algo, código o documentación, porfavor no dudes en enviar un pull request y feliz lo incorporamos.
### Agradecimientos
A Naren y Alessio de [floydhub](https://www.floydhub.com/) por donar 25hrs de servidores CPU2. 