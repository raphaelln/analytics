# analytics

This repository will contain some data analytics scenarios resolved using spark framework.

# Requirements
- cloudera distribution VM

# Scenario

## Top 3 cities with more accidents in Brazil between 2003 and 2007

The dataset was retrieve by **Public Brazilian Social Security** and shows the number of related accidents in all the cities of Brazil.

There are five kinds of accidents:
- Typical work accidents;
- Typical work route accidents;
- Accident by disease;
- Accident without official communication through the autorities;
- Death caused by work accidents

### Insights
- Top 3 cities with max number of accidents by state between 2003 and 2007 in Brazil.

### Result
- Results will be stored as parquet compressed files.
- Results will be stored as json uncompressed files.

### Files
- /accidents/retrieve.sh - Download the files from dataprev site and put on the hdfs
- /accidents/solution.py - Solution implemented with (spark + spark Sql) using python.

### Sample output
```
+--------------------+----+--------------+-------------------+--------------+----------------+
|                city|  uf|2003_accidents|2004_2007_accidents|totalAccidents|increase_percent|
+--------------------+----+--------------+-------------------+--------------+----------------+
|        "RIO BRANCO"|"AC"|           243|               1549|          1792|          637.45|
|   "CRUZEIRO DO SUL"|"AC"|             7|                113|           120|         1614.29|
|  "SENADOR GUIOMARD"|"AC"|            38|                 51|            89|          134.21|
|            "MACEIO"|"AL"|          1084|               6924|          8008|          638.75|
|"SAO MIGUEL DOS C...|"AL"|           539|               3662|          4201|          679.41|
|          "CORURIPE"|"AL"|           120|               1996|          2116|         1663.33|
|            "MANAUS"|"AM"|          3195|              22720|         25915|          711.11|
|       "ITACOATIARA"|"AM"|           185|                710|           895|          383.78|
|"PRESIDENTE FIGUE...|"AM"|            54|                530|           584|          981.48|
|            "MACAPA"|"AP"|           187|               1109|          1296|          593.05|
|           "SANTANA"|"AP"|            25|                279|           304|          1116.0|
|"PEDRA BRANCA DO ...|"AP"|             1|                214|           215|         21400.0|
|          "SALVADOR"|"BA"|          4594|              23668|         28262|          515.19|
|          "CAMACARI"|"BA"|          1595|               6853|          8448|          429.66|
|          "JUAZEIRO"|"BA"|           925|               4175|          5100|          451.35|
|         "FORTALEZA"|"CE"|          2112|              12511|         14623|          592.38|
|         "MARACANAU"|"CE"|           358|               2264|          2622|           632.4|
|            "SOBRAL"|"CE"|           258|               1795|          2053|          695.74|
|          "BRASILIA"|"DF"|          4448|              25170|         29618|          565.87|
|           "VITORIA"|"ES"|          2062|              10184|         12246|          493.89|
|             "SERRA"|"ES"|          1191|               8227|          9418|          690.76|
|        "VILA VELHA"|"ES"|           942|               5603|          6545|           594.8|
|           "GOIANIA"|"GO"|          3471|              15329|         18800|          441.63|
|         "RIO VERDE"|"GO"|          1044|               3973|          5017|          380.56|
|"APARECIDA DE GOI...|"GO"|           620|               3097|          3717|          499.52|
|          "SAO LUIS"|"MA"|           709|               4444|          5153|           626.8|
|       "COELHO NETO"|"MA"|           356|               1531|          1887|          430.06|
|        "ACAILANDIA"|"MA"|           172|               1023|          1195|          594.77|
|    "BELO HORIZONTE"|"MG"|          8138|              40166|         48304|          493.56|
|          "CONTAGEM"|"MG"|          1981|              11623|         13604|          586.72|
|             "BETIM"|"MG"|          1774|              11541|         13315|          650.56|
|      "CAMPO GRANDE"|"MS"|          1888|              10007|         11895|          530.03|
|          "DOURADOS"|"MS"|           397|               2056|          2453|          517.88|
|       "TRES LAGOAS"|"MS"|           294|               1784|          2078|           606.8|
|            "CUIABA"|"MT"|          1058|               6385|          7443|           603.5|
|      "RONDONOPOLIS"|"MT"|           435|               2399|          2834|          551.49|
|     "VARZEA GRANDE"|"MT"|           354|               2301|          2655|           650.0|
|             "BELEM"|"PA"|          2585|              11500|         14085|          444.87|
|         "TAILANDIA"|"PA"|           126|               3714|          3840|         2947.62|
|        "ANANINDEUA"|"PA"|           589|               2675|          3264|          454.16|
|       "JOAO PESSOA"|"PB"|           668|               3348|          4016|           501.2|
|    "CAMPINA GRANDE"|"PB"|           363|               2903|          3266|          799.72|
|        "SANTA RITA"|"PB"|           260|               1420|          1680|          546.15|
|            "RECIFE"|"PE"|          3546|              17262|         20808|           486.8|
|"JABOATAO DOS GUA...|"PE"|           686|               3966|          4652|          578.13|
|           "IPOJUCA"|"PE"|           298|               2602|          2900|          873.15|
|          "TERESINA"|"PI"|           552|               3302|          3854|          598.19|
|             "UNIAO"|"PI"|            15|                190|           205|         1266.67|
|          "PARNAIBA"|"PI"|            20|                181|           201|           905.0|
|          "CURITIBA"|"PR"|          5706|              34196|         39902|           599.3|
+--------------------+----+--------------+-------------------+--------------+----------------+

```
