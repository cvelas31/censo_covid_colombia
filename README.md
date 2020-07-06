# Data Engineer Project

## Project Summary
Relation COVID data with Census data for Colombia in order:
- To do some Analytics and identify vulnerable zones.
- To do estimates about mortality rates acoording to the zone
- To predict how the curve will evolve and identify pain zones
- To estimate UCI capacity in Bogota and how it is evolving.

## Datasets
### Colombia CENSUS 2018
Gather from the microdata of DANE (Statistics Colombian Institute) from main Department (States): 
- Antioquia
- Bogota
- Atlantico
- Meta
- Caldas

The data is obtained here: http://microdatos.dane.gov.co/index.php/catalog/643/get_microdata.

Each folder contains 5 geenral tables:
- VIVIENDAS: Information about physical homes like: Type of home, City, Number of people, Materials of the building, Public Servics, etc.
- HOGARES: Information about homes like: Number of rooms, kitchen available, water to cook, deceased in home, number of people, etc.
- FALLECIDOS: Deceased people info like: Number of people, Gender, Age of death, Has death medical certificate.
- PERSONAS: People information like: Gender, Age, Relation with home head, City of Birth, Home 5 years ago, Home 1 year ago, Health issues, Treatment, Dificulties, Alphabetism, Education Level, Work, Civil Status, Sons, Sons out of Colombia, etc.
- GEOREFERENCIA: Identifiers inside a Colombia. Depertment, City, Comuna, Urban area, Rural area, Neighborhood, Building number.

**Notes:**
- Rename Atlantico Folder: 08_AtlanticoCSV -> 08_Atlantico_CSV

### DIVIPOLA
División departamental y municipal de Colombia
Diccionario de codigo de municipios

### Colombia COVID-19 dataset
Gather from INS (Instituto Nacional de Salud - Natinal Instute of Health)
National Information about COVID 19 cases.

The data is obtained here: 
- https://www.datos.gov.co/Salud-y-Protecci-n-Social/Muestras-procesadas-de-COVID-19-en-Colombia/8835-5baf/data
- https://datosabiertos.bogota.gov.co/dataset/porcentaje-de-ocupacion-de-los-servicios-de-hospitalizacion-general-ucim-y-uci-en-bogota-d-c/resource/4911565b-4518-4931-93de-d117c10dbdce
- https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia/gt2j-8ykr/data

The information here corresponds to:
- COVID-19 samples proocessed by Department/Main City
- Bogota D.C UCI services
- COVIID-19 positive cases with age, city, recovered, severity, nexus, gender, death

## Scope of the project
The aim of this project is to combine the Census data and Covid 19 data:
- To identify vulnerable people.
    - Old people (>65 years old)
    - People with comorbidity (Asthma, Heart Diseases, etc)
    - People wthout access  to water or bad food conditions

- To identify zones where this vulnerable people are located to focus the health attention on those places and gather UCIs to that place

- To estimate changes on expected life with COVID-19.
- To have a clean and complete dataset with all information about census data and COVID-19 cases in Colombia.

## Exploring data
### Knowing the data
At first the data is displayed in jupyter in order to identify and clarify what a column means, some columns that will not be useful, and to clean the data according to nulls or similar things.

The exploration are found:
- Census data can be found at XXXXXX
- Covid-19 cases can be found at XXXXX




## Other Scenarios
- The data was increased by 100x.
- The pipelines would be run on a daily basis by 7 am every day.
- The database needed to be accessed by 100+ people.