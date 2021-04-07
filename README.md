# Recommendation Engine

## DatabaseCopy
In deze directory staat een link naar een google drive. In deze
google drive staat een sql bestand met de gehele database die wij
gebruiken voor onze recommendation engine. Dit sql bestand kan
direct in PGAdmin in een database gezet worden met behulp van
de 'Restore' optie. Dit duurt aanzlienlijk korter (+/- 100 seconden)
dan het script runnen van het overzetten van de MongoDB data
naar de PGAdmin database (1+ uur).


## DatabaseGenerate
In deze directory staan de scripts die wij gebruikt hebben om
de MongoDB data over te zetten naar een relationele database
in PGAdmin. Ook staan hier de scripts die wij gebruikt hebben
om vanuit de relationele database zelf nieuwe data genereert
voor onze recommendation engine.


## GeneralModules
Deze directory bevat algemene modules die wij gemaakt hebben
om bepaalde koppelingen in overzichtelijke functies te zetten.
Op deze manier konden wij ze makkelijk herbruiken en is het
makkelijker voor de lezer te begrijpen wat bepaalde queries
doen.


## RecommendationEngine
In deze directory staat de front-end die wij verkregen hebben
van de HU met daarbij onze recommendation engine die de
recommendations op de front-end zet.


## Scrum