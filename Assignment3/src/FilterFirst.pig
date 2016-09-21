REGISTER file:/home/hadoop/lib/pig/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

set default_parallel 10;

ORDFlights = LOAD '$INPUT' using CSVLoader(); 

JFKFlights = LOAD '$INPUT' using CSVLoader();

ORDFlights_FilterColumns = FOREACH ORDFlights GENERATE $5 AS FlightDate, $11 AS Origin, $17 AS Destination, $24 AS DepartureTime, $35 AS ArrivalTime, $37 AS ArrivalDelayMins, $41 AS Cancelled, $43 AS Diverted;

ORDFlights_OriginFilter = FILTER ORDFlights_FilterColumns BY (Origin eq 'ORD' AND Destination neq 'JFK');

Valid_ORDFlights = FILTER ORDFlights_OriginFilter BY (Cancelled eq '0.00' AND Diverted eq '0.00');

ORDFlights_DateFilter = FILTER Valid_ORDFlights BY (ToDate(FlightDate, 'yyyy-MM-dd') > ToDate('2007-05-31', 'yyyy-MM-dd')) AND (ToDate(FlightDate, 'yyyy-MM-dd') < ToDate('2008-06-01', 'yyyy-MM-dd')); 

JFKFlights_FilterColumns = FOREACH JFKFlights GENERATE $5 AS FlightDate, $11 AS Origin, $17 AS Destination, $24 AS DepartureTime, $35 AS ArrivalTime, $37 AS ArrivalDelayMins, $41 AS Cancelled, $43 AS Diverted;

JFKFlights_ArrivalFilter = FILTER JFKFlights_FilterColumns BY (Origin neq 'ORD' AND Destination eq 'JFK');

Valid_JFKFlights = FILTER JFKFlights_ArrivalFilter BY (Cancelled eq '0.00' AND Diverted eq '0.00');

JFKFlights_DateFilter = FILTER Valid_JFKFlights BY (ToDate(FlightDate, 'yyyy-MM-dd') > ToDate('2007-05-31', 'yyyy-MM-dd')) AND (ToDate(FlightDate, 'yyyy-MM-dd') < ToDate('2008-06-01', 'yyyy-MM-dd'));

Flights_Join = JOIN ORDFlights_DateFilter BY (FlightDate, Destination), JFKFlights_DateFilter BY (FlightDate, Origin);

TwoLeggedFlights = FILTER Flights_Join BY JFKFlights_DateFilter::DepartureTime > ORDFlights_DateFilter::ArrivalTime;

TotalDelay = FOREACH TwoLeggedFlights GENERATE (FLOAT)(JFKFlights_DateFilter::ArrivalDelayMins + ORDFlights_DateFilter::ArrivalDelayMins) AS TotalFlightDelay;

TotalDelay2 = GROUP TotalDelay ALL;

AVERAG = FOREACH TotalDelay2 GENERATE AVG(TotalDelay);

DUMP AVERAG;

