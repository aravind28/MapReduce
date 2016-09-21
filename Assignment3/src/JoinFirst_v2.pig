REGISTER file:/home/hadoop/lib/pig/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

set default_parallel 10;

ORDFlights_v2 = LOAD '$INPUT' using CSVLoader();

JFKFlights_v2 = LOAD '$INPUT' using CSVLoader();

ORDFlights_FilterColumns = FOREACH ORDFlights_v2 GENERATE $5 AS FlightDate, $11 AS Origin, $17 AS Destination, $24 AS DepartureTime, $35 AS ArrivalTime, $37 AS ArrivalDelayMins, $41 AS Cancelled, $43 AS Diverted;

ORDFlights_OriginFilter = FILTER ORDFlights_FilterColumns BY (Origin eq 'ORD' AND Destination neq 'JFK');

Valid_ORDFlights = FILTER ORDFlights_OriginFilter BY (Cancelled eq '0.00' AND Diverted eq '0.00');

JFKFlights_FilterColumns = FOREACH JFKFlights_v2 GENERATE $5 AS FlightDate, $11 AS Origin, $17 AS Destination, $24 AS DepartureTime, $35 AS ArrivalTime, $37 AS ArrivalDelayMins, $41 AS Cancelled, $43 AS Diverted;

JFKFlights_ArrivalFilter = FILTER JFKFlights_FilterColumns BY (Origin neq 'ORD' AND Destination eq 'JFK');

Valid_JFKFlights = FILTER JFKFlights_ArrivalFilter BY (Cancelled eq '0.00' AND Diverted eq '0.00');

Flights_Join = JOIN Valid_ORDFlights BY (FlightDate, Destination), Valid_JFKFlights BY (FlightDate, Origin);

TwoLeggedFlights = FILTER Flights_Join BY Valid_JFKFlights::DepartureTime > Valid_ORDFlights::ArrivalTime;

Flights_DateFilter = FILTER TwoLeggedFlights BY (ToDate(Valid_ORDFlights::FlightDate, 'yyyy-MM-dd') > ToDate('2007-05-31', 'yyyy-MM-dd')) AND (ToDate(Valid_ORDFlights::FlightDate, 'yyyy-MM-dd') < ToDate('2008-06-01', 'yyyy-MM-dd'));

TotalDelay = FOREACH Flights_DateFilter GENERATE (FLOAT)(Valid_ORDFlights::ArrivalDelayMins + Valid_JFKFlights::ArrivalDelayMins) AS TotalFlightDelay;

TotalDelay2 = GROUP TotalDelay ALL;

AVERAGE = FOREACH TotalDelay2 GENERATE AVG(TotalDelay);

DUMP AVERAGE;

