MATCH (p:Person{id: 2199023255557}), (f:Person{})
    where not p.id = f.id
    WITH p, f
    MATCH path = (p)-[e:Person_knows_Person*1..3]->(f)
    WITH min(length(e)) as distance, f
ORDER BY
    distance ASC,
    f.lastName ASC,
    f.id ASC
LIMIT 20
MATCH (f)-[:Person_isLocatedIn_Place]->(fCity{type: "City"})
OPTIONAL MATCH (f)-[:Person_studyAt_Organisation]-(uni:Organisation{type: "University"})-[:Organisation_isLocatedIn_Place]->(uniCity:Place{type: "City"})
WITH f, collect(uniCity) AS unis
OPTIONAL MATCH (f)-[:Person_workAt_Organisation]->(company:Organisation{type: "Company"})-[:Organisation_isLocatedIn_Place]->(companyCountry:Place{type: "Country"})
WITH f, collect(company) as comps
return f, comps

