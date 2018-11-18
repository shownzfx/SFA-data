select count(distinct(agencyid))
from import.energy_consumption
where mode = "MB"