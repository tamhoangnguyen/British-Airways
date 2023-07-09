-- CREATE VIEW TO TRUNCATE NULL DATA IN REVIEW_DATA TABLE
CREATE OR REPLACE VIEW rd (id,Type_Travel,Seat_Type,place_from,place_to)
AS
SELECT rd.index,rd.Type_Travel,rd.Seat_Type,rd.from,rd.to 
from review_data as rd 
WHERE rd.from is not null
AND ( rd.Type_Travel = 'Business' 
OR rd.Type_Travel = 'Couple Leisure' 
OR rd.Type_Travel = 'Family Leisure' 
OR rd.Type_Travel = 'Solo Leisure' );


-- CREATE A VIEW TO GET A TOTAL NUMBER OF review BY TYPE TRAVEL

CREATE OR REPLACE VIEW num_review_by_type_travel ( Type_Travel, Seat_Type,nums_review,nums_rank) 
AS
SELECT rd.Type_Travel, rd.Seat_Type, count(rd.id)  as num_review,
	rank() over( partition by rd.Type_Travel ORDER BY count(rd.id) DESC ) as rank_num
FROM rd
GROUP BY rd.Type_Travel , rd.Seat_Type 
ORDER BY rd.Type_Travel ASC  , rd.Seat_Type ASC;

-- CREATE A VIEW TO GET A TOTAL NUMBER OF review BY CITY
CREATE OR REPLACE VIEW num_review_by_city ( city,num_review_from, num_review_to )
AS 
SELECT tab1.place_from as place, tab1.num_review_from, tab2.num_review_to FROM
( 
	SELECT rd.place_from, count(rd.id) as num_review_from
	FROM rd
	GROUP BY rd.place_from
) as tab1
JOIN
(
	SELECT rd.place_to, count(rd.id) as num_review_to
	FROM rd
	GROUP BY rd.place_to
) as tab2
ON tab1.place_from = tab2.place_to
ORDER BY tab1.num_review_from DESC;

-- CREATE A VIEW TO GET A TOTAL NUMBER OF review BY ROUTE
CREATE OR REPLACE VIEW num_review_flight_route 
AS
WITH rd1
AS
(
	SELECT rd.id, CONCAT(rd.place_from, " to ", rd.place_to) as route
    FROM rd
)
SELECT route, count(rd1.id) as num_review 
FROM rd1
GROUP BY route
HAVING route is not null 
ORDER BY 2 DESC;

-- Create a view to get the total number of review in each year
CREATE OR REPLACE VIEW num_review_each_year ( year_flight,month_flight, nums_review)
AS
(
	SELECT df.year_flight,df.month_flight,count(df.index) as num_review
	FROM day_flight as df
	group by df.year_flight,df.month_flight
	ORDER BY df.year_flight,df.month_flight 
);


-- CREATE VIEW FIX THE HEADER COLUMN WITH THE CORRECT FORMAT
CREATE OR REPLACE VIEW cd
AS
(
	SELECT cd.index as id, replace(replace(header,'\\',"'"),".",',') as header ,
	cd.country,cd.name,cd.verify,cd.time,cd.rating,cd.processed_at
	FROM customer_data as cd
);
-- CREATE VIEW TO GET THE TOTAL NUMBER OF VERIFY AIRPLANES
CREATE OR REPLACE VIEW num_verify
AS
(
	SELECT cd.verify, count(id) as nums_verify
	FROM cd
	GROUP BY cd.verify
	ORDER BY nums_verify DESC
);

-- CREATE VIEW TO GET TOTAL NUMBER OF RATING
CREATE OR REPLACE VIEW num_rating
AS
SELECT rating,count(id) as nums_rating FROM cd
WHERE rating is not null
group by rating
ORDER BY nums_rating DESC;


-- CREATE VIEW TO GET TOTAL NUMBER review OF COUNTRY 
CREATE OR REPLACE VIEW num_country
AS
(
	SELECT country, count(id) as num_country FROM cd
	WHERE country is not null
	GROUP BY country
	ORDER BY num_country DESC
);

-- CREATE VIEW TO GET TOTAL FEEDBACK
CREATE OR REPLACE VIEW feedback
AS
WITH feedback 
AS
(
	SELECT *,  if(rating >= 5, 'Good','Bad') as feedback
	FROM customer_data
) SELECT feedback, count(fb.index) as id from feedback as fb
GROUP BY feedback
ORDER BY 2 DESC;

DROP TABLE if exists fact_table;
CREATE TABLE fact_table 
AS
SELECT * FROM 
(
	SELECT cd.index as id_header ,cd.index as id_name,cd.country,cd.verify,cd.rating,
		df.day_flight, df.month_flight,df.year_flight, rd.Type_Travel,rd.Seat_Type,rd.from as place_from,
		rd.to as place_to, cmd.index as comment_id
	FROM customer_data as cd
	LEFT JOIN day_flight as df ON cd.index = df.index
	LEFT JOIN review_data as rd ON cd.index = rd.index
	LEFT JOIN comment_data as cmd ON cd.index = cmd.index
) as new_table WHERE place_from is not null and place_to is not null;

SELECT * FROM num_review_each_year


