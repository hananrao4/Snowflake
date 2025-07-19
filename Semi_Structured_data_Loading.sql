----------------- CREATE File Format--------------------

CREATE OR REPLACE  FILE FORMAT HRMS.ETL.Json_ETL_File_Format
TYPE=Json


----------------- CREARE Storage Integration----------------------

CREATE OR REPLACE STORAGE INTEGRATION GCP_INT
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = GCS
ENABLED = TRUE 
STORAGE_ALLOWED_LOCATIONS = ('gcs://snowflake_datatransfer/Source_folder/Json/')


------------------CREATE STAGE ------------------------------

CREATE OR REPLACE STAGE HRMS.ETL.GCP_ETL_Json_STAGE   --- Create stag to fetch data from any Cloud like (GCP,AWS etc)
STORAGE_INTEGRATION = GCP_INT
URL = 'gcs://snowflake_datatransfer/Source_folder/Json/'
FILE_FORMAT = Json_ETL_File_Format

LIST @GCP_ETL_Json_STAGE 


----------------CREATE TABLE (Json) -----------------------------------

CREATE OR REPLACE Table Json_Data_Table
(
Earth_quakes  VARIANT
);


----------------COPY (data) INTO JSON Table--------------


COPY INTO Json_Data_Table FROM @GCP_ETL_Json_STAGE 
FILES=('DayQuakes.json')

--SELECT  Earth_quakes:features[0] FROM Json_Data_Table



------------------------INSERT data into Json Table--------------------------------------


SELECT 
			value:properties.alert	      :: STRING AS Alert,
			value:properties.cdi	      :: STRING AS CDI,
			value:properties.code	      :: STRING AS code,
			value:properties.detail	      :: STRING AS Details,
			value:properties.dmin         :: STRING AS Dmin,
			value:properties.felt         :: STRING AS Felt,
			value:properties.gap          :: STRING AS Gap,
			value:properties.ids          :: STRING AS IDs,
			value:properties.mag          :: STRING AS Magnitude,
			value:properties.magType      :: STRING AS MagType,
			value:properties.mmi          :: STRING AS MMI,
			value:properties.net          :: STRING AS Net,
			value:properties.nst          :: STRING AS NST,
			value:properties.place        :: STRING AS Place,
			value:properties.rms          :: STRING AS RMS,
			value:properties.sig          :: STRING AS Significance,
			value:properties.sources      :: STRING AS Sources,
			value:properties.status       :: STRING AS Status,
			value:properties.time         :: STRING AS Time,
			value:properties.title        :: STRING AS Title,
			value:properties.tsunami      :: STRING AS Tsunami,
			value:properties.type         :: STRING AS Type,
			value:properties.types        :: STRING AS Types,
			value:properties.tz           :: STRING AS TimeZone,
			value:properties.updated      :: STRING AS Updated,
			value:properties.url          :: STRING AS URL,
            		value:geometry.coordinates[0] :: STRING AS Longitude,
            		value:geometry.coordinates[1] :: STRING AS Latitude, 
            		value:geometry.coordinates[2] :: STRING AS Depth,
            		value:id                      :: STRING AS ID

FROM 
    Json_Data_Table,
    LATERAL FLATTEN(Earth_quakes:features) AS feature

