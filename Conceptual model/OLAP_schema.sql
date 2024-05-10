CREATE DATABASE test;
USE test;

-- Create Property Dimension
CREATE TABLE IF NOT EXISTS dim_property (
    proper_id INT PRIMARY KEY,
    ADDRESS VARCHAR(255),
    `ZIP CODE` VARCHAR(10),
    `LAND SQUARE FEET` float,
    `GROSS SQUARE FEET` float,
    `YEAR BUILT` INT,
    BIN INT,
    BBL INT
);

-- Create Location Dimension
CREATE TABLE IF NOT EXISTS dim_location (
    location_id INT PRIMARY KEY,
    NEIGHBORHOOD VARCHAR(50),
    BOROUGH VARCHAR(100),
    NTA_x VARCHAR(10)
);

-- Create Building Classification Dimension
CREATE TABLE IF NOT EXISTS dim_buildingclassification (
    buildingclass_id INT PRIMARY KEY,
    `BUILDING CLASS CATEGORY` VARCHAR(50),
    `TAX CLASS AS OF FINAL ROLL` VARCHAR(10),
    `BUILDING CLASS AS OF FINAL ROLL` VARCHAR(10)
);


-- Create Property Owner Dimension
CREATE TABLE IF NOT EXISTS dim_propertyowner (
    owner_id INT PRIMARY KEY,
    `OWNER_NAME` VARCHAR(100),
    `LAST_VISIT_DT` DATE
);


-- Create Fire Safety Inspection Dimension
CREATE TABLE IF NOT EXISTS dim_firesafetyinspection (
    inspect_id INT PRIMARY KEY,
    `LAST_VISIT_DT` DATE,
    `LAST_INSP_STAT` VARCHAR(20),
    `LAST_FULL_INSP_DT` DATE,
    `ALPHA` VARCHAR(10),
    `COF_ID` INT,
    `COF_NUM` INT,
    `COF_TYPE` VARCHAR(20)
);

-- Create Certificates Of Fitness Dimension
CREATE TABLE IF NOT EXISTS dim_certificatesoffitness (
    cof_id INT PRIMARY KEY,
    `COF_NUM` INT,
    `COF_TYPE` VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS fact_realestate (
    id INT PRIMARY KEY,
    proper_id INT,
    owner_id INT,
    inspect_id INT,
    cof_id INT,
    buildingclass_id INT,
    location_id INT,
    `SALE PRICE` NUMERIC(15, 2),
    `SALE DATE` DATE,
    `TAX CLASS AT TIME OF SALE` VARCHAR(10),
    `BUILDING CLASS AT TIME OF SALE` VARCHAR(10),
    FOREIGN KEY (proper_id) REFERENCES dim_property(proper_id) ON DELETE CASCADE,
    FOREIGN KEY (owner_id) REFERENCES dim_propertyowner(owner_id) ON DELETE CASCADE,
    FOREIGN KEY (inspect_id) REFERENCES dim_firesafetyinspection(inspect_id) ON DELETE CASCADE,
    FOREIGN KEY (cof_id) REFERENCES dim_certificatesoffitness(cof_id) ON DELETE CASCADE,
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id) ON DELETE CASCADE,
    FOREIGN KEY (buildingclass_id) REFERENCES dim_buildingclassification(buildingclass_id) ON DELETE CASCADE
);


