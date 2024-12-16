USE pos;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS t_product;
DROP TABLE IF EXISTS t_customer;
DROP TABLE IF EXISTS t_employee;
DROP TABLE IF EXISTS t_location;
DROP TABLE IF EXISTS t_order;

-- Create t_product table with partition (by upload_date) and skip header line
CREATE EXTERNAL TABLE t_product (
    id INT,
    category_name STRING,
    subcategory STRING,
    product_name STRING,
    sale_price DOUBLE,
    price DOUBLE
)
PARTITIONED BY (upload_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hdfs/userfile/product/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Create t_customer table with partition (by upload_date) and skip header line
CREATE EXTERNAL TABLE t_customer (
    id INT,
    firstName STRING,
    lastName STRING,
    gender STRING,
    phone STRING,
    email STRING,
    status STRING,
    createdDate STRING,
    loc_id INT
)
PARTITIONED BY (upload_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hdfs/userfile/customer/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Create t_employee table with partition (by upload_date) and skip header line
CREATE EXTERNAL TABLE t_employee (
    id INT,
    firstname STRING,
    lastname STRING,
    photo STRING,
    dob STRING,
    email STRING,
    phone STRING,
    position STRING
)
PARTITIONED BY (upload_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hdfs/userfile/employee/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Create t_location table with partition (by upload_date) and skip header line
CREATE EXTERNAL TABLE t_location (
    City STRING,
    Country STRING,
    CountryCode STRING,
    Lat DOUBLE,
    Long DOUBLE,
    Postal STRING,
    State STRING,
    StateAbbrev STRING,
    Street STRING,
    StreetName STRING,
    NoStreet STRING,
    StreetSuffix STRING,
    TimeZone STRING
)
PARTITIONED BY (upload_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hdfs/userfile/location/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Create t_order table with partition (by upload_date) and skip header line
CREATE EXTERNAL TABLE t_order (
    id INT,
    category_name STRING,
    subcategory STRING,
    product_name STRING,
    sale_price DOUBLE,
    price DOUBLE
)
PARTITIONED BY (upload_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hdfs/userfile/order/'
TBLPROPERTIES ("skip.header.line.count"="1");
