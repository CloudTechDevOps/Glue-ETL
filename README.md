# Glue-ETL

create database test;
use test;
CREATE TABLE users (
    id VARCHAR(255) NOT NULL,
    user_name VARCHAR(255),
    Email VARCHAR(255),
    age VARCHAR(50),
    city VARCHAR(100),
    country VARCHAR(100),
    status VARCHAR(50),
    phone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);
