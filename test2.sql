-- Create database
CREATE DATABASE IF NOT EXISTS test;

-- Use the database
USE test;

-- Create table with extra columns for incremental migration
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    age INT,
    city VARCHAR(50),
    country VARCHAR(50),
    created_date DATE,
    status VARCHAR(10),
    phone VARCHAR(15),
    last_login TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- row creation time
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- last updated time
);

-- Insert 50 records
INSERT INTO users (id, username, email, age, city, country, created_date, status) VALUES
(1,'user1','user1@example.com',22,'Bangalore','India','2024-01-01','ACTIVE'),
(2,'user2','user2@example.com',25,'Chennai','India','2024-01-02','ACTIVE'),
(3,'user3','user3@example.com',28,'Hyderabad','India','2024-01-03','INACTIVE'),
(4,'user4','user4@example.com',30,'Mumbai','India','2024-01-04','ACTIVE'),
(5,'user5','user5@example.com',26,'Delhi','India','2024-01-05','ACTIVE'),
(6,'user6','user6@example.com',24,'Pune','India','2024-01-06','INACTIVE'),
(7,'user7','user7@example.com',29,'Kochi','India','2024-01-07','ACTIVE'),
(8,'user8','user8@example.com',32,'Trivandrum','India','2024-01-08','ACTIVE'),
(9,'user9','user9@example.com',27,'Coimbatore','India','2024-01-09','INACTIVE'),
(10,'user10','user10@example.com',35,'Noida','India','2024-01-10','ACTIVE'),
(11,'user11','user11@example.com',23,'Jaipur','India','2024-01-11','ACTIVE'),
(12,'user12','user12@example.com',31,'Indore','India','2024-01-12','ACTIVE'),
(13,'user13','user13@example.com',28,'Bhopal','India','2024-01-13','INACTIVE'),
(14,'user14','user14@example.com',34,'Surat','India','2024-01-14','ACTIVE'),
(15,'user15','user15@example.com',26,'Vadodara','India','2024-01-15','ACTIVE'),
(16,'user16','user16@example.com',29,'Rajkot','India','2024-01-16','INACTIVE'),
(17,'user17','user17@example.com',33,'Nagpur','India','2024-01-17','ACTIVE'),
(18,'user18','user18@example.com',21,'Amritsar','India','2024-01-18','ACTIVE'),
(19,'user19','user19@example.com',27,'Ludhiana','India','2024-01-19','INACTIVE'),
(20,'user20','user20@example.com',36,'Chandigarh','India','2024-01-20','ACTIVE'),
(21,'user21','user21@example.com',24,'Patna','India','2024-01-21','ACTIVE'),
(22,'user22','user22@example.com',30,'Ranchi','India','2024-01-22','ACTIVE'),
(23,'user23','user23@example.com',28,'Guwahati','India','2024-01-23','INACTIVE'),
(24,'user24','user24@example.com',35,'Shillong','India','2024-01-24','ACTIVE'),
(25,'user25','user25@example.com',26,'Aizawl','India','2024-01-25','ACTIVE'),
(26,'user26','user26@example.com',31,'Imphal','India','2024-01-26','INACTIVE'),
(27,'user27','user27@example.com',29,'Kohima','India','2024-01-27','ACTIVE'),
(28,'user28','user28@example.com',34,'Agartala','India','2024-01-28','ACTIVE'),
(29,'user29','user29@example.com',22,'Gangtok','India','2024-01-29','INACTIVE'),
(30,'user30','user30@example.com',37,'Itanagar','India','2024-01-30','ACTIVE'),
(31,'user31','user31@example.com',25,'Raipur','India','2024-02-01','ACTIVE'),
(32,'user32','user32@example.com',28,'Bilaspur','India','2024-02-02','INACTIVE'),
(33,'user33','user33@example.com',33,'Durg','India','2024-02-03','ACTIVE'),
(34,'user34','user34@example.com',29,'Gwalior','India','2024-02-04','ACTIVE'),
(35,'user35','user35@example.com',36,'Ujjain','India','2024-02-05','INACTIVE'),
(36,'user36','user36@example.com',24,'Sagar','India','2024-02-06','ACTIVE'),
(37,'user37','user37@example.com',31,'Jabalpur','India','2024-02-07','ACTIVE'),
(38,'user38','user38@example.com',27,'Rewa','India','2024-02-08','INACTIVE'),
(39,'user39','user39@example.com',35,'Satna','India','2024-02-09','ACTIVE'),
(40,'user40','user40@example.com',26,'Katni','India','2024-02-10','ACTIVE'),
(41,'user41','user41@example.com',32,'Anantapur','India','2024-02-11','INACTIVE'),
(42,'user42','user42@example.com',28,'Kurnool','India','2024-02-12','ACTIVE'),
(43,'user43','user43@example.com',34,'Nellore','India','2024-02-13','ACTIVE'),
(44,'user44','user44@example.com',23,'Ongole','India','2024-02-14','INACTIVE'),
(45,'user45','user45@example.com',37,'Eluru','India','2024-02-15','ACTIVE'),
(46,'user46','user46@example.com',29,'Vijayawada','India','2024-02-16','ACTIVE'),
(47,'user47','user47@example.com',31,'Guntur','India','2024-02-17','INACTIVE'),
(48,'user48','user48@example.com',26,'Tirupati','India','2024-02-18','ACTIVE'),
(49,'user49','user49@example.com',33,'Kadapa','India','2024-02-19','ACTIVE'),
(50,'user50','user50@example.com',28,'Chittoor','India','2024-02-20','INACTIVE');
