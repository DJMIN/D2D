CREATE DATABASE  IF NOT EXISTS `gu` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `gu`;
-- MySQL dump 10.13  Distrib 8.0.19, for Win64 (x86_64)
--
-- Host: localhost    Database: gu
-- ------------------------------------------------------
-- Server version	8.0.19

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `gu_test`
--

DROP TABLE IF EXISTS `gu_test`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `gu_test` (
  `交易所行情日期` char(32) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `证券代码` char(16) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `今开盘价格` float DEFAULT NULL,
  `最高价` float DEFAULT NULL,
  `最低价` float DEFAULT NULL,
  `今收盘价` float DEFAULT NULL,
  `昨日收盘价` float DEFAULT NULL,
  `成交数量` bigint DEFAULT NULL,
  `成交金额` float DEFAULT NULL,
  `复权状态` int DEFAULT NULL,
  `换手率` float DEFAULT NULL,
  `交易状态` bigint DEFAULT NULL,
  `涨跌` float DEFAULT NULL,
  `滚动市盈率` float DEFAULT NULL,
  `滚动市销率` float DEFAULT NULL,
  `滚动市现率` float DEFAULT NULL,
  `市净率` float DEFAULT NULL,
  `是否ST` int DEFAULT NULL,
  `code` varchar(16) COLLATE utf8mb4_general_ci NOT NULL,
  `code_prefix` char(6) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `time_date_int` bigint DEFAULT NULL,
  `time_date_str` varchar(16) COLLATE utf8mb4_general_ci NOT NULL,
  `time_date` datetime(6) DEFAULT NULL,
  `close` float DEFAULT NULL,
  `industry` varchar(45) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `create_time` datetime(6) DEFAULT NULL,
  `status` int DEFAULT NULL,
  `upp` float DEFAULT NULL,
  `nan` varchar(45) COLLATE utf8mb4_general_ci DEFAULT NULL,
  PRIMARY KEY (`code`,`time_date_str`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `gu_test`
--

LOCK TABLES `gu_test` WRITE;
/*!40000 ALTER TABLE `gu_test` DISABLE KEYS */;
/*!40000 ALTER TABLE `gu_test` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2020-04-08  2:11:16
