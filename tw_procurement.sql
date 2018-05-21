CREATE DATABASE `tw_procurement` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;

CREATE TABLE `declaration_notify` (
  `id` varchar(32) NOT NULL,
  `org_name` varchar(64) DEFAULT NULL,
  `subject` varchar(128) DEFAULT NULL,
  `method` varchar(32) DEFAULT NULL,
  `category` varchar(16) DEFAULT NULL,
  `declare_date` date DEFAULT NULL,
  `deadline` date DEFAULT NULL,
  `budget` bigint(12) DEFAULT NULL,
  `url` varchar(512) DEFAULT NULL,
  `notified` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `DIS_IDX` (`declare_date`,`org_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
