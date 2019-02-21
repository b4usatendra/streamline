CREATE TABLE `job_cluster_map` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `topologyId` bigint(20) NOT NULL,
  `topologyName` varchar(100) NOT NULL,
  `runner` varchar(45) NOT NULL,
  `jobId` varchar(200) NOT NULL,
  `connectionEndpoint` varchar(1000) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `job_id_UNIQUE` (`jobId`),
  KEY `topology_id_index` (`topologyId`),
  CONSTRAINT `topology_id_fk` FOREIGN KEY (`topologyId`) REFERENCES `topology` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8