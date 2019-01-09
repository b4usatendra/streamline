--add a new streamingEngine column in udf table
ALTER TABLE `streamline_db`.`udf`
ADD COLUMN `streamingEngine` VARCHAR(100) NOT NULL AFTER `type`;
