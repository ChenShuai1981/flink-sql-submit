DROP TABLE IF EXISTS `user`;
CREATE TABLE `user`  (
  `user_id` int(20) NOT NULL AUTO_INCREMENT,
  `username` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `sex` int(2) NULL DEFAULT NULL,
  `age` int(4) NULL DEFAULT NULL,
  `registertime` datetime(0) NULL DEFAULT NULL,
  PRIMARY KEY (`user_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 11 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;


INSERT INTO `user` VALUES (1, 'aaa', 1, 13, '2019-05-15 14:17:10');
INSERT INTO `user` VALUES (2, 'YcDoS', 1, 36, '2018-09-03 00:00:00');
INSERT INTO `user` VALUES (3, 'oJWeV', 1, 25, '2017-05-09 00:00:00');
INSERT INTO `user` VALUES (4, 'hnGDY', 0, 35, '2017-05-31 00:00:00');
INSERT INTO `user` VALUES (5, 'xzuoF', 0, 32, '2018-04-10 00:00:00');
INSERT INTO `user` VALUES (6, 'DuUiZ', 0, 35, '2017-05-03 00:00:00');
INSERT INTO `user` VALUES (7, 'UrdjL', 1, 24, '2016-05-22 00:00:00');
INSERT INTO `user` VALUES (8, 'PDrmT', 0, 30, '2018-05-08 00:00:00');
INSERT INTO `user` VALUES (9, 'HJJkZ', 1, 32, '2017-01-14 00:00:00');
INSERT INTO `user` VALUES (10, 'xCHxS', 0, 15, '2019-01-06 00:00:00');

DROP TABLE IF EXISTS `lookup_sink`;
CREATE TABLE `lookup_sink`  (
  `user_id` int(20) NOT NULL AUTO_INCREMENT,
  `item_id` int(20) NULL DEFAULT NULL,
  `category_id` int(20) NULL DEFAULT NULL,
  `behavior` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `ts` datetime(0) NULL DEFAULT NULL,
  `username` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `sex` int(2) NULL DEFAULT NULL,
  `age` int(4) NULL DEFAULT NULL,
  `registertime` datetime(0) NULL DEFAULT NULL,
  PRIMARY KEY (`user_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
