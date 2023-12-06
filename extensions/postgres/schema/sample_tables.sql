CREATE TABLE sample_user_table(
  user_id_1 VARCHAR(255) NOT NULL,
  user_id_2 VARCHAR(255) NOT NULL,
  first_name VARCHAR(255),
  last_name VARCHAR(255),
  email VARCHAR(255),
  create_timestamp BIGINT,
  PRIMARY KEY (user_id_1, user_id_2)
);

CREATE TABLE sample_order_table(
  order_id BIGINT NOT NULL,
  user_id VARCHAR(255),
  sequence bigserial NOT NULL,
  create_timestamp BIGINT,
  item_name VARCHAR(255),
  PRIMARY KEY (order_id)
);