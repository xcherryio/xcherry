CREATE TABLE sample_user_table(
  user_id VARCHAR(31) NOT NULL,
  first_name VARCHAR(31) NOT NULL,
  last_name VARCHAR(31) NOT NULL,
  create_timestamp INTEGER NOT NULL,
  PRIMARY KEY (user_id)
);

CREATE TABLE sample_order_table(
  order_id INTEGER NOT NULL,
  user_name VARCHAR(31) NOT NULL,
  sequence bigserial NOT NULL,
  item_name VARCHAR(31) NOT NULL,
  PRIMARY KEY (order_id)
);