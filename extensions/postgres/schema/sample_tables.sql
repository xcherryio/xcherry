CREATE TABLE sample_user_table(
  user_id VARCHAR(31) NOT NULL,
  first_name VARCHAR(31),
  last_name VARCHAR(31),
  create_timestamp INTEGER,
  PRIMARY KEY (user_id)
);

CREATE TABLE sample_order_table(
  order_id INTEGER NOT NULL,
  user_id VARCHAR(31),
  sequence bigserial NOT NULL,
  item_name VARCHAR(31),
  PRIMARY KEY (order_id)
);